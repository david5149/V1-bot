import os
import re
import asyncio
import json
from dataclasses import dataclass
from typing import Optional

import discord
from discord import app_commands
from dotenv import load_dotenv
import yt_dlp

load_dotenv()

def _nacl_status() -> tuple[bool, str]:
    try:
        import nacl  # type: ignore

        ver = getattr(nacl, "__version__", "unknown")
        return True, f"PyNaCl OK (version {ver})"
    except Exception as e:
        return False, f"PyNaCl import failed: {e}"

ROLE_RANK_KEYWORDS = {
    "coach",
    "beginner",
    "amateur",
    "advanced",
    "professional",
    "mentor",
    "helper",
    "staff",
    "wired",  # Bot bypass role
}


def _env_int(name: str) -> Optional[int]:
    v = os.getenv(name)
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        return None


DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
if not DISCORD_TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN in .env")

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID") or None
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET") or None

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

# Optional bootstrap via .env (backwards compatible)
JTC_LOBBY_CHANNEL_ID = _env_int("JTC_LOBBY_CHANNEL_ID")
JTC_LOBBY_CHANNEL_IDS_RAW = os.getenv("JTC_LOBBY_CHANNEL_IDS", "").strip()

# Where we store which temp channel belongs to which owner.
# key: temp_channel_id, value: owner_user_id
TEMP_CHANNEL_OWNERS: dict[int, int] = {}

# Per-guild lobby configuration.
# Stored as: { "<guild_id>": { "lobbies": [<voice_channel_id>, ...] } }
CONFIG: dict[str, dict[str, list[int]]] = {}


class TempVCBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.voice_states = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self._cleanup_lock = asyncio.Lock()
        self.players: dict[int, "GuildPlayer"] = {}

    async def setup_hook(self) -> None:
        # Global sync can take a long time to propagate.
        # We'll still do it once, but also do per-guild sync in on_ready.
        await self.tree.sync()


client = TempVCBot()


def _sanitize_channel_name(name: str) -> str:
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"[^\w \-']", "", name, flags=re.UNICODE)
    name = name.strip()
    if not name:
        return "Temp Channel"
    if len(name) > 80:
        name = name[:80].rstrip()
    return name


def _clean_role_name_for_matching(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _role_group_key(role_name: str) -> Optional[str]:
    cleaned = _clean_role_name_for_matching(role_name)
    if not cleaned:
        return None

    words = cleaned.split(" ")
    if not any(w in ROLE_RANK_KEYWORDS for w in words):
        return None

    kept = [w for w in words if w not in ROLE_RANK_KEYWORDS]
    group = " ".join(kept).strip()
    return group or None


def _member_group_keys(member: discord.Member) -> set[str]:
    keys: set[str] = set()
    for r in member.roles:
        k = _role_group_key(r.name)
        if k:
            keys.add(k)
    return keys


@dataclass
class QueueItem:
    title: str
    webpage_url: str
    stream_url: str
    requested_by: int


YTDLP_OPTS = {
    "format": "bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "default_search": "ytsearch",
    "extract_flat": False,
}


def _ffmpeg_source(url: str) -> discord.AudioSource:
    before_options = "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5"
    options = "-vn"
    return discord.FFmpegPCMAudio(url, before_options=before_options, options=options)


async def _ytdlp_extract(query: str) -> QueueItem:
    def run() -> dict:
        with yt_dlp.YoutubeDL(YTDLP_OPTS) as ydl:
            return ydl.extract_info(query, download=False)

    info = await asyncio.to_thread(run)

    if "entries" in info and info["entries"]:
        info = info["entries"][0]

    stream_url = info.get("url")
    webpage_url = info.get("webpage_url") or info.get("original_url") or query
    title = info.get("title") or "Unknown title"
    if not stream_url:
        raise RuntimeError("Couldn't get stream URL from yt-dlp.")

    return QueueItem(title=title, webpage_url=webpage_url, stream_url=stream_url, requested_by=0)


def _spotify_client():
    if not (SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET):
        return None
    try:
        import spotipy  # type: ignore
        from spotipy.oauth2 import SpotifyClientCredentials  # type: ignore
    except Exception:
        return None
    auth = SpotifyClientCredentials(client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET)
    return spotipy.Spotify(auth_manager=auth)


def _is_spotify_url(s: str) -> bool:
    return "open.spotify.com/" in s or s.startswith("spotify:")


def _spotify_track_query(track: dict) -> str:
    name = track.get("name") or ""
    artists = ", ".join(a.get("name", "") for a in (track.get("artists") or []) if a.get("name"))
    q = f"{name} {artists}".strip()
    return q or name or "spotify track"


async def _spotify_to_queries(url: str) -> list[str]:
    sp = _spotify_client()
    if not sp:
        raise RuntimeError("Spotify is not configured. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in .env.")

    def run() -> list[str]:
        # Track
        if "open.spotify.com/track/" in url:
            tid = url.split("track/")[1].split("?")[0].split("/")[0]
            t = sp.track(tid)
            return [_spotify_track_query(t)]

        # Playlist
        if "open.spotify.com/playlist/" in url:
            pid = url.split("playlist/")[1].split("?")[0].split("/")[0]
            items: list[str] = []
            results = sp.playlist_items(pid, additional_types=("track",))
            while results:
                for it in results.get("items", []) or []:
                    tr = (it or {}).get("track")
                    if tr:
                        items.append(_spotify_track_query(tr))
                results = sp.next(results) if results.get("next") else None
            return items

        # Album
        if "open.spotify.com/album/" in url:
            aid = url.split("album/")[1].split("?")[0].split("/")[0]
            results = sp.album_tracks(aid)
            items: list[str] = []
            while results:
                for tr in results.get("items", []) or []:
                    items.append(_spotify_track_query(tr))
                results = sp.next(results) if results.get("next") else None
            return items

        raise RuntimeError("Unsupported Spotify link. Use track/playlist/album.")

    return await asyncio.to_thread(run)


class GuildPlayer:
    def __init__(self, guild_id: int) -> None:
        self.guild_id = guild_id
        self.queue: asyncio.Queue[QueueItem] = asyncio.Queue()
        self.current: Optional[QueueItem] = None
        self._task: Optional[asyncio.Task] = None
        self._text_channel_id: Optional[int] = None

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._runner())

    async def _runner(self) -> None:
        while True:
            item = await self.queue.get()
            self.current = item

            guild = client.get_guild(self.guild_id)
            if not guild:
                continue
            vc = guild.voice_client
            if not vc or not vc.is_connected():
                continue

            done = asyncio.Event()

            def _after(_: Optional[Exception]) -> None:
                client.loop.call_soon_threadsafe(done.set)

            try:
                vc.play(_ffmpeg_source(item.stream_url), after=_after)
                await done.wait()
            except Exception:
                try:
                    vc.stop()
                except Exception:
                    pass
            finally:
                self.current = None


def _get_player(guild_id: int) -> GuildPlayer:
    p = client.players.get(guild_id)
    if not p:
        p = GuildPlayer(guild_id)
        client.players[guild_id] = p
    p.start()
    return p


async def _ensure_connected(interaction: discord.Interaction) -> discord.VoiceClient:
    if not interaction.guild:
        raise RuntimeError("Use this in a server.")
    if not isinstance(interaction.user, discord.Member):
        raise RuntimeError("Couldn't resolve your member info.")
    if not interaction.user.voice or not interaction.user.voice.channel:
        raise RuntimeError("Join a voice channel first.")

    vc = interaction.guild.voice_client
    if vc and vc.is_connected():
        return vc

    return await interaction.user.voice.channel.connect()


def _load_config() -> None:
    global CONFIG
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            CONFIG = data
    except FileNotFoundError:
        CONFIG = {}
    except json.JSONDecodeError:
        CONFIG = {}


def _save_config() -> None:
    tmp_path = f"{CONFIG_PATH}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(CONFIG, f, indent=2, sort_keys=True)
    os.replace(tmp_path, CONFIG_PATH)


def _get_guild_lobbies(guild_id: int) -> set[int]:
    entry = CONFIG.get(str(guild_id), {})
    lobbies = entry.get("lobbies", [])
    if not isinstance(lobbies, list):
        return set()
    out: set[int] = set()
    for x in lobbies:
        try:
            out.add(int(x))
        except (TypeError, ValueError):
            pass
    return out


def _set_guild_lobbies(guild_id: int, lobby_ids: set[int]) -> None:
    CONFIG[str(guild_id)] = {"lobbies": sorted(lobby_ids)}


def _bootstrap_env_lobbies_into_config(guild: discord.Guild) -> None:
    # If config already has lobbies for this guild, don't overwrite.
    existing = _get_guild_lobbies(guild.id)
    if existing:
        return

    env_ids: set[int] = set()
    if JTC_LOBBY_CHANNEL_ID:
        env_ids.add(JTC_LOBBY_CHANNEL_ID)

    if JTC_LOBBY_CHANNEL_IDS_RAW:
        for part in JTC_LOBBY_CHANNEL_IDS_RAW.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                env_ids.add(int(part))
            except ValueError:
                pass

    if env_ids:
        _set_guild_lobbies(guild.id, env_ids)
        _save_config()


async def _maybe_delete_temp_channel(channel: discord.VoiceChannel) -> None:
    if channel.id not in TEMP_CHANNEL_OWNERS:
        return
    if len(channel.members) > 0:
        return
    try:
        await channel.delete(reason="Temporary voice channel empty")
    finally:
        TEMP_CHANNEL_OWNERS.pop(channel.id, None)


@client.event
async def on_ready() -> None:
    _load_config()
    for g in client.guilds:
        _bootstrap_env_lobbies_into_config(g)
        try:
            # Per-guild sync makes new commands appear immediately.
            await client.tree.sync(guild=g)
        except Exception as e:
            print(f"[sync] guild={g.id} failed: {e!r}")
            
        # Create "wired" role for bot if it doesn't exist and assign it
        try:
            wired_role = discord.utils.get(g.roles, name="wired")
            if not wired_role:
                wired_role = await g.create_role(
                    name="wired",
                    color=discord.Color.blue(),
                    reason="Bot bypass role for temp voice channels",
                )
                print(f"Created 'wired' role in {g.name}")
            
            # Assign the role to the bot if it doesn't have it
            bot_member = g.get_member(client.user.id)
            if bot_member and wired_role not in bot_member.roles:
                await bot_member.add_roles(wired_role, reason="Bot access to all temp channels")
                print(f"Assigned 'wired' role to bot in {g.name}")
                
        except discord.HTTPException as e:
            print(f"Failed to create/assign wired role in {g.name}: {e}")
        except Exception as e:
            print(f"Unexpected error with wired role in {g.name}: {e}")
            
    print(f"Logged in as {client.user} (id={client.user.id})")
    print("Bot is ready.")


@client.tree.command(name="sync", description="Force-refresh slash commands for this server.")
async def sync_cmd(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        local_cmds = client.tree.get_commands()
        synced = await client.tree.sync(guild=interaction.guild)
        await interaction.followup.send(
            f"Local commands loaded: **{len(local_cmds)}**\n"
            f"Synced to this server: **{len(synced)}**",
            ephemeral=True,
        )
    except Exception as e:
        await interaction.followup.send(f"Sync failed: **{e}**", ephemeral=True)


@client.event
async def on_voice_state_update(
    member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
) -> None:
    # 1) If user JOINED a channel, check if it’s the join-to-create lobby.
    if after.channel and (before.channel is None or before.channel.id != after.channel.id):
        lobby_ids = _get_guild_lobbies(member.guild.id)
        if after.channel.id in lobby_ids and isinstance(after.channel, discord.VoiceChannel):
            lobby = after.channel
            # Default behavior: create temp channels in the SAME category as the lobby.
            category = lobby.category

            base_name = f"{member.display_name}'s channel"
            channel_name = _sanitize_channel_name(base_name)

            # Role-based access:
            # If the creator has roles like "Web Apps ... Coach", then anyone with "Web Apps ..." roles can join.
            overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
                member.guild.default_role: discord.PermissionOverwrite(connect=False, view_channel=False),
                member: discord.PermissionOverwrite(
                    view_channel=True,
                    connect=True,
                    manage_channels=True,
                    move_members=True,
                    mute_members=True,
                    deafen_members=True,
                ),
            }

            creator_groups = _member_group_keys(member)
            if creator_groups:
                for role in member.guild.roles:
                    if role.is_default():
                        continue
                    key = _role_group_key(role.name)
                    # Always give "wired" role access to any temp channel
                    if key and (key in creator_groups or key == "wired"):
                        overwrites[role] = discord.PermissionOverwrite(view_channel=True, connect=True)
            else:
                # If no matching-group roles were found, fall back to a public temp channel.
                overwrites[member.guild.default_role] = discord.PermissionOverwrite(connect=True, view_channel=True)
                
            # Ensure wired role always has access, even in public channels
            for role in member.guild.roles:
                if _role_group_key(role.name) == "wired":
                    overwrites[role] = discord.PermissionOverwrite(view_channel=True, connect=True)

            temp_channel = await member.guild.create_voice_channel(
                name=channel_name,
                category=category,
                overwrites=overwrites,
                reason="Join-to-create temporary voice channel",
            )
            TEMP_CHANNEL_OWNERS[temp_channel.id] = member.id

            try:
                await member.move_to(temp_channel, reason="Move user into their temporary channel")
            except discord.HTTPException:
                # If move fails (permissions/latency), we still keep the channel; cleanup will delete it if empty.
                pass

    # 2) Cleanup: if someone LEFT a temp channel, delete it when empty.
    if before.channel and (after.channel is None or before.channel.id != after.channel.id):
        if isinstance(before.channel, discord.VoiceChannel):
            async with client._cleanup_lock:
                await _maybe_delete_temp_channel(before.channel)


@client.tree.command(name="add_lobby", description="Add a join-to-create lobby voice channel.")
@app_commands.describe(channel="Voice channel users join to create a temp channel")
async def add_lobby(interaction: discord.Interaction, channel: discord.VoiceChannel) -> None:
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)
        return
    if not interaction.guild:
        await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
        return
    lobby_ids = _get_guild_lobbies(interaction.guild.id)
    lobby_ids.add(channel.id)
    _set_guild_lobbies(interaction.guild.id, lobby_ids)
    _save_config()
    await interaction.response.send_message(
        f"Added lobby {channel.mention}. Now tracking **{len(lobby_ids)}** lobby channel(s).",
        ephemeral=True,
    )


@client.tree.command(name="remove_lobby", description="Remove a join-to-create lobby voice channel.")
@app_commands.describe(channel="Lobby voice channel to stop tracking")
async def remove_lobby(interaction: discord.Interaction, channel: discord.VoiceChannel) -> None:
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)
        return
    if not interaction.guild:
        await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
        return
    lobby_ids = _get_guild_lobbies(interaction.guild.id)
    lobby_ids.discard(channel.id)
    _set_guild_lobbies(interaction.guild.id, lobby_ids)
    _save_config()
    await interaction.response.send_message(
        f"Removed lobby {channel.mention}. Now tracking **{len(lobby_ids)}** lobby channel(s).",
        ephemeral=True,
    )


@client.tree.command(name="tempvc_info", description="Show current temp-VC configuration.")
async def tempvc_info(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
        return

    lobby_ids = sorted(_get_guild_lobbies(interaction.guild.id))
    if lobby_ids:
        mentions: list[str] = []
        for lid in lobby_ids[:20]:
            ch = interaction.guild.get_channel(lid)
            mentions.append(ch.mention if isinstance(ch, discord.VoiceChannel) else str(lid))
        lobby_txt = ", ".join(mentions) + (f" (+{len(lobby_ids) - 20} more)" if len(lobby_ids) > 20 else "")
    else:
        lobby_txt = "Not set"

    await interaction.response.send_message(
        f"**Lobbies**: {lobby_txt}\n**Active temp channels**: {len(TEMP_CHANNEL_OWNERS)}",
        ephemeral=True,
    )


@client.tree.command(name="join", description="Make the bot join your voice channel.")
async def join(interaction: discord.Interaction) -> None:
    try:
        ok, msg = _nacl_status()
        if not ok:
            await interaction.response.send_message(
                f"Couldn't join voice. **{msg}**\n"
                f"Install it with: `python -m pip install --user PyNaCl` then restart the bot.",
                ephemeral=True,
            )
            return
        await _ensure_connected(interaction)
        await interaction.response.send_message("Joined your voice channel.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Couldn't join: **{e}**", ephemeral=True)


@client.tree.command(name="voice_debug", description="Show voice dependency status (PyNaCl).")
async def voice_debug(interaction: discord.Interaction) -> None:
    ok, msg = _nacl_status()
    await interaction.response.send_message(msg, ephemeral=True)


@client.tree.command(name="leave", description="Disconnect the bot from voice.")
async def leave(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    vc = interaction.guild.voice_client
    if not vc:
        await interaction.response.send_message("I'm not connected.", ephemeral=True)
        return
    await vc.disconnect()
    await interaction.response.send_message("Disconnected.", ephemeral=True)


@client.tree.command(name="play", description="Play music from YouTube or a Spotify link.")
@app_commands.describe(query="YouTube search/url, or Spotify track/playlist/album link")
async def play(interaction: discord.Interaction, query: str) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        vc = await _ensure_connected(interaction)
        player = _get_player(interaction.guild.id)

        # Spotify: expand to queries, then search YouTube.
        if _is_spotify_url(query):
            track_queries = await _spotify_to_queries(query)
            if not track_queries:
                raise RuntimeError("Spotify link had no tracks.")
            # Limit to avoid huge playlists nuking rate limits.
            track_queries = track_queries[:50]
            added = 0
            for tq in track_queries:
                item = await _ytdlp_extract(tq)
                item.requested_by = interaction.user.id
                await player.queue.put(item)
                added += 1
            await interaction.followup.send(f"Queued **{added}** track(s) from Spotify (matched on YouTube).", ephemeral=True)
            if not vc.is_playing() and not vc.is_paused():
                # kick runner (it will pick up queue naturally)
                pass
            return

        # YouTube / search
        item = await _ytdlp_extract(query)
        item.requested_by = interaction.user.id
        await player.queue.put(item)
        await interaction.followup.send(f"Queued: **{item.title}**", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"Play failed: **{e}**", ephemeral=True)


@client.tree.command(name="skip", description="Skip the current song.")
async def skip(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    vc = interaction.guild.voice_client
    if not vc or not (vc.is_playing() or vc.is_paused()):
        await interaction.response.send_message("Nothing is playing.", ephemeral=True)
        return
    vc.stop()
    await interaction.response.send_message("Skipped.", ephemeral=True)


@client.tree.command(name="pause", description="Pause playback.")
async def pause(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    vc = interaction.guild.voice_client
    if not vc or not vc.is_playing():
        await interaction.response.send_message("Nothing is playing.", ephemeral=True)
        return
    vc.pause()
    await interaction.response.send_message("Paused.", ephemeral=True)


@client.tree.command(name="resume", description="Resume playback.")
async def resume(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    vc = interaction.guild.voice_client
    if not vc or not vc.is_paused():
        await interaction.response.send_message("Nothing is paused.", ephemeral=True)
        return
    vc.resume()
    await interaction.response.send_message("Resumed.", ephemeral=True)


@client.tree.command(name="stop", description="Stop playback and clear the queue.")
async def stop(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    vc = interaction.guild.voice_client
    player = _get_player(interaction.guild.id)
    # Drain queue
    try:
        while True:
            player.queue.get_nowait()
            player.queue.task_done()
    except asyncio.QueueEmpty:
        pass
    if vc:
        vc.stop()
    await interaction.response.send_message("Stopped and cleared queue.", ephemeral=True)


@client.tree.command(name="queue", description="Show the next items in the queue.")
async def queue_cmd(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    player = _get_player(interaction.guild.id)
    items = list(player.queue._queue)  # snapshot
    if not items and not player.current:
        await interaction.response.send_message("Queue is empty.", ephemeral=True)
        return
    lines: list[str] = []
    if player.current:
        lines.append(f"**Now**: {player.current.title}")
    for i, it in enumerate(items[:10], start=1):
        lines.append(f"{i}. {it.title}")
    if len(items) > 10:
        lines.append(f"... and {len(items) - 10} more")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)


if __name__ == "__main__":
    client.run(DISCORD_TOKEN)