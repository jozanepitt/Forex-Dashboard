"""Discord bot exposing an on-demand `/analyse` slash command.

Backed by the dashboard's own `/analyze` HTTP endpoint (see app.py) -- this
process does no analysis itself, it just relays a Discord slash command to
the already-running dashboard service and formats the JSON result as an
embed. Runs as its own long-lived process, separate from app.py:

    python discord_bot.py

Requires DISCORD_BOT_TOKEN in .env (a real Discord Bot application token,
NOT the DISCORD_WEBHOOK_URL used for outbound alerts -- a webhook can't
receive slash commands). Invite the bot to your server with the
`applications.commands` + `bot` scopes; no privileged intents are needed
since this only uses slash commands, not free-text message parsing.
"""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

import discord
import requests
from discord import app_commands

from config import DISCORD_BOT_TOKEN, SERVICE_PORT, SERVICE_ROOT

log = logging.getLogger("discord_bot")

# Log to a rotating file (same pattern as app.py) so activity/errors are
# diagnosable even when the guardian runs this headless under pythonw (no
# console) -- stdout alone would otherwise be silently discarded.
_log_fmt = "%(asctime)s %(name)s %(levelname)s: %(message)s"
_file_handler = RotatingFileHandler(
    SERVICE_ROOT / "discord_bot.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8"
)
_file_handler.setFormatter(logging.Formatter(_log_fmt))
logging.basicConfig(level=logging.INFO, format=_log_fmt, handlers=[logging.StreamHandler(), _file_handler])

ANALYZE_URL = f"http://127.0.0.1:{SERVICE_PORT}/analyze"
REQUEST_TIMEOUT_SECS = 20

# JZ's Discord server (guild) ID. Slash commands are synced to this guild
# specifically so they propagate to the client in seconds -- a *global* sync
# (no guild argument) can take Discord up to ~1 hour to appear in clients.
# This bot only ever serves this one server, so guild-scoped registration is
# also the correct long-term choice (no need for global commands at all).
GUILD_ID = 1499049344152764466

STRATEGY_CHOICES = [
    app_commands.Choice(name="BTMM", value="BTMM"),
    app_commands.Choice(name="TDI123", value="TDI123"),
    app_commands.Choice(name="BTMM123", value="BTMM123"),
]


def _normalize_pair(raw: str) -> str:
    """Mirrors app.py's /analyze and /id50/detail pair normalization so
    'EURUSD', 'eur/usd', and 'EUR_USD' all resolve the same way."""
    sym = raw.strip().upper().replace("_", "/")
    if "/" not in sym and len(sym) == 6:
        sym = f"{sym[:3]}/{sym[3:]}"
    return sym


def _fmt_price(v) -> str:
    return f"{v:.5f}" if isinstance(v, (int, float)) else "-"


def _color_for(text: str) -> int:
    t = (text or "").lower()
    if "buy" in t:
        return 0x00E676
    if "sell" in t:
        return 0xFF5252
    return 0x9E9E9E


def _build_embed(pair: str, strategy: str, payload: dict) -> discord.Embed:
    result = payload.get("result", {})
    stale = payload.get("stale")

    if strategy == "BTMM":
        if result.get("signal") == "insufficient_data":
            embed = discord.Embed(title=f"{pair} - BTMM", color=_color_for(""),
                                   description="Not enough cached candle history yet.")
            return embed

        setup = result.get("active_setup") or {}
        embed = discord.Embed(title=f"{pair} - BTMM", color=_color_for(result.get("signal")))
        embed.add_field(name="Signal", value=result.get("signal", "-"), inline=True)
        embed.add_field(name="Confidence", value=result.get("confidence", "-"), inline=True)
        embed.add_field(name="Checklist", value=f"{result.get('checklist_score', '-')}/13", inline=True)
        embed.add_field(name="Score", value=str(result.get("score", "-")), inline=True)
        embed.add_field(name="Kill zone", value=result.get("kz") or "-", inline=True)
        embed.add_field(name="ADR consumed", value=f"{result.get('adr_consumed', '-')}%", inline=True)
        if setup:
            embed.add_field(name="Setup", value=setup.get("key", "-"), inline=True)
            embed.add_field(name="Entry", value=_fmt_price(setup.get("entry")), inline=True)
            embed.add_field(name="SL", value=_fmt_price(setup.get("sl")), inline=True)
            embed.add_field(name="TP1", value=_fmt_price(setup.get("tp1")), inline=True)
            if setup.get("tp2") is not None:
                embed.add_field(name="TP2", value=_fmt_price(setup.get("tp2")), inline=True)
    else:
        setup = result.get("setup", "NO-TRADE")
        plan = result.get("trade_plan") or {}
        embed = discord.Embed(title=f"{pair} - {strategy}", color=_color_for(setup))
        embed.add_field(name="Setup", value=setup, inline=True)
        embed.add_field(name="Grade", value=result.get("grade", "-"), inline=True)
        embed.add_field(name="Score", value=str(result.get("score", "-")), inline=True)
        embed.add_field(name="Session", value=result.get("session", "-"), inline=True)
        if plan:
            embed.add_field(name="Entry", value=_fmt_price(plan.get("entry")), inline=True)
            embed.add_field(name="SL", value=_fmt_price(plan.get("sl")), inline=True)
            embed.add_field(name="TP1", value=_fmt_price(plan.get("tp1")), inline=True)

    if stale:
        embed.set_footer(text="Data may be stale -- couldn't refresh from MT5 this call")
    return embed


class AnalysisClient(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        guild = discord.Object(id=GUILD_ID)
        try:
            # Register commands to JZ's server for near-instant propagation.
            self.tree.copy_global_to(guild=guild)
            synced = await self.tree.sync(guild=guild)
            log.info("synced %d slash command(s) to guild %s", len(synced), GUILD_ID)
            # Remove any leftover GLOBAL registration from the earlier global
            # sync so /analyse doesn't eventually show up twice (global + guild).
            self.tree.clear_commands(guild=None)
            await self.tree.sync()
        except discord.Forbidden:
            # 403 (50001 Missing Access): the bot is not authorized for
            # application commands in this guild -- it must be re-invited with
            # the `applications.commands` scope (or GUILD_ID is wrong). Don't
            # crash (which would just make the guardian relaunch into a crash
            # loop); log loudly and fall back to a GLOBAL sync so the process
            # stays alive. NOTE: until the scope is granted, commands won't
            # appear in the server at all, global or guild.
            log.exception(
                "guild command sync to %s failed: 403 Missing Access. Re-invite "
                "the bot with the 'applications.commands' scope (or verify "
                "GUILD_ID). Falling back to a global sync.",
                GUILD_ID,
            )
            synced = await self.tree.sync()
            log.info("fallback: synced %d slash command(s) GLOBALLY", len(synced))


client = AnalysisClient()


@client.event
async def on_ready():
    log.info("logged in as %s (id=%s)", client.user, client.user.id if client.user else "?")
    # Diagnostic: which guilds is the bot actually in, and is the target one of
    # them? A present target guild + a 403 on guild command sync means the
    # `applications.commands` scope is missing (re-invite needed); an absent
    # target means GUILD_ID is wrong or the bot was never added to that server.
    guilds = ", ".join(f"{g.name!r}:{g.id}" for g in client.guilds) or "(none)"
    log.info(
        "in %d guild(s): %s | target GUILD_ID=%s present=%s",
        len(client.guilds), guilds, GUILD_ID,
        any(g.id == GUILD_ID for g in client.guilds),
    )


@client.tree.command(name="analyse", description="Run a live BTMM / TDI123 / BTMM123 read on a pair")
@app_commands.describe(pair="e.g. EURUSD, XAU/USD, DE30", strategy="Which strategy to run")
@app_commands.choices(strategy=STRATEGY_CHOICES)
async def analyse(interaction: discord.Interaction, pair: str, strategy: app_commands.Choice[str]):
    await interaction.response.defer(thinking=True)

    symbol = _normalize_pair(pair)
    try:
        resp = requests.get(
            ANALYZE_URL,
            params={"pair": symbol, "strategy": strategy.value},
            timeout=REQUEST_TIMEOUT_SECS,
        )
    except requests.RequestException as e:
        log.warning("dashboard unreachable: %s", e)
        await interaction.followup.send(
            f"Dashboard service isn't reachable on `{ANALYZE_URL}` -- is app.py running?"
        )
        return

    if resp.status_code == 400:
        body = resp.json()
        error = body.get("error", "bad request")
        if "tracked_pairs" in body:
            await interaction.followup.send(f"{error}. Tracked pairs: {', '.join(body['tracked_pairs'])}")
        else:
            await interaction.followup.send(error)
        return

    if resp.status_code != 200:
        await interaction.followup.send(f"Dashboard returned an unexpected error ({resp.status_code}).")
        return

    payload = resp.json()
    embed = _build_embed(symbol, strategy.value, payload)
    await interaction.followup.send(embed=embed)


def main():
    if not DISCORD_BOT_TOKEN:
        # Logged (not just printed) so this is visible in discord_bot.log even
        # when launched headless via pythonw/the guardian, which has no console.
        log.error(
            "DISCORD_BOT_TOKEN is not set in .env -- create a bot application in the "
            "Discord Developer Portal, invite it to your server (applications.commands "
            "+ bot scopes), and add its token before starting this script."
        )
        raise SystemExit(1)
    try:
        # log_handler=None: prevent discord.py from installing its own
        # StreamHandler on the root logger (it would duplicate every line
        # through our RotatingFileHandler-backed logger above). Errors during
        # run() (e.g. an invalid token) print a traceback to stderr by
        # default, which pythonw silently discards -- catch and log explicitly
        # so a bad token is actually diagnosable from discord_bot.log.
        client.run(DISCORD_BOT_TOKEN, log_handler=None)
    except discord.LoginFailure:
        log.error("Discord rejected the bot token (LoginFailure) -- it's invalid, "
                   "was reset since being copied, or has extra whitespace/characters. "
                   "Re-copy it from the Bot tab in the Developer Portal.")
        raise SystemExit(1)
    except Exception:
        log.exception("discord_bot crashed during client.run()")
        raise


if __name__ == "__main__":
    main()
