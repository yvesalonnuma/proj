"""
Asynchronous Multiplayer Tag Game - Server (Fixed Edition)
==========================================================
Handles concurrent TCP client connections using asyncio.
Protocol: newline-delimited JSON messages over TCP.
"""

import asyncio
import json
import time
import math
import logging
from datetime import datetime

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, # שונה ל-INFO כדי למנוע הצפת קונסול, אפשר להחזיר ל-DEBUG במידת הצורך
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("TagServer")

# ── Constants ──────────────────────────────────────────────────────────────────
HOST = "127.0.0.1"
PORT = 5555
ROUND_DURATION = 60.0  # seconds per round
GAME_OVER_DURATION = 5.0 # כמה שניות מסך הסיום יוצג לפני סיבוב חדש
TAG_COOLDOWN = 2.0  # seconds before re-tag is allowed
TAG_RADIUS = 40  # pixel collision radius
TICK_RATE = 0.05  # server broadcast interval (20 Hz)

# ── Global game state ──────────────────────────────────────────────────────────
players: dict[str, dict] = {}  # player_id → {x, y, dx, dy, writer, lock}
it_id: str | None = None
game_state = "PLAYING"  # מצבי משחק: "PLAYING" או "GAME_OVER"
game_over_until = 0.0
round_timer: float = ROUND_DURATION
round_start: float = time.time()
tag_cooldown_until: float = 0.0
player_counter = 0


# ── Helpers ────────────────────────────────────────────────────────────────────

def timestamp() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def next_player_id() -> str:
    global player_counter
    player_counter += 1
    return f"p{player_counter}"


def distance(a: dict, b: dict) -> float:
    return math.hypot(a["x"] - b["x"], a["y"] - b["y"])


def public_state() -> dict:
    """Return serialisable snapshot of all players (no writer or lock object)."""
    return {
        pid: {k: v for k, v in data.items() if k not in ("writer", "lock")}
        for pid, data in players.items()
    }


async def broadcast(payload: dict) -> None:
    """Send a JSON line to every connected client safely using per-player locks."""
    raw = (json.dumps(payload) + "\n").encode()
    for pid, data in list(players.items()):
        try:
            # שימוש בנעילה מונע התנגשויות קצבי שידור שמנתקות שחקנים (פתרון באג השחקן השלישי)
            async with data["lock"]:
                data["writer"].write(raw)
                await data["writer"].drain()
        except Exception as exc:
            log.warning("[%s] Broadcast failed for %s: %s", timestamp(), pid, exc)


async def send_to(player_id: str, payload: dict) -> None:
    """Send a message to a single player safely using their lock."""
    data = players.get(player_id)
    if not data:
        return
    try:
        raw = (json.dumps(payload) + "\n").encode()
        async with data["lock"]:
            data["writer"].write(raw)
            await data["writer"].drain()
    except Exception as exc:
        log.warning("[%s] Direct send to %s failed: %s", timestamp(), player_id, exc)


# ── Tag logic ──────────────────────────────────────────────────────────────────

def check_tags() -> None:
    """Validate tag collisions and transfer 'it' state when appropriate."""
    global it_id, tag_cooldown_until

    # לא בודקים תיוגים אם המשחק נגמר או שיש קולדאון
    if game_state != "PLAYING" or it_id is None or time.time() < tag_cooldown_until:
        return
    it_player = players.get(it_id)
    if not it_player:
        return

    for pid, pdata in players.items():
        if pid == it_id:
            continue
        if distance(it_player, pdata) <= TAG_RADIUS:
            old_it = it_id
            it_id = pid
            tag_cooldown_until = time.time() + TAG_COOLDOWN
            log.info(
                "[%s] TAG! %s tagged %s. Cooldown until %.2f",
                timestamp(), old_it, pid, tag_cooldown_until,
            )
            asyncio.create_task(
                broadcast({
                    "type": "tag_event",
                    "new_it": pid,
                    "old_it": old_it,
                    "cooldown": TAG_COOLDOWN,
                })
            )
            break


# ── Sync broadcast loop ────────────────────────────────────────────────────────

async def game_loop() -> None:
    """Periodically broadcast world state and manage the round timer / state transitions."""
    global round_timer, it_id, round_start, game_state, game_over_until

    log.info("[%s] Game loop started.", timestamp())
    while True:
        await asyncio.sleep(TICK_RATE)

        if game_state == "PLAYING":
            round_timer = max(0.0, ROUND_DURATION - (time.time() - round_start))
            if players:
                check_tags()

            # בדיקה האם הזמן נגמר - מעבר למסך סיום
            if round_timer <= 0.0 and players:
                log.info("[%s] Round over! Sending game_over packet.", timestamp())
                game_state = "GAME_OVER"
                game_over_until = time.time() + GAME_OVER_DURATION
                await broadcast({"type": "game_over", "loser_id": it_id})

        elif game_state == "GAME_OVER":
            round_timer = 0.0
            # בדיקה האם עברו 5 שניות של מסך סיום - מתחילים מחדש
            if time.time() >= game_over_until:
                log.info("[%s] Restarting round! Sending round_reset packet.", timestamp())
                game_state = "PLAYING"
                round_start = time.time()
                round_timer = ROUND_DURATION
                if players:
                    # הגדרת השחקן הראשון ברשימה כתופס ההתחלתי של הסיבוב החדש
                    it_id = next(iter(players))
                await broadcast({"type": "round_reset", "new_it": it_id})

        # שידור מצב המשחק לכל הלקוחות המחוברים
        if players:
            payload = {
                "type": "sync",
                "players": public_state(),
                "it_id": it_id,
                "timer": round(round_timer, 2),
            }
            await broadcast(payload)


# ── Per-client handler ─────────────────────────────────────────────────────────

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    global it_id

    addr = writer.get_extra_info("peername")
    pid = next_player_id()

    log.info("[%s] New connection from %s → assigned id=%s", timestamp(), addr, pid)

    # רישום שחקן חדש עם מנגנון נעילה ייעודי למניעת קריסות רשת מהודעות מקבילות
    players[pid] = {
        "x": 100,
        "y": 400,
        "dx": 0,
        "dy": 0,
        "writer": writer,
        "lock": asyncio.Lock()
    }

    # First player becomes 'it'
    if it_id is None:
        it_id = pid
        log.info("[%s] %s is the first player — designated 'it'.", timestamp(), pid)

    # Send welcome / handshake
    await send_to(pid, {
        "type": "welcome",
        "your_id": pid,
        "it_id": it_id,
        "timer": round(round_timer, 2),
        "players": public_state(),
    })

    # Announce new player to existing clients
    await broadcast({"type": "player_joined", "id": pid})

    buffer = b""
    try:
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break  # client disconnected cleanly

            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError as exc:
                    log.warning("[%s] Bad JSON from %s: %s", timestamp(), pid, exc)
                    continue

                # ── Handle move action ──────────────────────────────────────
                if msg.get("action") == "move" and game_state == "PLAYING":
                    p = players.get(pid)
                    if p:
                        p["x"] = msg.get("x", p["x"])
                        p["y"] = msg.get("y", p["y"])
                        p["dx"] = msg.get("dx", 0)
                        p["dy"] = msg.get("dy", 0)

    except (asyncio.IncompleteReadError, ConnectionResetError, OSError) as exc:
        log.warning("[%s] Connection error for %s: %s", timestamp(), pid, exc)
    finally:
        # Clean-up on disconnect
        players.pop(pid, None)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        log.info("[%s] %s disconnected. Active players: %d", timestamp(), pid, len(players))

        # Re-assign 'it' if needed
        if it_id == pid:
            it_id = next(iter(players), None)
            log.info("[%s] 'it' re-assigned to %s", timestamp(), it_id)

        await broadcast({"type": "player_left", "id": pid, "new_it": it_id})


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    server = await asyncio.start_server(handle_client, HOST, PORT)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    log.info("Tag Game Server listening on %s", addrs)

    asyncio.create_task(game_loop())

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())