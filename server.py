"""
title: server/client project - Async Tag Server
author: Yves Alon Numa
date: 4.6.2026
description: this is the asynchronous tag game server code
"""
import asyncio
import json
import time
import math
import logging
from datetime import datetime

HOST = "127.0.0.1"
PORT = 5555
ROUND_DURATION = 60.0
GAME_OVER_DURATION = 5.0
TAG_COOLDOWN = 2.0
TAG_RADIUS = 40
TICK_RATE = 0.05

players: dict[str, dict] = {}
it_id: str | None = None
game_state = "PLAYING"
game_over_until = 0.0
round_timer: float = ROUND_DURATION
round_start: float = time.time()
tag_cooldown_until: float = 0.0
player_counter = 0


def get_timestamp() -> str:
    """
    the function that returns the current formatted timestamp string
    :return: the wanted timestamp string
    """
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def generate_player_id() -> str:
    """
    the function that generates a unique new player ID
    :return: the wanted player id string
    """
    global player_counter
    player_counter += 1
    return f"p{player_counter}"


def calculate_distance(a: dict, b: dict) -> float:
    """
    the function that calculates distance between two player positions
    :param a:
    :param b:
    :return: the wanted distance floating value
    """
    return math.hypot(a["x"] - b["x"], a["y"] - b["y"])


def get_public_state() -> dict:
    """
    the function that returns serialisable snapshot of all players
    :return: the wanted dict of players snapshot
    """
    return {
        pid: {k: v for k, v in data.items() if k not in ("writer", "lock")}
        for pid, data in players.items()
    }


async def broadcast_to_all(payload: dict) -> None:
    """
    the function that sends a JSON line to every connected client safely
    :param payload:
    :return: send to the clients the wanted payload data
    """
    raw = (json.dumps(payload) + "\n").encode()
    for pid, data in list(players.items()):
        try:
            async with data["lock"]:
                data["writer"].write(raw)
                await data["writer"].drain()
        except Exception as exc:
            logging.error("Server Broadcast Error: " + str(exc))


async def send_to_player(player_id: str, payload: dict) -> None:
    """
    the function that sends a message to a single player safely
    :param player_id:
    :param payload:
    :return: send to the single client the wanted payload data
    """
    data = players.get(player_id)
    if not data:
        return
    try:
        raw = (json.dumps(payload) + "\n").encode()
        async with data["lock"]:
            data["writer"].write(raw)
            await data["writer"].drain()
    except Exception as exc:
        logging.error("Server direct send failed: " + str(exc))


def check_player_tags() -> None:
    """
    the function that validates tag collisions and transfers it state
    :return: message if the command failed or succeed
    """
    global it_id, tag_cooldown_until

    try:
        if game_state != "PLAYING" or it_id is None or time.time() < tag_cooldown_until:
            return
        it_player = players.get(it_id)
        if not it_player:
            return

        for pid, pdata in players.items():
            if pid == it_id:
                continue
            if calculate_distance(it_player, pdata) <= TAG_RADIUS:
                old_it = it_id
                it_id = pid
                tag_cooldown_until = time.time() + TAG_COOLDOWN
                logging.info(f"TAG event succeeded: {old_it} tagged {pid}")
                asyncio.create_task(
                    broadcast_to_all({
                        "type": "tag_event",
                        "new_it": pid,
                        "old_it": old_it,
                        "cooldown": TAG_COOLDOWN,
                    })
                )
                break
    except Exception as e:
        logging.error(f"check_player_tags failed: {e}")


async def run_game_loop() -> None:
    """
    the function that periodically broadcasts world state and manages the round timer
    :return:
    """
    global round_timer, it_id, round_start, game_state, game_over_until

    logging.info("Game loop loop started")
    while True:
        try:
            await asyncio.sleep(TICK_RATE)

            if game_state == "PLAYING":
                round_timer = max(0.0, ROUND_DURATION - (time.time() - round_start))
                if players:
                    check_player_tags()

                if round_timer <= 0.0 and players:
                    logging.info("Round over state triggered")
                    game_state = "GAME_OVER"
                    game_over_until = time.time() + GAME_OVER_DURATION
                    await broadcast_to_all({"type": "game_over", "loser_id": it_id})

            elif game_state == "GAME_OVER":
                round_timer = 0.0
                if time.time() >= game_over_until:
                    logging.info("Restarting round state triggered")
                    game_state = "PLAYING"
                    round_start = time.time()
                    round_timer = ROUND_DURATION
                    if players:
                        it_id = next(iter(players))
                    await broadcast_to_all({"type": "round_reset", "new_it": it_id})

            if players:
                payload = {
                    "type": "sync",
                    "players": get_public_state(),
                    "it_id": it_id,
                    "timer": round(round_timer, 2),
                }
                await broadcast_to_all(payload)
        except Exception as e:
            logging.error(f"game_loop encountered error: {e}")


async def handle_client_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """
    the function that handles the client logic and receives client actions
    :param reader:
    :param writer:
    :return:
    """
    global it_id

    try:
        addr = writer.get_extra_info("peername")
        pid = generate_player_id()

        logging.info(f"New client connected from address: {addr} assigned id: {pid}")

        players[pid] = {
            "x": 100,
            "y": 400,
            "dx": 0,
            "dy": 0,
            "writer": writer,
            "lock": asyncio.Lock()
        }

        if it_id is None:
            it_id = pid
            logging.info(f"First client {pid} is now the designated it")

        await send_to_player(pid, {
            "type": "welcome",
            "your_id": pid,
            "it_id": it_id,
            "timer": round(round_timer, 2),
            "players": get_public_state(),
        })

        await broadcast_to_all({"type": "player_joined", "id": pid})

        buffer = b""
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break

            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError as exc:
                    logging.error("Bad JSON structure from client: " + str(exc))
                    continue

                if msg.get("action") == "move" and game_state == "PLAYING":
                    p = players.get(pid)
                    if p:
                        p["x"] = msg.get("x", p["x"])
                        p["y"] = msg.get("y", p["y"])
                        p["dx"] = msg.get("dx", 0)
                        p["dy"] = msg.get("dy", 0)

    except (asyncio.IncompleteReadError, ConnectionResetError, OSError) as exc:
        logging.error(f"Connection error occurred for {pid}: {exc}")
    finally:
        players.pop(pid, None)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        logging.info(f"Client {pid} disconnected successfully")

        if it_id == pid:
            it_id = next(iter(players), None)
            logging.info(f"it role reallocated to {it_id}")

        await broadcast_to_all({"type": "player_left", "id": pid, "new_it": it_id})


async def main():
    """
    Main function for the server that boots the network server and tasks
    """
    server = await asyncio.start_server(handle_client_connection, HOST, PORT)
    logging.info("Tag Game Server listening initiated")

    asyncio.create_task(run_game_loop())

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(filename="server.log",
                        format='%(asctime)s %(message)s',
                        filemode='w')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    """
    the asserts make a client so in the game it starts at player 2
    """
    assert get_timestamp() is not None, "assert test failed"
    assert "p" in generate_player_id(), "assert test failed"
    assert calculate_distance({"x": 0, "y": 0}, {"x": 3, "y": 4}) == 5.0, "assert test failed"
    assert isinstance(get_public_state(), dict), "assert test failed"
    assert handle_client_connection is not None, "assert test failed"
    assert broadcast_to_all is not None, "assert test failed"
    assert send_to_player is not None, "assert test failed"

    logging.info("all the asserts passed")
    asyncio.run(main())