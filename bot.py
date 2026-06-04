"""
title: server/client project - Autonomous Bot Client
author: Yves Alon Numa
date: 4/6/2026
description: this is the automated bot code which connects to the tag server
"""
import socket
import json
import time
import math
import threading
import queue
import random
import logging

HOST = "127.0.0.1"
PORT = 5555
WIDTH, HEIGHT = 800, 600
PLAYER_SIZE = 28

PLATFORMS = [
    (0, 570, 800, 30),
    (100, 460, 160, 18),
    (320, 390, 160, 18),
    (540, 460, 160, 18),
    (200, 300, 120, 18),
    (480, 300, 120, 18),
    (340, 220, 120, 18),
]

PORTAL_A_CENTER = (45, 540)
PORTAL_B_CENTER = (755, 540)

GRAVITY = 0.6
JUMP_FORCE = -14
MOVE_SPEED = 5
FRICTION = 0.75
MAX_FALL = 18

SEND_INTERVAL = 0.04
EVASION_RADIUS = 150
CHASE_JUMP_THRESH = 50


def calculate_distance(ax, ay, bx, by) -> float:
    """
    the function that calculates the distance between two coordinate pairs
    :param ax:
    :param ay:
    :param bx:
    :param by:
    :return: the distance value
    """
    return math.hypot(ax - bx, ay - by)


def create_bounding_box(x, y):
    """
    the function that creates a bounding dimension tuple matching player dimensions
    :param x:
    :param y:
    :return: the created bounding box tuple elements
    """
    return (int(x), int(y), PLAYER_SIZE, PLAYER_SIZE)


def check_rect_collision(r1, r2) -> bool:
    """
    the function that intersects two rectangular elements and gives visibility boolean status
    :param r1:
    :param r2:
    :return: True if hit otherwise False
    """
    x1, y1, w1, h1 = r1
    x2, y2, w2, h2 = r2
    return x1 < x2 + w2 and x1 + w1 > x2 and y1 < y2 + h2 and y1 + h1 > y2


class BotState:
    def __init__(self):
        self.x = float(random.randint(100, 700))
        self.y = 400.0
        self.dx = 0.0
        self.dy = 0.0
        self.on_ground = False
        self.portal_cd = 0.0

    def update_bot_physics(self) -> None:
        """
        the function that calculates internal positions and platforms response mechanics
        :return:
        """
        try:
            self.dx *= FRICTION
            if abs(self.dx) < 0.1:
                self.dx = 0

            self.dy = min(self.dy + GRAVITY, MAX_FALL)

            self.x += self.dx
            for px, py, pw, ph in PLATFORMS:
                if check_rect_collision(create_bounding_box(self.x, self.y), (px, py, pw, ph)):
                    if self.dx > 0:
                        self.x = px - PLAYER_SIZE
                    elif self.dx < 0:
                        self.x = px + pw
                    self.dx = 0

            self.on_ground = False
            self.y += self.dy
            for px, py, pw, ph in PLATFORMS:
                if check_rect_collision(create_bounding_box(self.x, self.y), (px, py, pw, ph)):
                    if self.dy > 0:
                        self.y = py - PLAYER_SIZE
                        self.dy = 0
                        self.on_ground = True
                    elif self.dy < 0:
                        self.y = py + ph
                        self.dy = 0

            if check_rect_collision(create_bounding_box(self.x, self.y), (650, 552, 100, 18)) and self.dy > 0:
                self.dy = -22
                self.on_ground = False

            now = time.time()
            if now > self.portal_cd:
                if check_rect_collision(create_bounding_box(self.x, self.y), (30, 540, 30, 30)):
                    self.x, self.y = PORTAL_B_CENTER[0] - PLAYER_SIZE // 2, 510
                    self.portal_cd = now + 1.5
                elif check_rect_collision(create_bounding_box(self.x, self.y), (740, 540, 30, 30)):
                    self.x, self.y = PORTAL_A_CENTER[0] - PLAYER_SIZE // 2, 510
                    self.portal_cd = now + 1.5

            self.x = max(0, min(WIDTH - PLAYER_SIZE, self.x))
            if self.y > HEIGHT:
                self.y = 0
        except Exception as e:
            logging.error(f"Bot state update failed: {e}")


class AIBot:
    def __init__(self):
        self.sock: socket.socket | None = None
        self.my_id: str | None = None
        self.it_id: str | None = None
        self.timer: float = 60.0
        self.remote: dict[str, dict] = {}
        self.state = BotState()
        self.inbound: queue.Queue = queue.Queue()
        self._running = True
        self._buf = b""
        self._last_send = 0.0

    def connect_to_server(self) -> bool:
        """
        the function that triggers connection pipeline to socket destination host
        :return: True if active, False if error
        """
        try:
            self.sock = socket.create_connection((HOST, PORT), timeout=5)
            self.sock.settimeout(None)
            logging.info("Bot socket connection launched successfully")
            return True
        except Exception as exc:
            logging.error("Bot connection routine failed: " + str(exc))
            return False

    def receive_messages_loop(self) -> None:
        """
        the function that runs receiving task extracting newline lines constantly
        :return:
        """
        while self._running:
            try:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                self._buf += chunk
                while b"\n" in self._buf:
                    line, self._buf = self._buf.split(b"\n", 1)
                    line = line.strip()
                    if line:
                        try:
                            self.inbound.put(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            except Exception as exc:
                logging.error("Receive loop error: " + str(exc))
                break
        self._running = False

    def send_to_server(self, payload: dict) -> None:
        """
        the function that pushes a text line buffer encoded to socket destination
        :param payload:
        :return:
        """
        try:
            raw = (json.dumps(payload) + "\n").encode()
            self.sock.sendall(raw)
        except Exception as exc:
            logging.error("Bot direct send failed: " + str(exc))
            self._running = False

    def handle_incoming_messages(self) -> None:
        """
        the function that scans inbound queue data buffers and updates variables maps
        :return:
        """
        while not self.inbound.empty():
            msg = self.inbound.get()
            mtype = msg.get("type")

            if mtype == "welcome":
                self.my_id = msg["your_id"]
                self.it_id = msg.get("it_id")
                self.timer = msg.get("timer", 60.0)
                logging.info(f"Bot handshake welcome sequence accepted as {self.my_id}")

            elif mtype == "sync":
                self.it_id = msg.get("it_id")
                self.timer = msg.get("timer", self.timer)
                for pid, pdata in msg.get("players", {}).items():
                    if pid != self.my_id:
                        self.remote[pid] = pdata

            elif mtype == "tag_event":
                self.it_id = msg.get("new_it")

            elif mtype == "player_left":
                self.remote.pop(msg.get("id"), None)
                if msg.get("new_it"):
                    self.it_id = msg["new_it"]

            elif mtype == "round_reset":
                self.it_id = msg.get("new_it")

    def find_closest_player(self) -> tuple[str | None, dict | None, float]:
        """
        the function that maps remote entities list and outputs the lowest distance match
        :param self:
        :return: tuple containing targeted details metadata
        """
        best_pid, best_data, best_dist = None, None, float("inf")
        for pid, data in self.remote.items():
            d = calculate_distance(self.state.x, self.state.y, data["x"], data["y"])
            if d < best_dist:
                best_pid, best_data, best_dist = pid, data, d
        return best_pid, best_data, best_dist

    def make_ai_decision(self) -> None:
        """
        the function that computes chasing tracking or evasion steering vectors
        :param self:
        :return:
        """
        s = self.state
        am_it = (self.my_id == self.it_id)

        if not self.remote:
            if random.random() < 0.02:
                s.dx += random.choice([-MOVE_SPEED, MOVE_SPEED])
            return

        if am_it:
            target_pid, target, dist = self.find_closest_player()
            if not target:
                return

            tx, ty = target["x"], target["y"]
            if tx < s.x - 5:
                s.dx -= 1.5
            elif tx > s.x + 5:
                s.dx += 1.5

            s.dx = max(-MOVE_SPEED, min(MOVE_SPEED, s.dx))

            if s.on_ground and (s.y - ty > CHASE_JUMP_THRESH or abs(s.dx) < 0.5):
                s.dy = JUMP_FORCE
        else:
            it_data = self.remote.get(self.it_id)
            if not it_data:
                return
            ix, iy = it_data["x"], it_data["y"]
            dist = calculate_distance(s.x, s.y, ix, iy)
            if dist < EVASION_RADIUS:
                if ix < s.x:
                    s.dx += 1.5
                else:
                    s.dx -= 1.5
                s.dx = max(-MOVE_SPEED, min(MOVE_SPEED, s.dx))
                if s.on_ground and random.random() < 0.05:
                    s.dy = JUMP_FORCE

    def start_bot_execution(self):
        """
        the function that initiates the loops for processing bot AI steps
        :param self:
        :return:
        """
        if not self.connect_to_server():
            return

        t = threading.Thread(target=self.receive_messages_loop, daemon=True)
        t.start()

        while self._running:
            time.sleep(0.01)
            self.handle_incoming_messages()
            self.state.update_bot_physics()
            self.make_ai_decision()

            now = time.time()
            if now - self._last_send >= SEND_INTERVAL:
                self.send_to_server({
                    "action": "move",
                    "x": round(self.state.x, 1),
                    "y": round(self.state.y, 1),
                    "dx": round(self.state.dx, 2),
                    "dy": round(self.state.dy, 2),
                })
                self._last_send = now


def main():
    """
    Main execution routine building and activating the AI client loop
    """
    bot = AIBot()
    bot.start_bot_execution()


if __name__ == "__main__":
    logging.basicConfig(filename="bot.log",
                        format='%(asctime)s %(message)s',
                        filemode='w')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    assert calculate_distance(0, 0, 3, 4) == 5.0, "assert test failed"
    assert create_bounding_box(10, 20) == (10, 20, PLAYER_SIZE, PLAYER_SIZE), "assert test failed"
    assert check_rect_collision((0, 0, 10, 10), (5, 5, 10, 10)) is True, "assert test failed"
    assert check_rect_collision((0, 0, 10, 10), (20, 20, 5, 5)) is False, "assert test failed"
    assert BotState is not None, "assert test failed"
    assert AIBot is not None, "assert test failed"

    logging.info("all the asserts passed")
    main()