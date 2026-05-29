"""
Asynchronous Multiplayer Tag Game - Autonomous AI Bot
======================================================
Connects to the tag server via standard TCP (same JSON protocol as a human client)
and autonomously chases or evades other players using simple pathfinding.
"""

import socket
import json
import time
import math
import threading
import queue
import random

# ── Server config ──────────────────────────────────────────────────────────────
HOST = "127.0.0.1"
PORT = 5555

# ── World constants (must match client.py) ─────────────────────────────────────
WIDTH, HEIGHT = 800, 600
PLAYER_SIZE = 28

# Platform data (x, y, w, h) for basic navigation
PLATFORMS = [
    (0, 570, 800, 30),
    (100, 460, 160, 18),
    (320, 390, 160, 18),
    (540, 460, 160, 18),
    (200, 300, 120, 18),
    (480, 300, 120, 18),
    (340, 220, 120, 18),
]

# Portal centres (source → dest)
PORTAL_A_CENTER = (45, 540)
PORTAL_B_CENTER = (755, 540)

# ── Bot physics (mirror client) ────────────────────────────────────────────────
GRAVITY = 0.6
JUMP_FORCE = -14
MOVE_SPEED = 5
FRICTION = 0.75
MAX_FALL = 18

# ── AI tuning ──────────────────────────────────────────────────────────────────
SEND_INTERVAL = 0.04  # seconds between move packets (~25 Hz)
EVASION_RADIUS = 150  # pixels – flee if 'it' player is closer than this
CHASE_JUMP_THRESH = 50  # jump if target is this many pixels above bot


def distance(ax, ay, bx, by) -> float:
    return math.hypot(ax - bx, ay - by)


def rect_from(x, y):
    """Return a simple bounding box tuple for collision checks."""
    return (int(x), int(y), PLAYER_SIZE, PLAYER_SIZE)


def rects_collide(r1, r2) -> bool:
    x1, y1, w1, h1 = r1
    x2, y2, w2, h2 = r2
    return x1 < x2 + w2 and x1 + w1 > x2 and y1 < y2 + h2 and y1 + h1 > y2


class BotState:
    """Full physics state of the bot."""

    def __init__(self):
        self.x = float(random.randint(100, 700))
        self.y = 400.0
        self.dx = 0.0
        self.dy = 0.0
        self.on_ground = False
        self.portal_cd = 0.0

    def update(self) -> None:
        self.dx *= FRICTION
        if abs(self.dx) < 0.1:
            self.dx = 0

        self.dy = min(self.dy + GRAVITY, MAX_FALL)

        # X movement
        self.x += self.dx
        for px, py, pw, ph in PLATFORMS:
            if rects_collide(rect_from(self.x, self.y), (px, py, pw, ph)):
                if self.dx > 0:
                    self.x = px - PLAYER_SIZE
                elif self.dx < 0:
                    self.x = px + pw
                self.dx = 0

        # Y movement
        self.on_ground = False
        self.y += self.dy
        for px, py, pw, ph in PLATFORMS:
            if rects_collide(rect_from(self.x, self.y), (px, py, pw, ph)):
                if self.dy > 0:
                    self.y = py - PLAYER_SIZE
                    self.dy = 0
                    self.on_ground = True
                elif self.dy < 0:
                    self.y = py + ph
                    self.dy = 0

        # Trampoline (650, 552, 100, 18)
        if rects_collide(rect_from(self.x, self.y), (650, 552, 100, 18)) and self.dy > 0:
            self.dy = -22
            self.on_ground = False

        # Portals
        now = time.time()
        if now > self.portal_cd:
            if rects_collide(rect_from(self.x, self.y), (30, 540, 30, 30)):
                self.x, self.y = PORTAL_B_CENTER[0] - PLAYER_SIZE // 2, 510
                self.portal_cd = now + 1.5
            elif rects_collide(rect_from(self.x, self.y), (740, 540, 30, 30)):
                self.x, self.y = PORTAL_A_CENTER[0] - PLAYER_SIZE // 2, 510
                self.portal_cd = now + 1.5

        # Clamp
        self.x = max(0, min(WIDTH - PLAYER_SIZE, self.x))
        if self.y > HEIGHT:
            self.y = 0


class AIBot:
    """Autonomous agent that chases or evades based on game state."""

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

    # ── Network ────────────────────────────────────────────────────────────────

    def connect(self) -> bool:
        try:
            self.sock = socket.create_connection((HOST, PORT), timeout=5)
            self.sock.settimeout(None)
            print(f"[Bot] Connected to {HOST}:{PORT}")
            return True
        except Exception as exc:
            print(f"[Bot] Connection failed: {exc}")
            return False

    def _recv_loop(self) -> None:
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
                print(f"[Bot] Recv error: {exc}")
                break
        self._running = False

    def send(self, payload: dict) -> None:
        try:
            raw = (json.dumps(payload) + "\n").encode()
            self.sock.sendall(raw)
        except Exception as exc:
            print(f"[Bot] Send error: {exc}")
            self._running = False

    # ── State processing ───────────────────────────────────────────────────────

    def process_messages(self) -> None:
        while not self.inbound.empty():
            msg = self.inbound.get()
            mtype = msg.get("type")

            if mtype == "welcome":
                self.my_id = msg["your_id"]
                self.it_id = msg.get("it_id")
                self.timer = msg.get("timer", 60.0)
                print(f"[Bot] Assigned ID: {self.my_id} | 'it': {self.it_id}")

            elif mtype == "sync":
                self.it_id = msg.get("it_id")
                self.timer = msg.get("timer", self.timer)
                for pid, pdata in msg.get("players", {}).items():
                    if pid != self.my_id:
                        self.remote[pid] = pdata

            elif mtype == "tag_event":
                self.it_id = msg.get("new_it")
                print(f"[Bot] Tag event! 'it' is now {self.it_id}")

            elif mtype == "player_left":
                self.remote.pop(msg.get("id"), None)
                if msg.get("new_it"):
                    self.it_id = msg["new_it"]

            elif mtype == "round_reset":
                self.it_id = msg.get("new_it")

    # ── AI decision logic ──────────────────────────────────────────────────────

    def nearest_player(self) -> tuple[str | None, dict | None, float]:
        """Return (pid, data, dist) for the closest remote player."""
        best_pid, best_data, best_dist = None, None, float("inf")
        for pid, data in self.remote.items():
            d = distance(self.state.x, self.state.y, data["x"], data["y"])
            if d < best_dist:
                best_pid, best_data, best_dist = pid, data, d
        return best_pid, best_data, best_dist

    def decide(self) -> None:
        """Apply movement impulses based on current role."""
        s = self.state
        am_it = (self.my_id == self.it_id)

        if not self.remote:
            # Wander randomly
            if random.random() < 0.02:
                s.dx += random.choice([-MOVE_SPEED, MOVE_SPEED])
            return

        if am_it:
            # ── Chase mode ──────────────────────────────────────────────────
            target_pid, target, dist = self.nearest_player()
            if not target:
                return

            tx, ty = target["x"], target["y"]
            # Move horizontally toward target
            if tx < s.x - 5:
                s.dx -= 1.5
            elif tx > s.x + 5:
                s.dx += 1.5

            # Clamp speed
            s.dx = max(-MOVE_SPEED, min(MOVE_SPEED, s.dx))

            # Jump if target is significantly higher OR bot is stuck horizontally
            if s.on_ground and (s.y - ty > CHASE_JUMP_THRESH or abs(s.dx) < 0.5):
                s.dy = JUMP_FORCE

        else:
            # ── Evasion mode ────────────────────────────────────────────────
            it_data = self.remote.get(self.it_id)
            if not it_data:
                return

            ix, iy = it_data["x"], it_data["y"]
            dist_to_it = distance(s.x, s.y, ix, iy)

            if dist_to_it < EVASION_RADIUS:
                # Flee: move in opposite direction
                if ix < s.x:
                    s.dx += 1.5  # 'it' is to the left → go right
                else:
                    s.dx -= 1.5  # 'it' is to the right → go left

                # Jump onto platforms when threatened
                if s.on_ground:
                    s.dy = JUMP_FORCE

                # Prioritise portals when 'it' is very close
                if dist_to_it < 80:
                    portal_a_dist = distance(s.x, s.y, *PORTAL_A_CENTER)
                    portal_b_dist = distance(s.x, s.y, *PORTAL_B_CENTER)
                    if portal_a_dist < portal_b_dist:
                        if PORTAL_A_CENTER[0] < s.x:
                            s.dx -= 1.5
                        else:
                            s.dx += 1.5
                    else:
                        if PORTAL_B_CENTER[0] < s.x:
                            s.dx -= 1.5
                        else:
                            s.dx += 1.5
            else:
                # Safe: move casually to avoid standing still
                if random.random() < 0.03:
                    s.dx += random.choice([-1, 1]) * 2

            s.dx = max(-MOVE_SPEED, min(MOVE_SPEED, s.dx))

    # ── Main loop ──────────────────────────────────────────────────────────────

    def run(self) -> None:
        if not self.connect():
            return

        # Start receive thread
        recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        recv_thread.start()

        print("[Bot] Waiting for welcome message…")
        # Wait for our ID assignment
        deadline = time.time() + 5.0
        while self.my_id is None and time.time() < deadline:
            self.process_messages()
            time.sleep(0.05)

        if self.my_id is None:
            print("[Bot] Did not receive welcome. Exiting.")
            return

        print(f"[Bot] Running as {self.my_id}. Press Ctrl+C to stop.")

        try:
            while self._running:
                loop_start = time.time()

                self.process_messages()
                self.decide()
                self.state.update()

                # Send position at controlled rate
                now = time.time()
                if now - self._last_send >= SEND_INTERVAL:
                    self.send({
                        "action": "move",
                        "x": round(self.state.x, 1),
                        "y": round(self.state.y, 1),
                        "dx": round(self.state.dx, 2),
                        "dy": round(self.state.dy, 2),
                    })
                    self._last_send = now

                # Maintain ~60 Hz loop
                elapsed = time.time() - loop_start
                sleep_time = max(0, (1 / 60) - elapsed)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("\n[Bot] Shutting down.")
        finally:
            self._running = False
            if self.sock:
                self.sock.close()


if __name__ == "__main__":
    AIBot().run()
