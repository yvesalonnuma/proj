"""
Asynchronous Multiplayer Tag Game - Pygame Client
==================================================
Connects to the tag game server, renders the game at 60 FPS,
and handles local physics + network synchronisation via a daemon thread.
"""

import pygame
import socket
import threading
import queue
import json
import time
import sys

# ── Network config ─────────────────────────────────────────────────────────────
HOST = "127.0.0.1"
PORT = 5555

# ── Display / FPS ──────────────────────────────────────────────────────────────
WIDTH, HEIGHT = 800, 600
FPS = 60

# ── Physics ────────────────────────────────────────────────────────────────────
GRAVITY = 0.6
JUMP_FORCE = -14
MOVE_SPEED = 5
FRICTION = 0.75
MAX_FALL = 18

# ── Colors (neon dark palette) ─────────────────────────────────────────────────
BG_COLOR = (10, 10, 20)
PLATFORM_COLOR = (40, 40, 70)
PLATFORM_EDGE = (80, 80, 140)
PLAYER_COLOR = (0, 200, 255)
IT_COLOR = (255, 50, 50)
REMOTE_COLOR = (100, 220, 100)
TEXT_COLOR = (220, 220, 255)
PORTAL_COLOR_A = (180, 0, 255)
PORTAL_COLOR_B = (0, 255, 200)
TRAMPOLINE_CLR = (255, 200, 0)
HUD_BG = (0, 0, 0, 160)

# ── Map layout ─────────────────────────────────────────────────────────────────
PLATFORMS = [
    pygame.Rect(0, 570, 800, 30),  # ground
    pygame.Rect(100, 460, 160, 18),
    pygame.Rect(320, 390, 160, 18),
    pygame.Rect(540, 460, 160, 18),
    pygame.Rect(200, 300, 120, 18),
    pygame.Rect(480, 300, 120, 18),
    pygame.Rect(340, 220, 120, 18),
]

TRAMPOLINE = pygame.Rect(650, 552, 100, 18)
TRAMPOLINE_BOOST = -22

PORTAL_A = pygame.Rect(30, 540, 30, 30)
PORTAL_B = pygame.Rect(740, 540, 30, 30)
PORTAL_COOLDOWN = 1.5


class Player:
    SIZE = 28

    def __init__(self, pid: str):
        self.pid = pid
        self.x = 100.0
        self.y = 400.0
        self.dx = 0.0
        self.dy = 0.0
        self.on_ground = False
        self.portal_cd = 0.0

    @property
    def rect(self) -> pygame.Rect:
        return pygame.Rect(int(self.x), int(self.y), self.SIZE, self.SIZE)

    def handle_input(self) -> None:
        keys = pygame.key.get_pressed()
        if keys[pygame.K_LEFT] or keys[pygame.K_a]:
            self.dx -= 1.5
        if keys[pygame.K_RIGHT] or keys[pygame.K_d]:
            self.dx += 1.5
        if (keys[pygame.K_SPACE] or keys[pygame.K_UP] or keys[pygame.K_w]) and self.on_ground:
            self.dy = JUMP_FORCE
            self.on_ground = False

    def update(self) -> None:
        self.dx *= FRICTION
        if abs(self.dx) < 0.1:
            self.dx = 0

        self.dy = min(self.dy + GRAVITY, MAX_FALL)

        self.x += self.dx
        for plat in PLATFORMS:
            if self.rect.colliderect(plat):
                if self.dx > 0:
                    self.x = plat.left - self.SIZE
                elif self.dx < 0:
                    self.x = plat.right
                self.dx = 0

        self.on_ground = False
        self.y += self.dy
        for plat in PLATFORMS:
            if self.rect.colliderect(plat):
                if self.dy > 0:
                    self.y = plat.top - self.SIZE
                    self.dy = 0
                    self.on_ground = True
                elif self.dy < 0:
                    self.y = plat.bottom
                    self.dy = 0

        if self.rect.colliderect(TRAMPOLINE) and self.dy > 0:
            self.dy = TRAMPOLINE_BOOST
            self.on_ground = False

        now = time.time()
        if now > self.portal_cd:
            if self.rect.colliderect(PORTAL_A):
                self.x, self.y = PORTAL_B.centerx - self.SIZE // 2, PORTAL_B.top - self.SIZE
                self.portal_cd = now + PORTAL_COOLDOWN
            elif self.rect.colliderect(PORTAL_B):
                self.x, self.y = PORTAL_A.centerx - self.SIZE // 2, PORTAL_A.top - self.SIZE
                self.portal_cd = now + PORTAL_COOLDOWN

        self.x = max(0, min(WIDTH - self.SIZE, self.x))
        if self.y > HEIGHT:
            self.y = 0


class NetworkThread(threading.Thread):
    def __init__(self, host: str, port: int):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.sock: socket.socket | None = None
        self.inbound: queue.Queue = queue.Queue()
        self.outbound: queue.Queue = queue.Queue()
        self.connected = False
        self._buf = b""

    def run(self) -> None:
        try:
            self.sock = socket.create_connection((self.host, self.port), timeout=5)
            self.sock.settimeout(None)
            self.connected = True
            print(f"[Net] Connected to {self.host}:{self.port}")
        except Exception as exc:
            print(f"[Net] Could not connect: {exc}")
            return

        send_thread = threading.Thread(target=self._sender, daemon=True)
        send_thread.start()

        while True:
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
                print(f"[Net] Recv error: {exc}")
                break

        self.connected = False
        print("[Net] Disconnected.")

    def _sender(self) -> None:
        while self.connected:
            try:
                msg = self.outbound.get(timeout=0.1)
                raw = (json.dumps(msg) + "\n").encode()
                self.sock.sendall(raw)
            except queue.Empty:
                pass
            except Exception as exc:
                print(f"[Net] Send error: {exc}")
                break

    def send(self, payload: dict) -> None:
        if self.connected:
            self.outbound.put(payload)


class TagClient:
    def __init__(self):
        pygame.init()
        self.screen = pygame.display.set_mode((WIDTH, HEIGHT))
        pygame.display.set_caption("Multiplayer Tag – Neon Edition")
        self.clock = pygame.time.Clock()

        self.font_hud = pygame.font.SysFont("consolas", 20, bold=True)
        self.font_big = pygame.font.SysFont("consolas", 48, bold=True)
        self.font_sm = pygame.font.SysFont("consolas", 14)

        self.net = NetworkThread(HOST, PORT)
        self.net.start()

        self.my_id: str | None = None
        self.local: Player | None = None
        self.remote_players: dict[str, dict] = {}
        self.it_id: str | None = None
        self.timer: float = 60.0
        self.last_send = 0.0
        self.send_rate = 0.04

        # תוספות עבור מסך הסיום (Game Over)
        self.game_over = False
        self.end_message = ""
        self.end_color = TEXT_COLOR

        self._glow_phase = 0.0

    def process_messages(self) -> None:
        while not self.net.inbound.empty():
            msg = self.net.inbound.get()
            mtype = msg.get("type")

            if mtype == "welcome":
                self.my_id = msg["your_id"]
                self.it_id = msg.get("it_id")
                self.timer = msg.get("timer", 60.0)
                self.local = Player(self.my_id)
                print(f"[Game] I am {self.my_id}, 'it' is {self.it_id}")

            elif mtype == "sync":
                self.it_id = msg.get("it_id")
                # אל תעדכן טיימר אם המשחק נגמר כדי שיישאר על 0
                if not self.game_over:
                    self.timer = msg.get("timer", self.timer)
                for pid, pdata in msg.get("players", {}).items():
                    if pid != self.my_id:
                        self.remote_players[pid] = pdata

            elif mtype == "tag_event":
                self.it_id = msg.get("new_it")
                print(f"[Game] Tag! New 'it': {self.it_id}")

            elif mtype == "player_left":
                gone = msg.get("id")
                self.remote_players.pop(gone, None)
                if msg.get("new_it"):
                    self.it_id = msg["new_it"]

            # --- הלוגיקה החדשה של סיום המשחק ---
            elif mtype == "game_over":
                self.game_over = True
                self.timer = 0.0
                loser_id = msg.get("loser_id")
                if self.my_id == loser_id:
                    self.end_message = "YOU LOST! YOU WERE IT!"
                    self.end_color = IT_COLOR
                else:
                    self.end_message = "YOU SURVIVED! YOU WIN!"
                    self.end_color = PLAYER_COLOR

            elif mtype == "round_reset":
                self.game_over = False
                self.it_id = msg.get("new_it")
                self.timer = 60.0
                print("[Game] New round started!")

    def draw_platforms(self) -> None:
        for plat in PLATFORMS:
            pygame.draw.rect(self.screen, PLATFORM_COLOR, plat)
            pygame.draw.rect(self.screen, PLATFORM_EDGE, plat, 2)

        pygame.draw.rect(self.screen, TRAMPOLINE_CLR, TRAMPOLINE)
        label = self.font_sm.render("TRAMPOLINE", True, (0, 0, 0))
        self.screen.blit(label, (TRAMPOLINE.x + 2, TRAMPOLINE.y + 2))

        for rect, color in [(PORTAL_A, PORTAL_COLOR_A), (PORTAL_B, PORTAL_COLOR_B)]:
            pygame.draw.ellipse(self.screen, color, rect)
            pygame.draw.ellipse(self.screen, (255, 255, 255), rect, 2)

    def draw_player(self, x: float, y: float, pid: str, is_local: bool = False) -> None:
        size = Player.SIZE
        rect = pygame.Rect(int(x), int(y), size, size)
        is_it = (pid == self.it_id)

        color = IT_COLOR if is_it else (PLAYER_COLOR if is_local else REMOTE_COLOR)
        pygame.draw.rect(self.screen, color, rect, border_radius=6)

        if is_it:
            glow_alpha = int(128 + 127 * abs(
                (self._glow_phase % 1.0) * 2 - 1
            ))
            glow_surf = pygame.Surface((size + 12, size + 12), pygame.SRCALPHA)
            glow_color = (255, 60, 60, glow_alpha)
            pygame.draw.rect(glow_surf, glow_color,
                             glow_surf.get_rect(), border_radius=8, width=4)
            self.screen.blit(glow_surf, (rect.x - 6, rect.y - 6))

            lbl = self.font_hud.render("IT", True, IT_COLOR)
            self.screen.blit(lbl, (rect.centerx - lbl.get_width() // 2, rect.top - 22))

        id_lbl = self.font_sm.render(pid, True, TEXT_COLOR)
        self.screen.blit(id_lbl, (rect.centerx - id_lbl.get_width() // 2, rect.bottom + 2))

    def draw_hud(self) -> None:
        hud = pygame.Surface((WIDTH, 36), pygame.SRCALPHA)
        hud.fill((0, 0, 0, 160))
        self.screen.blit(hud, (0, 0))

        timer_txt = self.font_hud.render(f"⏱ {int(self.timer):02d}s", True, TEXT_COLOR)
        self.screen.blit(timer_txt, (WIDTH // 2 - timer_txt.get_width() // 2, 8))

        if self.my_id:
            id_txt = self.font_sm.render(f"You: {self.my_id}", True, PLAYER_COLOR)
            self.screen.blit(id_txt, (8, 10))

        if self.it_id:
            color = IT_COLOR if self.it_id == self.my_id else TEXT_COLOR
            it_txt = self.font_sm.render(f"IT: {self.it_id}", True, color)
            self.screen.blit(it_txt, (WIDTH - it_txt.get_width() - 8, 10))

        if not self.net.connected:
            banner = self.font_big.render("Connecting...", True, (255, 200, 0))
            self.screen.blit(banner, (WIDTH // 2 - banner.get_width() // 2, HEIGHT // 2 - 20))

    def run(self) -> None:
        running = True
        while running:
            dt = self.clock.tick(FPS) / 1000.0
            self._glow_phase += dt * 1.5

            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                if event.type == pygame.KEYDOWN and event.key == pygame.K_ESCAPE:
                    running = False

            self.process_messages()

            # פיזיקה מקומית (תפעל רק אם המשחק עדיין רץ)
            if self.local and not self.game_over:
                self.local.handle_input()
                self.local.update()

                now = time.time()
                if now - self.last_send >= self.send_rate:
                    self.net.send({
                        "action": "move",
                        "x": round(self.local.x, 1),
                        "y": round(self.local.y, 1),
                        "dx": round(self.local.dx, 2),
                        "dy": round(self.local.dy, 2),
                    })
                    self.last_send = now

            # ── Render ─────────────────────────────────────────────────────
            self.screen.fill(BG_COLOR)
            self.draw_platforms()

            for pid, pdata in self.remote_players.items():
                self.draw_player(pdata["x"], pdata["y"], pid, is_local=False)

            if self.local:
                self.draw_player(self.local.x, self.local.y, self.my_id, is_local=True)

            self.draw_hud()

            # ציור מסך סיום אם המשחק נגמר
            if self.game_over:
                overlay = pygame.Surface((WIDTH, HEIGHT), pygame.SRCALPHA)
                overlay.fill((0, 0, 0, 200))  # מסך כהה חצי-שקוף
                self.screen.blit(overlay, (0, 0))

                title = self.font_big.render("TIME'S UP!", True, (255, 255, 255))
                self.screen.blit(title, (WIDTH // 2 - title.get_width() // 2, HEIGHT // 2 - 60))

                msg_surf = self.font_hud.render(self.end_message, True, self.end_color)
                self.screen.blit(msg_surf, (WIDTH // 2 - msg_surf.get_width() // 2, HEIGHT // 2))

                wait_txt = self.font_sm.render("Waiting for server to restart round...", True, TEXT_COLOR)
                self.screen.blit(wait_txt, (WIDTH // 2 - wait_txt.get_width() // 2, HEIGHT - 60))

            pygame.display.flip()

        pygame.quit()
        sys.exit()


if __name__ == "__main__":
    TagClient().run()
