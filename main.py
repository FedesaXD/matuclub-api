import os
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import Optional

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CLUBS = {
    "1": "#282Y2LR8R",
    "2": "#2Y9GY220C",
    "3": "#2VG0RQ299",
    "4": "#2LLQ8VR2Q"
}

def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"), connect_timeout=10)


# ── EXISTING ENDPOINTS (unchanged) ───────────────────────────────────────────

@app.get("/player/{player_tag}")
def ver_datos(player_tag: str):
    if not player_tag.startswith("#"):
        player_tag = "#" + player_tag

    conn = get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT name, highest_trophies, wins3v3, winsSolo,
                   total_prestige, highestWinstreak, maxWsBrawler, club_tag, club_name, icon_url
            FROM players WHERE tag = %s
        """, (player_tag,))

        result = cursor.fetchone()
        if not result:
            return {"error": "Jugador no encontrado"}

        (name, highest_trophies, wins3v3, winsSolo,
         total_prestige, highest_ws, ws_brawler, club_tag, club_name, icon_url) = result

        cursor.execute("""
            SELECT timestamp, trophies, wins3v3, winsSolo, total_prestige
            FROM player_stats_history
            WHERE player_tag = %s
            ORDER BY timestamp ASC
        """, (player_tag,))
        history = cursor.fetchall()

        cursor.execute("""
            SELECT brawler_name, power_level, gadgets, star_powers, hipercharge, trophies
            FROM player_brawlers
            WHERE player_tag = %s
            ORDER BY trophies DESC
            LIMIT 12
        """, (player_tag,))
        brawlers = cursor.fetchall()

        return {
            "name": name,
            "highest_trophies": highest_trophies,
            "wins3v3": wins3v3,
            "winsSolo": winsSolo,
            "total_prestige": total_prestige,
            "best_winstreak": {
                "value": highest_ws,
                "brawler": ws_brawler
            },
            "club_tag": club_tag,
            "club_name": club_name,
            "icon_url": icon_url,
            "history": [list(h) for h in history],
            "top_brawlers": [list(b) for b in brawlers]
        }
    finally:
        cursor.close()
        conn.close()


@app.get("/top/prestige")
def topPrestige():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT tag, name, total_prestige
            FROM players ORDER BY total_prestige DESC LIMIT 50
        """)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/trophies")
def topTrophies():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT tag, name, highest_trophies
            FROM players ORDER BY highest_trophies DESC LIMIT 50
        """)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/wins3v3")
def topWins3v3():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT tag, name, wins3v3
            FROM players ORDER BY wins3v3 DESC LIMIT 50
        """)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/winssolo")
def topWinsSolo():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT tag, name, winsSolo
            FROM players ORDER BY winsSolo DESC LIMIT 50
        """)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/winstreak")
def topWinstreak():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT tag, name, highestWinstreak, maxWsBrawler
            FROM players ORDER BY highestWinstreak DESC LIMIT 50
        """)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v, "brawler": b}
                for i, (t, n, v, b) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/brawler-trophies")
def topBrawlerTrophies():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT p.tag, p.name, pb.brawler_name, pb.trophies
            FROM player_brawlers pb
            JOIN players p ON pb.player_tag = p.tag
            WHERE (pb.player_tag, pb.trophies) IN (
                SELECT player_tag, MAX(trophies) FROM player_brawlers GROUP BY player_tag
            )
            ORDER BY pb.trophies DESC LIMIT 50
        """)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "brawler": b, "value": v}
                for i, (t, n, b, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/brawler/{brawler_name}")
def topBrawler(brawler_name: str, club: str = None):
    brawler_name = brawler_name.strip().upper()

    conn = get_conn()
    cursor = conn.cursor()

    try:
        if club and club in CLUBS:
            club_tag = CLUBS[club]
            cursor.execute("""
                SELECT p.tag, p.name, pb.trophies
                FROM player_brawlers pb
                JOIN players p ON pb.player_tag = p.tag
                WHERE pb.brawler_name = %s AND p.club_tag = %s
                ORDER BY pb.trophies DESC
                LIMIT 12
            """, (brawler_name, club_tag))
        else:
            cursor.execute("""
                SELECT p.tag, p.name, pb.trophies
                FROM player_brawlers pb
                JOIN players p ON pb.player_tag = p.tag
                WHERE pb.brawler_name = %s
                ORDER BY pb.trophies DESC
                LIMIT 12
            """, (brawler_name,))

        rows = cursor.fetchall()

        if not rows:
            return {"error": "No hay datos"}

        return [{"rank": i+1, "tag": tag, "name": n, "trophies": t}
                for i, (tag, n, t) in enumerate(rows)]
    finally:
        cursor.close()
        conn.close()


@app.get("/club/{club_num}/members")
def clubMembers(club_num: str):
    if club_num not in CLUBS:
        return {"error": "Club no encontrado"}

    club_tag = CLUBS[club_num]
    conn = get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT tag, name, highest_trophies, icon_url
            FROM players
            WHERE club_tag = %s
            ORDER BY highest_trophies DESC
        """, (club_tag,))
        rows = cursor.fetchall()

        return [{"rank": i+1, "tag": tag, "name": n, "trophies": t, "icon_url": ico}
                for i, (tag, n, t, ico) in enumerate(rows)]
    finally:
        cursor.close()
        conn.close()


# ── EVENTS: HELPERS ───────────────────────────────────────────────────────────

def ensure_events_tables(cursor):
    """Create events tables if they don't exist. Safe to call on every request."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            reward TEXT NOT NULL,
            metric TEXT NOT NULL CHECK (metric IN ('trophies')),
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ends_at TIMESTAMPTZ NOT NULL,
            closed_at TIMESTAMPTZ,
            is_active BOOLEAN NOT NULL DEFAULT TRUE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_snapshots (
            id SERIAL PRIMARY KEY,
            event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
            player_tag TEXT NOT NULL,
            player_name TEXT NOT NULL,
            icon_url TEXT,
            trophies_start INTEGER NOT NULL,
            trophies_end INTEGER,
            UNIQUE(event_id, player_tag)
        )
    """)


def check_admin(x_admin_key: Optional[str]):
    admin_key = os.getenv("ADMIN_KEY", "")
    if not admin_key or x_admin_key != admin_key:
        raise HTTPException(status_code=403, detail="No autorizado")


def auto_close_expired(cursor):
    """Mark events as inactive if their end time has passed."""
    cursor.execute("""
        UPDATE events
        SET is_active = FALSE, closed_at = NOW()
        WHERE is_active = TRUE AND ends_at <= NOW()
    """)


def compute_results(cursor, event_id: int):
    """Return ranked leaderboard for an event."""
    cursor.execute("""
        SELECT
            es.player_tag,
            es.player_name,
            es.icon_url,
            es.trophies_start,
            COALESCE(es.trophies_end, p.highest_trophies) AS trophies_now,
            COALESCE(es.trophies_end, p.highest_trophies) - es.trophies_start AS delta
        FROM event_snapshots es
        LEFT JOIN players p ON es.player_tag = p.tag
        WHERE es.event_id = %s
        ORDER BY delta DESC
    """, (event_id,))
    rows = cursor.fetchall()
    return [
        {
            "rank": i + 1,
            "tag": tag,
            "name": name,
            "icon_url": ico,
            "trophies_start": ts,
            "trophies_now": tn,
            "delta": delta
        }
        for i, (tag, name, ico, ts, tn, delta) in enumerate(rows)
    ]


# ── EVENTS: PUBLIC ENDPOINTS ─────────────────────────────────────────────────

@app.get("/events")
def getEvents():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_events_tables(cursor)
        auto_close_expired(cursor)
        conn.commit()

        cursor.execute("""
            SELECT id, title, description, reward, metric,
                   started_at, ends_at, closed_at, is_active
            FROM events
            ORDER BY started_at DESC
        """)
        rows = cursor.fetchall()

        result = []
        for (eid, title, desc, reward, metric, started_at, ends_at, closed_at, is_active) in rows:
            participants = compute_results(cursor, eid)
            result.append({
                "id": eid,
                "title": title,
                "description": desc,
                "reward": reward,
                "metric": metric,
                "started_at": started_at.isoformat() if started_at else None,
                "ends_at": ends_at.isoformat() if ends_at else None,
                "closed_at": closed_at.isoformat() if closed_at else None,
                "is_active": is_active,
                "participants": participants
            })
        return result
    finally:
        cursor.close()
        conn.close()


# ── EVENTS: ADMIN ENDPOINTS ──────────────────────────────────────────────────

class CreateEventBody(BaseModel):
    title: str
    description: Optional[str] = None
    reward: str
    metric: str = "trophies"
    duration_hours: float


@app.post("/events")
def createEvent(body: CreateEventBody, x_admin_key: Optional[str] = Header(None)):
    check_admin(x_admin_key)

    if body.metric not in ("trophies",):
        raise HTTPException(status_code=400, detail="metric invalida. Valores válidos: trophies")
    if body.duration_hours <= 0:
        raise HTTPException(status_code=400, detail="duration_hours debe ser mayor a 0")

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_events_tables(cursor)

        # Close any currently active event before creating a new one
        cursor.execute("""
            UPDATE events SET is_active = FALSE, closed_at = NOW()
            WHERE is_active = TRUE
        """)

        cursor.execute("""
            INSERT INTO events (title, description, reward, metric, ends_at)
            VALUES (%s, %s, %s, %s, NOW() + INTERVAL '1 hour' * %s)
            RETURNING id
        """, (body.title, body.description, body.reward, body.metric, body.duration_hours))
        event_id = cursor.fetchone()[0]

        # Snapshot current trophies for all tracked players
        cursor.execute("""
            INSERT INTO event_snapshots (event_id, player_tag, player_name, icon_url, trophies_start)
            SELECT %s, tag, name, icon_url, highest_trophies
            FROM players
        """, (event_id,))

        conn.commit()
        return {"ok": True, "event_id": event_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.patch("/events/{event_id}/close")
def closeEvent(event_id: int, x_admin_key: Optional[str] = Header(None)):
    check_admin(x_admin_key)

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_events_tables(cursor)

        cursor.execute("SELECT is_active FROM events WHERE id = %s", (event_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Evento no encontrado")
        if not row[0]:
            raise HTTPException(status_code=400, detail="El evento ya está cerrado")

        # Freeze final trophies for all participants
        cursor.execute("""
            UPDATE event_snapshots es
            SET trophies_end = p.highest_trophies
            FROM players p
            WHERE es.player_tag = p.tag AND es.event_id = %s
        """, (event_id,))

        cursor.execute("""
            UPDATE events SET is_active = FALSE, closed_at = NOW()
            WHERE id = %s
        """, (event_id,))

        conn.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
