import os
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import Optional
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded


load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
# Limiter basado en IP del cliente
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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
@limiter.limit("30/minute")
def ver_datos(request: Request, player_tag: str):
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


def _club_filter(club: Optional[str]):
    """Devuelve (WHERE clause, params) para filtrar por club si corresponde."""
    if club and club in CLUBS:
        return "WHERE club_tag = %s", (CLUBS[club],)
    return "", ()


@app.get("/top/prestige")
@limiter.limit("20/minute")
def topPrestige(request: Request, club: Optional[str] = None):
    where, params = _club_filter(club)
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT tag, name, total_prestige
            FROM players {where} ORDER BY total_prestige DESC LIMIT 50
        """, params)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/trophies")
@limiter.limit("20/minute")
def topTrophies(request: Request, club: Optional[str] = None):
    where, params = _club_filter(club)
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT tag, name, highest_trophies
            FROM players {where} ORDER BY highest_trophies DESC LIMIT 50
        """, params)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/wins3v3")
@limiter.limit("20/minute")
def topWins3v3(request: Request, club: Optional[str] = None):
    where, params = _club_filter(club)
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT tag, name, wins3v3
            FROM players {where} ORDER BY wins3v3 DESC LIMIT 50
        """, params)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/winssolo")
@limiter.limit("20/minute")
def topWinsSolo(request: Request, club: Optional[str] = None):
    where, params = _club_filter(club)
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT tag, name, winsSolo
            FROM players {where} ORDER BY winsSolo DESC LIMIT 50
        """, params)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/winstreak")
@limiter.limit("20/minute")
def topWinstreak(request: Request, club: Optional[str] = None):
    where, params = _club_filter(club)
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT tag, name, highestWinstreak, maxWsBrawler
            FROM players {where} ORDER BY highestWinstreak DESC LIMIT 50
        """, params)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v, "brawler": b}
                for i, (t, n, v, b) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/brawler-trophies")
@limiter.limit("20/minute")
def topBrawlerTrophies(request: Request, club: Optional[str] = None):
    where_p = "WHERE p.club_tag = %s AND" if (club and club in CLUBS) else "WHERE"
    params_pre = (CLUBS[club],) if (club and club in CLUBS) else ()
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT p.tag, p.name, pb.brawler_name, pb.trophies
            FROM player_brawlers pb
            JOIN players p ON pb.player_tag = p.tag
            {where_p} (pb.player_tag, pb.trophies) IN (
                SELECT player_tag, MAX(trophies) FROM player_brawlers GROUP BY player_tag
            )
            ORDER BY pb.trophies DESC LIMIT 50
        """, params_pre)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "brawler": b, "value": v}
                for i, (t, n, b, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/brawler/{brawler_name}")
@limiter.limit("20/minute")
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
@limiter.limit("20/minute")
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

VALID_METRICS = ("trophies", "wins3v3", "winsSolo", "prestige", "brawler_trophies")

# Maps metric → (players column for snapshot, players column for live value)
# For brawler_trophies we query player_brawlers instead, handled separately.
METRIC_PLAYER_COL = {
    "trophies":       "highest_trophies",
    "wins3v3":        "wins3v3",
    "winsSolo":       "winsSolo",
    "prestige":       "total_prestige",
}

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


def snapshot_values(cursor, event_id: int, metric: str, brawler_name: Optional[str]):
    """Insert starting-value snapshot for all players for the given metric."""
    if metric == "brawler_trophies":
        bn = (brawler_name or "").strip().upper()
        cursor.execute("""
            INSERT INTO event_snapshots (event_id, player_tag, player_name, icon_url, value_start)
            SELECT %s, p.tag, p.name, p.icon_url, COALESCE(pb.trophies, 0)
            FROM players p
            LEFT JOIN player_brawlers pb
              ON pb.player_tag = p.tag AND UPPER(pb.brawler_name) = %s
        """, (event_id, bn))
    else:
        col = METRIC_PLAYER_COL[metric]
        cursor.execute(f"""
            INSERT INTO event_snapshots (event_id, player_tag, player_name, icon_url, value_start)
            SELECT %s, tag, name, icon_url, {col}
            FROM players
        """, (event_id,))


def freeze_values(cursor, event_id: int, metric: str, brawler_name: Optional[str]):
    """Freeze final values into value_end for all participants of an event."""
    if metric == "brawler_trophies":
        bn = (brawler_name or "").strip().upper()
        cursor.execute("""
            UPDATE event_snapshots es
            SET value_end = COALESCE(pb.trophies, 0)
            FROM players p
            LEFT JOIN player_brawlers pb
              ON pb.player_tag = p.tag AND UPPER(pb.brawler_name) = %s
            WHERE es.player_tag = p.tag AND es.event_id = %s
        """, (bn, event_id))
    else:
        col = METRIC_PLAYER_COL[metric]
        cursor.execute(f"""
            UPDATE event_snapshots es
            SET value_end = p.{col}
            FROM players p
            WHERE es.player_tag = p.tag AND es.event_id = %s
        """, (event_id,))


def compute_results(cursor, event_id: int, metric: str, brawler_name: Optional[str]):
    """Return ranked leaderboard for an event, using live values when still active."""
    if metric == "brawler_trophies":
        bn = (brawler_name or "").strip().upper()
        cursor.execute("""
            SELECT
                es.player_tag,
                es.player_name,
                es.icon_url,
                es.value_start,
                COALESCE(es.value_end, COALESCE(pb.trophies, 0)) AS value_now,
                COALESCE(es.value_end, COALESCE(pb.trophies, 0)) - es.value_start AS delta
            FROM event_snapshots es
            LEFT JOIN players p ON es.player_tag = p.tag
            LEFT JOIN player_brawlers pb
              ON pb.player_tag = es.player_tag AND UPPER(pb.brawler_name) = %s
            WHERE es.event_id = %s
            ORDER BY delta DESC
        """, (bn, event_id))
    else:
        col = METRIC_PLAYER_COL.get(metric, "highest_trophies")
        cursor.execute(f"""
            SELECT
                es.player_tag,
                es.player_name,
                es.icon_url,
                es.value_start,
                COALESCE(es.value_end, p.{col}) AS value_now,
                COALESCE(es.value_end, p.{col}) - es.value_start AS delta
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
            "value_start": vs,
            "value_now": vn,
            "delta": delta
        }
        for i, (tag, name, ico, vs, vn, delta) in enumerate(rows)
    ]


# ── EVENTS: PUBLIC ENDPOINTS ─────────────────────────────────────────────────

@app.get("/events")
@limiter.limit("10/minute")
def getEvents():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_events_tables(cursor)
        auto_close_expired(cursor)
        conn.commit()

        cursor.execute("""
            SELECT id, title, description, reward, metric, brawler_name,
                   started_at, ends_at, closed_at, is_active
            FROM events
            ORDER BY started_at DESC
        """)
        rows = cursor.fetchall()

        result = []
        for (eid, title, desc, reward, metric, brawler_name,
             started_at, ends_at, closed_at, is_active) in rows:
            participants = compute_results(cursor, eid, metric, brawler_name)
            result.append({
                "id": eid,
                "title": title,
                "description": desc,
                "reward": reward,
                "metric": metric,
                "brawler_name": brawler_name,
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
    brawler_name: Optional[str] = None   # required when metric == brawler_trophies
    duration_hours: float


@app.post("/events")
def createEvent(body: CreateEventBody, x_admin_key: Optional[str] = Header(None)):
    check_admin(x_admin_key)

    if body.metric not in VALID_METRICS:
        raise HTTPException(
            status_code=400,
            detail=f"metric invalida. Valores válidos: {', '.join(VALID_METRICS)}"
        )
    if body.metric == "brawler_trophies" and not body.brawler_name:
        raise HTTPException(status_code=400, detail="brawler_name es requerido para la metric brawler_trophies")
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
            INSERT INTO events (title, description, reward, metric, brawler_name, ends_at)
            VALUES (%s, %s, %s, %s, %s, NOW() + INTERVAL '1 hour' * %s)
            RETURNING id
        """, (body.title, body.description, body.reward, body.metric,
              body.brawler_name, body.duration_hours))
        event_id = cursor.fetchone()[0]

        snapshot_values(cursor, event_id, body.metric, body.brawler_name)

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

        cursor.execute("SELECT is_active, metric, brawler_name FROM events WHERE id = %s", (event_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Evento no encontrado")
        if not row[0]:
            raise HTTPException(status_code=400, detail="El evento ya está cerrado")

        metric, brawler_name = row[1], row[2]
        freeze_values(cursor, event_id, metric, brawler_name)

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


# ── STATUS ────────────────────────────────────────────────────────────────────

@app.get("/status")
@limiter.limit("10/minute")
def getStatus():
    """Devuelve el timestamp del último dato guardado en la DB."""
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT MAX(timestamp) FROM player_stats_history
        """)
        row = cursor.fetchone()
        last_updated = row[0].isoformat() if row and row[0] else None
        return {"last_updated": last_updated}
    finally:
        cursor.close()
        conn.close()
