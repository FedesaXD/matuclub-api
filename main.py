import os
import re
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import Optional
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded


load_dotenv()

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)  # deshabilitar docs públicas

# ── CORS: debe agregarse ÚLTIMO para ejecutarse PRIMERO (orden inverso en Starlette) ──
# Maneja los preflight OPTIONS antes que cualquier otro middleware.
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
    allow_headers=["Content-Type", "X-Admin-Key", "Authorization"],
    allow_credentials=False,
    max_age=600,  # cachear preflight 10 minutos
)

# ── Security headers: se agrega antes para ejecutarse DESPUÉS de CORS ────────
# Así no interfiere con las respuestas preflight OPTIONS.
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        # No agregar headers en preflight OPTIONS — CORS los maneja
        if request.method != "OPTIONS":
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response

app.add_middleware(SecurityHeadersMiddleware)

# ── Rate limiter: leer IP real detrás del proxy de Render ────────────────────
def get_real_ip(request: Request) -> str:
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

limiter = Limiter(key_func=get_real_ip)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Devuelve errores HTTP conocidos con su código y mensaje — sin stack trace."""
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """
    Captura cualquier error inesperado (DB caída, bug, etc.) y devuelve
    un mensaje genérico. Nunca expone detalles internos al cliente.
    """
    import logging
    logging.error(f"Unhandled error on {request.method} {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Error interno del servidor. Intentá de nuevo en unos segundos."}
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
@limiter.limit("30/minute")
def ver_datos(request: Request, player_tag: str):
    player_tag = player_tag.strip().upper()
    if not player_tag.startswith("#"):
        player_tag = "#" + player_tag
    # Tags de Brawl Stars: # seguido de 3-15 caracteres alfanuméricos (0-9, A-Z, sin I,O,U)
    if not re.match(r"^#[0-9A-Z]{3,15}$", player_tag):
        raise HTTPException(status_code=400, detail="Tag de jugador inválido")

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

        # Muestreo uniforme de hasta 60 puntos sobre toda la historia del jugador.
        # Paso 1: numerar filas y asignar bucket con NTILE en una CTE.
        # Paso 2: en otra CTE tomar ROW_NUMBER dentro de cada bucket.
        # Esto evita anidar window functions, que PostgreSQL no permite.
        # Siempre se incluye la última fila (rn = total) para llegar al presente.
        cursor.execute("""
            WITH numbered AS (
                SELECT timestamp, trophies, wins3v3, winsSolo, total_prestige,
                       ROW_NUMBER() OVER (ORDER BY timestamp ASC) AS rn,
                       COUNT(*)     OVER ()                        AS total,
                       NTILE(60)    OVER (ORDER BY timestamp ASC) AS bucket
                FROM player_stats_history
                WHERE player_tag = %s
            ),
            bucketed AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY timestamp ASC) AS rn_in_bucket
                FROM numbered
            )
            SELECT timestamp, trophies, wins3v3, winsSolo, total_prestige
            FROM bucketed
            WHERE rn_in_bucket = 1  -- primer punto de cada bucket
               OR rn = total         -- siempre incluir el último snapshot
            ORDER BY timestamp ASC
            LIMIT 60
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
def topBrawler(request: Request, brawler_name: str, club: str = None):
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
def clubMembers(request: Request, club_num: str):
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

def ensure_events_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            reward TEXT NOT NULL,
            metric TEXT NOT NULL,
            brawler_name TEXT,
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
            value_start INTEGER NOT NULL,
            value_end INTEGER,
            UNIQUE(event_id, player_tag)
        )
    """)
    cursor.execute("""
        DO $$
        DECLARE c TEXT;
        BEGIN
            SELECT conname INTO c FROM pg_constraint
            WHERE conrelid = 'events'::regclass AND contype = 'c' AND conname LIKE '%metric%';
            IF c IS NOT NULL THEN
                EXECUTE 'ALTER TABLE events DROP CONSTRAINT ' || quote_ident(c);
            END IF;
        END $$
    """)
    cursor.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS brawler_name TEXT")
    cursor.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='event_snapshots' AND column_name='trophies_start'
            ) THEN
                ALTER TABLE event_snapshots RENAME COLUMN trophies_start TO value_start;
                ALTER TABLE event_snapshots RENAME COLUMN trophies_end   TO value_end;
            END IF;
        END $$
    """)


def check_admin(x_admin_key: Optional[str]):
    admin_key = os.getenv("ADMIN_KEY", "")
    if not admin_key or x_admin_key != admin_key:
        raise HTTPException(status_code=403, detail="No autorizado")


def auto_close_expired(cursor):
    """
    Cierra eventos expirados y freezea sus valores finales.
    Sin el freeze, compute_results usa valores live del jugador
    y el resultado del torneo cambia después de terminar.
    """
    # Primero obtener los eventos que hay que cerrar
    cursor.execute("""
        SELECT id, metric, brawler_name FROM events
        WHERE is_active = TRUE AND ends_at <= NOW()
    """)
    to_close = cursor.fetchall()

    for (event_id, metric, brawler_name) in to_close:
        # Freeze solo los snapshots que aún no tienen value_end
        freeze_values(cursor, event_id, metric, brawler_name)

    if to_close:
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
    """
    Freeze final values into value_end for all participants of an event.
    Solo actualiza filas con value_end IS NULL para no sobreescribir
    snapshots ya freezeados correctamente por un cierre manual previo.
    """
    if metric == "brawler_trophies":
        bn = (brawler_name or "").strip().upper()
        cursor.execute("""
            UPDATE event_snapshots es
            SET value_end = COALESCE(pb.trophies, 0)
            FROM players p
            LEFT JOIN player_brawlers pb
              ON pb.player_tag = p.tag AND UPPER(pb.brawler_name) = %s
            WHERE es.player_tag = p.tag AND es.event_id = %s
              AND es.value_end IS NULL
        """, (bn, event_id))
    else:
        col = METRIC_PLAYER_COL[metric]
        cursor.execute(f"""
            UPDATE event_snapshots es
            SET value_end = p.{col}
            FROM players p
            WHERE es.player_tag = p.tag AND es.event_id = %s
              AND es.value_end IS NULL
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
def getEvents(request: Request):
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
@limiter.limit("5/minute")
def createEvent(request: Request, body: CreateEventBody, x_admin_key: Optional[str] = Header(None)):
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
        raise HTTPException(status_code=500, detail="Error al crear el evento")
    finally:
        cursor.close()
        conn.close()


@app.patch("/events/{event_id}/close")
@limiter.limit("5/minute")
def closeEvent(request: Request, event_id: int, x_admin_key: Optional[str] = Header(None)):
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
        raise HTTPException(status_code=500, detail="Error al cerrar el evento")
    finally:
        cursor.close()
        conn.close()


# ── STATUS ────────────────────────────────────────────────────────────────────

@app.get("/status")
@limiter.limit("10/minute")
def getStatus(request: Request):
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


# ── JUGADOR DEL DÍA ───────────────────────────────────────────────────────────
# Sistema de puntos:
#   1 trofeo ganado      = 1 punto
#   1 victoria (3v3/solo) = 4 puntos
#   1 prestige subido    = 80 puntos
#
# Se compara el primer snapshot del día vs el último snapshot del día anterior
# (o el último snapshot disponible antes del día) para calcular el delta.
# Se guarda el resultado en la tabla player_of_day para historial de 7 días.

def ensure_player_of_day_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_of_day (
            day         DATE PRIMARY KEY,
            player_tag  TEXT NOT NULL,
            player_name TEXT NOT NULL,
            icon_url    TEXT,
            club_name   TEXT,
            points      INTEGER NOT NULL,
            delta_trophies  INTEGER NOT NULL DEFAULT 0,
            delta_wins3v3   INTEGER NOT NULL DEFAULT 0,
            delta_winsSolo  INTEGER NOT NULL DEFAULT 0,
            delta_prestige  INTEGER NOT NULL DEFAULT 0,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)


@app.get("/player-of-day/winners")
@limiter.limit("20/minute")
def getPlayerOfDayWinners(request: Request):
    """
    Top 20 jugadores que más veces fueron jugador del día,
    ordenados por cantidad de victorias descendente.
    """
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_player_of_day_table(cursor)
        cursor.execute("""
            SELECT
                p.tag,
                pod.player_name,
                p.icon_url,
                p.club_name,
                COUNT(*) AS wins
            FROM player_of_day pod
            JOIN players p ON p.tag = pod.player_tag
            GROUP BY p.tag, pod.player_name, p.icon_url, p.club_name
            ORDER BY wins DESC
            LIMIT 20
        """)
        rows = cursor.fetchall()
        return [
            {
                "rank":        i + 1,
                "player_tag":  tag,
                "player_name": name,
                "icon_url":    icon_url,
                "club_name":   club_name,
                "wins":        wins,
            }
            for i, (tag, name, icon_url, club_name, wins) in enumerate(rows)
        ]
    finally:
        cursor.close()
        conn.close()


@app.get("/player-of-day")
@limiter.limit("20/minute")
def getPlayerOfDay(request: Request):
    """
    Devuelve:
      - today_ranking: top 20 jugadores del día actual con sus deltas (calculado en vivo)
      - history: ganadores de los últimos 7 días (sin incluir hoy)
      - last_updated: timestamp del último cómputo del collector
    """
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_player_of_day_table(cursor)
        conn.commit()

        from datetime import date, timedelta
        today = (datetime.now(timezone.utc) - timedelta(hours=3)).date()

        # Fallback: calcular ganador de hoy si el collector aún no corrió
        cursor.execute("SELECT 1 FROM player_of_day WHERE day = %s", (today,))
        if not cursor.fetchone():
            _compute_and_save_player_of_day(cursor, today)
            conn.commit()

        # ── Ranking de hoy en vivo (top 20 con deltas) ──────────────────
        day_start = datetime(today.year, today.month, today.day,
                             3, 0, 0, tzinfo=timezone.utc)
        day_end   = day_start + timedelta(hours=24)

        cursor.execute("""
            WITH ranked AS (
                SELECT player_tag, trophies, wins3v3, winssolo, total_prestige, timestamp,
                       ROW_NUMBER() OVER (PARTITION BY player_tag ORDER BY timestamp ASC)  AS rn_asc,
                       ROW_NUMBER() OVER (PARTITION BY player_tag ORDER BY timestamp DESC) AS rn_desc
                FROM player_stats_history
                WHERE timestamp >= %s AND timestamp < %s
            ),
            day_first AS (SELECT player_tag, trophies, wins3v3, winssolo, total_prestige FROM ranked WHERE rn_asc  = 1),
            day_last  AS (SELECT player_tag, trophies, wins3v3, winssolo, total_prestige FROM ranked WHERE rn_desc = 1),
            prev_last AS (
                SELECT DISTINCT ON (player_tag) player_tag, trophies, wins3v3, winssolo, total_prestige
                FROM player_stats_history WHERE timestamp < %s
                ORDER BY player_tag, timestamp DESC
            )
            SELECT
                dl.player_tag,
                p.name, p.icon_url, p.club_name,
                GREATEST(0, COALESCE(dl.trophies,       pv.trophies)       - COALESCE(df.trophies,       pv.trophies,       0)) AS dt,
                GREATEST(0, COALESCE(dl.wins3v3,        pv.wins3v3)        - COALESCE(df.wins3v3,        pv.wins3v3,        0)) AS dw3,
                GREATEST(0, COALESCE(dl.winssolo,       pv.winssolo)       - COALESCE(df.winssolo,       pv.winssolo,       0)) AS dws,
                GREATEST(0, COALESCE(dl.total_prestige, pv.total_prestige) - COALESCE(df.total_prestige, pv.total_prestige, 0)) AS dp
            FROM day_last dl
            LEFT JOIN day_first df USING (player_tag)
            LEFT JOIN prev_last  pv USING (player_tag)
            JOIN players p ON p.tag = dl.player_tag
            ORDER BY (
                GREATEST(0, COALESCE(dl.trophies,       pv.trophies)       - COALESCE(df.trophies,       pv.trophies,       0)) * 1 +
                GREATEST(0, COALESCE(dl.wins3v3,        pv.wins3v3)        - COALESCE(df.wins3v3,        pv.wins3v3,        0)) * 4 +
                GREATEST(0, COALESCE(dl.winssolo,       pv.winssolo)       - COALESCE(df.winssolo,       pv.winssolo,       0)) * 4 +
                GREATEST(0, COALESCE(dl.total_prestige, pv.total_prestige) - COALESCE(df.total_prestige, pv.total_prestige, 0)) * 80
            ) DESC
            LIMIT 20
        """, (day_start, day_end, day_start))

        today_rows = cursor.fetchall()
        today_ranking = []
        for i, (tag, name, icon_url, club_name, dt, dw3, dws, dp) in enumerate(today_rows):
            points = dt * 1 + (dw3 + dws) * 4 + dp * 80
            today_ranking.append({
                "rank":           i + 1,
                "player_tag":     tag,
                "player_name":    name,
                "icon_url":       icon_url,
                "club_name":      club_name,
                "points":         points,
                "delta_trophies": dt,
                "delta_wins3v3":  dw3,
                "delta_winsSolo": dws,
                "delta_prestige": dp,
            })

        # ── Historial: ganadores de días anteriores (últimos 7, sin hoy) ─
        cursor.execute("""
            SELECT day, player_tag, player_name, icon_url, club_name,
                   points, delta_trophies, delta_wins3v3, delta_winsSolo,
                   delta_prestige, computed_at
            FROM player_of_day
            WHERE day >= %s AND day < %s
            ORDER BY day DESC
            LIMIT 7
        """, (today - timedelta(days=7), today))

        history = []
        last_updated = None
        for row in cursor.fetchall():
            (day, tag, name, icon_url, club_name,
             points, dt, dw3, dws, dp, computed_at) = row
            if last_updated is None and computed_at:
                last_updated = computed_at.isoformat()
            history.append({
                "day":            day.isoformat(),
                "player_tag":     tag,
                "player_name":    name,
                "icon_url":       icon_url,
                "club_name":      club_name,
                "points":         points,
                "delta_trophies": dt,
                "delta_wins3v3":  dw3,
                "delta_winsSolo": dws,
                "delta_prestige": dp,
            })

        # last_updated: usar computed_at del registro de hoy si existe
        cursor.execute("SELECT computed_at FROM player_of_day WHERE day = %s", (today,))
        row = cursor.fetchone()
        if row and row[0]:
            last_updated = row[0].isoformat()

        return {
            "last_updated":   last_updated,
            "today_ranking":  today_ranking,
            "history":        history,
        }

    finally:
        cursor.close()
        conn.close()


def _compute_and_save_player_of_day(cursor, day):
    """
    Calcula el jugador del día para `day` comparando snapshots
    del historial y guarda el resultado en player_of_day.
    """
    from datetime import date, timedelta

    day_start = datetime(day.year, day.month, day.day,
                         3, 0, 0, tzinfo=timezone.utc)   # 00:00 UY = 03:00 UTC
    day_end   = day_start + timedelta(hours=24)
    prev_end  = day_start   # = inicio del día actual = fin del día anterior

    # Para cada jugador: valor al INICIO del día (o último antes) y al FINAL del día
    cursor.execute("""
        WITH ranked AS (
            SELECT
                player_tag,
                trophies,
                wins3v3,
                winsSolo,
                total_prestige,
                timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY player_tag
                    ORDER BY timestamp ASC
                ) AS rn_asc,
                ROW_NUMBER() OVER (
                    PARTITION BY player_tag
                    ORDER BY timestamp DESC
                ) AS rn_desc
            FROM player_stats_history
            WHERE timestamp >= %s AND timestamp < %s
        ),
        day_first AS (
            SELECT player_tag, trophies, wins3v3, winsSolo, total_prestige
            FROM ranked WHERE rn_asc = 1
        ),
        day_last AS (
            SELECT player_tag, trophies, wins3v3, winsSolo, total_prestige
            FROM ranked WHERE rn_desc = 1
        ),
        -- Último snapshot ANTES del inicio del día (referencia base)
        prev_last AS (
            SELECT DISTINCT ON (player_tag)
                player_tag, trophies, wins3v3, winsSolo, total_prestige
            FROM player_stats_history
            WHERE timestamp < %s
            ORDER BY player_tag, timestamp DESC
        )
        SELECT
            dl.player_tag,
            -- Si hay snapshots del día usamos first→last, sino comparamos con prev
            COALESCE(dl.trophies, pf.trophies)    - COALESCE(df.trophies, pf.trophies, 0)    AS dt,
            COALESCE(dl.wins3v3, pf.wins3v3)      - COALESCE(df.wins3v3,  pf.wins3v3,  0)    AS dw3,
            COALESCE(dl.winsSolo, pf.winsSolo)    - COALESCE(df.winsSolo, pf.winsSolo, 0)    AS dws,
            COALESCE(dl.total_prestige, pf.total_prestige) - COALESCE(df.total_prestige, pf.total_prestige, 0) AS dp
        FROM day_last dl
        LEFT JOIN day_first df USING (player_tag)
        LEFT JOIN prev_last  pf USING (player_tag)
    """, (day_start, day_end, day_start))

    deltas = cursor.fetchall()
    if not deltas:
        return  # Sin datos para este día, no guardamos nada

    # Calcular puntos y elegir ganador
    best = None
    best_points = -1
    for (tag, dt, dw3, dws, dp) in deltas:
        dt  = max(0, dt  or 0)
        dw3 = max(0, dw3 or 0)
        dws = max(0, dws or 0)
        dp  = max(0, dp  or 0)
        points = dt * 1 + (dw3 + dws) * 4 + dp * 80
        if points > best_points:
            best_points = points
            best = (tag, dt, dw3, dws, dp, points)

    if not best or best_points == 0:
        return  # Nadie progresó ese día

    tag, dt, dw3, dws, dp, points = best

    # Traer info del jugador
    cursor.execute("""
        SELECT name, icon_url, club_name FROM players WHERE tag = %s
    """, (tag,))
    row = cursor.fetchone()
    if not row:
        return
    name, icon_url, club_name = row

    cursor.execute("""
        INSERT INTO player_of_day
            (day, player_tag, player_name, icon_url, club_name,
             points, delta_trophies, delta_wins3v3, delta_winsSolo, delta_prestige,
             computed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (day) DO UPDATE SET
            player_tag     = EXCLUDED.player_tag,
            player_name    = EXCLUDED.player_name,
            icon_url       = EXCLUDED.icon_url,
            club_name      = EXCLUDED.club_name,
            points         = EXCLUDED.points,
            delta_trophies = EXCLUDED.delta_trophies,
            delta_wins3v3  = EXCLUDED.delta_wins3v3,
            delta_winsSolo = EXCLUDED.delta_winsSolo,
            delta_prestige = EXCLUDED.delta_prestige,
            computed_at    = NOW()
    """, (day, tag, name, icon_url, club_name, points, dt, dw3, dws, dp))
