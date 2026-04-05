import os
import psycopg2
import psycopg2.extras
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

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
            FROM players
            ORDER BY total_prestige DESC
            LIMIT 20
        """)
        rows = cursor.fetchall()

        return [{"rank": i+1, "tag": tag, "name": n, "prestige": p}
                for i, (tag, n, p) in enumerate(rows)]
    finally:
        cursor.close()
        conn.close()


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
