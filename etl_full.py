import os, sys, time, argparse
import pymysql
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from decimal import Decimal

load_dotenv()

# --- Connections ---
sql = pymysql.connect(
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "football_db"),
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True,
)
mongo = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
mdb = mongo.get_database(os.getenv("MONGO_DB", "football_nonrelationaldb"))

def ensure_indexes():
    mdb.games.create_index([("competition_id", 1), ("season", 1), ("date", -1)])
    mdb.games.create_index([("home.club_id", 1)])
    mdb.games.create_index([("away.club_id", 1)])
    mdb.player_seasons.create_index([("player_id", 1), ("competition_id", 1), ("season", -1)])
    mdb.transfers.create_index([("player_id", 1), ("transfer_date", -1)])
    mdb.transfers.create_index([("to.club_id", 1), ("transfer_season", -1)])

def fetchall(cur, q, args=None):
    cur.execute(q, args or ())
    return cur.fetchall()

# --- Helpers to sanitize Decimal values for Mongo ---
def _to_plain(value):
  if isinstance(value, Decimal):
    # If integer-valued Decimal, cast to int; else cast to float
    try:
      if value == value.to_integral():
        return int(value)
    except Exception:
      pass
    return float(value)
  return value

def sanitize(obj):
  if isinstance(obj, dict):
    return {k: sanitize(v) for k, v in obj.items()}
  if isinstance(obj, list):
    return [sanitize(v) for v in obj]
  return _to_plain(obj)

# --- ETL: Games ---
def upsert_games(batch=1000):
    print("ETL games...")
    t0 = time.time()
    with sql.cursor() as cur:
        games = fetchall(cur, r"""
          SELECT g.game_id, DATE_FORMAT(g.date,'%%Y-%%m-%%d') AS date,
                 g.competition_id, g.season, g.round,
                 g.home_club_id, hc.name AS home_name, g.home_club_goals, g.home_club_formation,
                 g.away_club_id, ac.name AS away_name, g.away_club_goals, g.away_club_formation,
                 g.stadium, g.attendance, g.referee
          FROM game g
          JOIN club hc ON hc.club_id=g.home_club_id
          JOIN club ac ON ac.club_id=g.away_club_id
        """)
    ops, n = [], 0
    with sql.cursor() as cur:
        for g in games:
            evs = fetchall(cur, """
              SELECT minute, type, club_id, player_id,
                     player_in_id AS sub_in_id,
                     player_assist_id AS assist_id,
                     description AS event_desc
              FROM game_events
              WHERE game_id=%s
              ORDER BY minute
            """, (g["game_id"],))
            doc = sanitize({
              "_id": g["game_id"],
              "date": g["date"],
              "competition_id": g["competition_id"],
              "season": g["season"],
              "round": g["round"],
              "home": { "club_id": g["home_club_id"], "name": g["home_name"],
                        "goals": g["home_club_goals"],
                        "formation": (g["home_club_formation"] or "").replace("/","-").strip() },
              "away": { "club_id": g["away_club_id"], "name": g["away_name"],
                        "goals": g["away_club_goals"],
                        "formation": (g["away_club_formation"] or "").replace("/","-").strip() },
              "stadium": g["stadium"], "attendance": g["attendance"], "referee": g["referee"],
              "events": sanitize(evs),
              "updated_at": int(time.time())
            })
            ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
            if len(ops) >= batch:
                mdb.games.bulk_write(ops); n += len(ops); ops = []
        if ops: mdb.games.bulk_write(ops); n += len(ops)
    print(f"games upserts: {n} in {time.time()-t0:.1f}s")

# --- ETL: Player seasons ---
def upsert_player_seasons(batch=1000):
    print("ETL player_seasons...")
    t0 = time.time()
    with sql.cursor() as cur:
        rows = fetchall(cur, """
          SELECT a.player_id, g.competition_id, g.season,
                 COUNT(*) apps,
                 SUM(a.minutes_played) minutes,
                 SUM(a.goals) goals,
                 SUM(a.assists) assists,
                 SUM(a.yellow_cards) yc,
                 SUM(a.red_cards) rc
          FROM appearance a
          JOIN game g ON g.game_id=a.game_id
          GROUP BY a.player_id, g.competition_id, g.season
        """)
    ops, n = [], 0
    with sql.cursor() as cur:
        for r in rows:
            pid, comp, season = r["player_id"], r["competition_id"], r["season"]
            ga_per90 = ((r["goals"] or 0) + (r["assists"] or 0)) * 90 / max(r["minutes"] or 0, 1)
            latest = fetchall(cur, r"""
              SELECT g.game_id,
                     DATE_FORMAT(g.date,'%%Y-%%m-%%d') AS date,
                     a.minutes_played AS min,
                     a.goals AS g,
                     a.assists AS a,
                     g.home_club_id,
                     hc.name AS home_name,
                     g.away_club_id,
                     ac.name AS away_name,
                     g.home_club_goals,
                     g.away_club_goals
              FROM appearance a
              JOIN game g   ON g.game_id = a.game_id
              JOIN club hc  ON hc.club_id = g.home_club_id
              JOIN club ac  ON ac.club_id = g.away_club_id
              WHERE a.player_id=%s AND g.competition_id=%s AND g.season=%s
              ORDER BY g.date DESC
              LIMIT 10
            """, (pid, comp, season))
            doc = sanitize({
              "_id": f"{pid}_{comp}_{season}",
              "player_id": pid, "competition_id": comp, "season": season,
              "totals": { "apps": r["apps"], "minutes": r["minutes"], "goals": r["goals"],
                          "assists": r["assists"], "yc": r["yc"], "rc": r["rc"],
                          "ga_per90": round(ga_per90, 3) },
              "latest_matches": sanitize(latest),
              "updated_at": int(time.time())
            })
            ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
            if len(ops) >= batch:
                mdb.player_seasons.bulk_write(ops); n += len(ops); ops = []
        if ops: mdb.player_seasons.bulk_write(ops); n += len(ops)
    print(f"player_seasons upserts: {n} in {time.time()-t0:.1f}s")

# --- ETL: Transfers ---
def upsert_transfers(batch=2000):
    print("ETL transfers...")
    t0 = time.time()
    with sql.cursor() as cur:
        rows = fetchall(cur, r"""
          SELECT
            t.transfer_id,
            t.player_id,
            DATE_FORMAT(t.transfer_date,'%%Y-%%m-%%d') AS transfer_date,
            t.transfer_season,
            t.from_club_id, COALESCE(fc.name,'') AS from_name,
            t.to_club_id,   COALESCE(tc.name,'') AS to_name,
            t.transfer_fee,
            t.market_value_in_eur
          FROM transfer t
          LEFT JOIN club fc ON fc.club_id=t.from_club_id
          LEFT JOIN club tc ON tc.club_id=t.to_club_id
        """)
    ops, n = [], 0
    for r in rows:
        doc = sanitize({
          "_id": r["transfer_id"],
          "player_id": r["player_id"],
          "transfer_date": r["transfer_date"],
          "transfer_season": r["transfer_season"],
          "from": { "club_id": r["from_club_id"], "name": r["from_name"] },
          "to":   { "club_id": r["to_club_id"],   "name": r["to_name"] },
          "transfer_fee": r["transfer_fee"],
          "market_value_in_eur": r["market_value_in_eur"],
          "updated_at": int(time.time())
        })
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
        if len(ops) >= batch:
            mdb.transfers.bulk_write(ops); n += len(ops); ops = []
    if ops: mdb.transfers.bulk_write(ops); n += len(ops)
    print(f"transfers upserts: {n} in {time.time()-t0:.1f}s")

def main():
    ensure_indexes()
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", action="store_true")
    ap.add_argument("--playerseasons", action="store_true")
    ap.add_argument("--transfers", action="store_true")
    args = ap.parse_args()

    # If no specific flag, run all
    if not (args.games or args.playerseasons or args.transfers):
        upsert_games()
        upsert_player_seasons()
        upsert_transfers()
        return

    if args.games: upsert_games()
    if args.playerseasons: upsert_player_seasons()
    if args.transfers: upsert_transfers()

if __name__ == "__main__":
    main()
