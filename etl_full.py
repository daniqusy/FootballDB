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
    # Transfers additional indexes to support Mongo list search
    mdb.transfers.create_index([("player_name", 1)])
    mdb.transfers.create_index([("from.name", 1)])
    mdb.transfers.create_index([("to.name", 1)])
    # Players collection indexes (added for profile + market compare queries)
    mdb.players.create_index([("player_id", 1)], unique=True)
    mdb.players.create_index([("market_value_eur", -1)])
    mdb.players.create_index([("position", 1)])
    mdb.players.create_index([("current_club_id", 1)])
    mdb.players.create_index([("country_of_citizenship", 1)])
    mdb.players.create_index([("agent_name", 1)])
    mdb.players.create_index([("city_of_birth", 1)])
    # Clubs collection indexes (for Mongo club endpoints)
    mdb.clubs.create_index([("club_id", 1)], unique=True)
    mdb.clubs.create_index([("name", 1)])
    mdb.clubs.create_index([("total_market_value_eur", -1)])
    # Appearances collection indexes (for Mongo appearances list)
    if "appearances" in mdb.list_collection_names():
        mdb.appearances.create_index([("game_id", -1), ("date", -1)])
        mdb.appearances.create_index([("player_id", 1)])
        mdb.appearances.create_index([("player_name", 1)])
        mdb.appearances.create_index([("club_name", 1)])

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

# Safe date formatting helper (handles already-string dates, date/datetime objects, or None)
def fmt_date(value):
  if not value:
    return None
  # If already a string (e.g., MySQL driver returned VARCHAR), normalize length to YYYY-MM-DD when possible
  if isinstance(value, (str, bytes)):
    try:
      if len(value) >= 10 and value[4] == '-' and value[7] == '-':
        return value[:10]
    except Exception:
      pass
    return value
  # Attempt strftime on date/datetime objects
  try:
    return value.strftime('%Y-%m-%d')
  except Exception:
    return str(value)

# --- ETL: Games ---
def upsert_games(batch=1000):
    print("ETL games...")
    t0 = time.time()
    with sql.cursor() as cur:
        games = fetchall(cur, r"""
          SELECT g.game_id,
                 DATE_FORMAT(g.date,'%%Y-%%m-%%d') AS date,
                 g.competition_id, c.name AS competition_name,
                 g.season, g.round,
                 g.home_club_id, hc.name AS home_name, g.home_club_goals, g.home_club_formation,
                 g.home_club_position, g.home_club_manager_name,
                 g.away_club_id, ac.name AS away_name, g.away_club_goals, g.away_club_formation,
                 g.away_club_position, g.away_club_manager_name,
                 g.stadium, g.attendance, g.referee,
                 TIME_FORMAT(COALESCE(g.match_time,'00:00:00'),'%%H:%%i') AS match_time
          FROM game g
          JOIN competition c ON c.competition_id=g.competition_id
          JOIN club hc ON hc.club_id=g.home_club_id
          JOIN club ac ON ac.club_id=g.away_club_id
        """)
    ops, n = [], 0
    with sql.cursor() as cur:
        for g in games:
            evs = fetchall(cur, r"""
              SELECT ge.game_event_id,
                     ge.minute,
                     ge.type,
                     ge.club_id,
                     ge.player_id,
                     p1.name AS player_name,
                     ge.player_in_id AS sub_in_id,
                     p2.name AS player_in_name,
                     ge.player_assist_id AS assist_id,
                     p3.name AS assist_name,
                     ge.description AS event_desc
              FROM game_events ge
              LEFT JOIN player p1 ON p1.player_id = ge.player_id
              LEFT JOIN player p2 ON p2.player_id = ge.player_in_id
              LEFT JOIN player p3 ON p3.player_id = ge.player_assist_id
              WHERE ge.game_id=%s
              ORDER BY ge.minute, ge.game_event_id
            """, (g["game_id"],))
            doc = sanitize({
              "_id": g["game_id"],
              "date": g["date"],
              "competition_id": g["competition_id"],
              "competition_name": g.get("competition_name"),
              "season": g["season"],
              "round": g["round"],
              "home": { "club_id": g["home_club_id"], "name": g["home_name"],
                        "goals": g["home_club_goals"],
                        "formation": (g["home_club_formation"] or "").replace("/","-").strip(),
                        "position": g.get("home_club_position"),
                        "manager_name": g.get("home_club_manager_name") },
              "away": { "club_id": g["away_club_id"], "name": g["away_name"],
                        "goals": g["away_club_goals"],
                        "formation": (g["away_club_formation"] or "").replace("/","-").strip(),
                        "position": g.get("away_club_position"),
                        "manager_name": g.get("away_club_manager_name") },
              "stadium": g["stadium"], "attendance": g["attendance"], "referee": g["referee"],
              "match_time": g.get("match_time"),
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
                     a.player_club_id AS player_club_id,
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
            t.market_value_in_eur,
            p.name AS player_name
          FROM transfer t
          LEFT JOIN club fc ON fc.club_id=t.from_club_id
          LEFT JOIN club tc ON tc.club_id=t.to_club_id
          JOIN player p ON p.player_id = t.player_id
        """)
    ops, n = [], 0
    for r in rows:
        doc = sanitize({
          "_id": r["transfer_id"],
          "player_id": r["player_id"],
          "player_name": r.get("player_name"),
          "transfer_date": r["transfer_date"],
          "transfer_season": r["transfer_season"],
          "from": { "club_id": r["from_club_id"], "name": r["from_name"] },
          "to":   { "club_id": r["to_club_id"],   "name": r["to_name"] },
          "transfer_fee": r["transfer_fee"],
          "market_value_in_eur": r["market_value_in_eur"],
          # Duplicated flattened fields for fast Mongo list projection (avoid deep lookups)
          "from_club_name": r["from_name"],
          "to_club_name": r["to_name"],
          "updated_at": int(time.time())
        })
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
        if len(ops) >= batch:
            mdb.transfers.bulk_write(ops); n += len(ops); ops = []
    if ops: mdb.transfers.bulk_write(ops); n += len(ops)
    print(f"transfers upserts: {n} in {time.time()-t0:.1f}s")

# --- ETL: Players (for Mongo player profile & market compare) ---
def upsert_players(batch=2000):
    print("ETL players...")
    t0 = time.time()
    with sql.cursor() as cur:
        rows = fetchall(cur, r"""
          SELECT p.player_id, p.name, p.position, p.sub_position,
                 p.current_club_id, c.name AS current_club_name,
                 p.market_value_eur, p.highest_market_value_eur,
                 pb.image_url, pb.height_in_cm, pb.dob, pb.country_of_citizenship,
                 pb.foot, pb.city_of_birth, pb.agent_name, pb.contract_expiration_date
          FROM player p
          LEFT JOIN club c ON c.club_id = p.current_club_id
          LEFT JOIN player_bio pb ON pb.player_id = p.player_id
        """)
    ops, n = [], 0
    for r in rows:
        doc = sanitize({
          "_id": r["player_id"],
          "player_id": r["player_id"],
          "name": r["name"],
          "position": r["position"],
          "sub_position": r.get("sub_position"),
          "current_club_id": r.get("current_club_id"),
          "current_club_name": r.get("current_club_name"),
          "market_value_eur": r.get("market_value_eur"),
          "highest_market_value_eur": r.get("highest_market_value_eur"),
          "image_url": r.get("image_url"),
          "height_in_cm": r.get("height_in_cm"),
          "dob": fmt_date(r.get("dob")),
          "country_of_citizenship": r.get("country_of_citizenship"),
          "foot": r.get("foot"),
          "city_of_birth": r.get("city_of_birth"),
          "agent_name": r.get("agent_name"),
          "contract_expiration_date": fmt_date(r.get("contract_expiration_date")),
          "updated_at": int(time.time())
        })
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
        if len(ops) >= batch:
            mdb.players.bulk_write(ops); n += len(ops); ops = []
    if ops:
        mdb.players.bulk_write(ops); n += len(ops)
    print(f"players upserts: {n} in {time.time()-t0:.1f}s")

# --- ETL: Clubs (for Mongo club profile & listings) ---
def upsert_clubs(batch=2000):
    print("ETL clubs...")
    t0 = time.time()
    with sql.cursor() as cur:
        rows = fetchall(cur, r"""
          SELECT c.club_id, c.name, c.domestic_competition_id, c.squad_size, c.average_age,
                 c.stadium_name, c.stadium_seats,
                 COALESCE(SUM(p.market_value_eur),0) AS total_market_value_eur,
                 COUNT(p.player_id) AS player_count
          FROM club c
          LEFT JOIN player p ON p.current_club_id = c.club_id AND p.market_value_eur IS NOT NULL
          GROUP BY c.club_id, c.name, c.domestic_competition_id, c.squad_size, c.average_age, c.stadium_name, c.stadium_seats
        """)
    ops, n = [], 0
    for r in rows:
        doc = sanitize({
          "_id": r["club_id"],
          "club_id": r["club_id"],
          "name": r.get("name"),
          "domestic_competition_id": r.get("domestic_competition_id"),
          "squad_size": r.get("squad_size"),
          "average_age": r.get("average_age"),
          "stadium_name": r.get("stadium_name"),
          "stadium_seats": r.get("stadium_seats"),
          "total_market_value_eur": r.get("total_market_value_eur"),
          "player_count": r.get("player_count"),
          "updated_at": int(time.time())
        })
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
        if len(ops) >= batch:
            mdb.clubs.bulk_write(ops); n += len(ops); ops = []
    if ops:
        mdb.clubs.bulk_write(ops); n += len(ops)
    print(f"clubs upserts: {n} in {time.time()-t0:.1f}s")

# --- ETL: Appearances (denormalized list for Mongo list endpoint) ---
def upsert_appearances(batch=5000):
    print("ETL appearances...")
    t0 = time.time()
    with sql.cursor() as cur:
        rows = fetchall(cur, r"""
          SELECT a.appearance_id, a.game_id, a.player_id, a.player_club_id,
                 a.player_current_club_id, DATE_FORMAT(a.date,'%%Y-%%m-%%d') AS date,
                 a.yellow_cards, a.red_cards, a.goals, a.assists, a.minutes_played,
                 p.name AS player_name, c.name AS club_name
          FROM appearance a
          LEFT JOIN player p ON p.player_id = a.player_id
          LEFT JOIN club c ON c.club_id = a.player_club_id
        """)
    ops, n = [], 0
    for r in rows:
        doc = sanitize({
          "_id": r["appearance_id"],
          "appearance_id": r["appearance_id"],
          "game_id": r["game_id"],
          "player_id": r["player_id"],
          "player_club_id": r.get("player_club_id"),
          "player_current_club_id": r.get("player_current_club_id"),
          "date": r.get("date"),
          "yellow_cards": r.get("yellow_cards"),
          "red_cards": r.get("red_cards"),
          "goals": r.get("goals"),
          "assists": r.get("assists"),
          "minutes_played": r.get("minutes_played"),
          "player_name": r.get("player_name"),
          "club_name": r.get("club_name"),
          "updated_at": int(time.time())
        })
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
        if len(ops) >= batch:
            mdb.appearances.bulk_write(ops); n += len(ops); ops = []
    if ops:
        mdb.appearances.bulk_write(ops); n += len(ops)
    print(f"appearances upserts: {n} in {time.time()-t0:.1f}s")


def main():
    ensure_indexes()
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", action="store_true")
    ap.add_argument("--playerseasons", action="store_true")
    ap.add_argument("--transfers", action="store_true")
    ap.add_argument("--players", action="store_true")
    ap.add_argument("--clubs", action="store_true")
    ap.add_argument("--appearances", action="store_true")
    args = ap.parse_args()

    # If no specific flag, run all
    if not (args.games or args.playerseasons or args.transfers or args.players or args.clubs or args.appearances):
        upsert_games()
        upsert_player_seasons()
        upsert_transfers()
        upsert_players()
        upsert_clubs()
        upsert_appearances()
        return

    if args.games: upsert_games()
    if args.playerseasons: upsert_player_seasons()
    if args.transfers: upsert_transfers()
    if args.players: upsert_players()
    if args.clubs: upsert_clubs()
    if args.appearances: upsert_appearances()

if __name__ == "__main__":
  main()
