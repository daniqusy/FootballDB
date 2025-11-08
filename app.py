import os, time
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
import pymysql
from pymongo import MongoClient

load_dotenv()
app = Flask(__name__)

conn_args = dict(
    host=os.getenv("DB_HOST","127.0.0.1"),
    user=os.getenv("DB_USER",""),
    password=os.getenv("DB_PASS",""),
    database=os.getenv("DB_NAME",""),
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True
)

def run_sql(sql, params=()):
    t0 = time.perf_counter()
    with pymysql.connect(**conn_args) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    ms = round((time.perf_counter() - t0) * 1000.0, 2)
    return rows, ms

# Extended SQL runner with performance diagnostics
def run_sql_ex(sql, params=()):
  """Execute SQL and collect performance diagnostics with separated timings.

  Returns (rows, execution_ms, perf) where perf contains:
    rows_examined, rows_sent, explain_type, explain, query_cache,
    execution_ms, diagnostics_ms, total_ms.
  """
  perf = {
    "rows_examined": None,
    "rows_sent": None,
    "explain_type": None,
    "explain": None,
    "query_cache": None,
    "execution_ms": None,
    "diagnostics_ms": None,
    "total_ms": None,
  }
  t_total_start = time.perf_counter()
  with pymysql.connect(**conn_args) as conn:
    with conn.cursor() as cur:
      # Capture before-status
      try:
        cur.execute("SHOW SESSION STATUS LIKE 'Rows_examined'")
        rex_before = int((cur.fetchone() or {}).get('Value', 0))
        cur.execute("SHOW SESSION STATUS LIKE 'Rows_sent'")
        rse_before = int((cur.fetchone() or {}).get('Value', 0))
      except Exception:
        rex_before = rse_before = None

      # Main query timing
      t_exec_start = time.perf_counter()
      cur.execute(sql, params)
      rows = cur.fetchall()
      perf["execution_ms"] = round((time.perf_counter() - t_exec_start) * 1000.0, 2)

      # After-status deltas
      try:
        if rex_before is not None:
          cur.execute("SHOW SESSION STATUS LIKE 'Rows_examined'")
          rex_after = int((cur.fetchone() or {}).get('Value', 0))
          perf["rows_examined"] = max(rex_after - rex_before, 0)
        if rse_before is not None:
          cur.execute("SHOW SESSION STATUS LIKE 'Rows_sent'")
          rse_after = int((cur.fetchone() or {}).get('Value', 0))
          perf["rows_sent"] = max(rse_after - rse_before, 0)
      except Exception:
        pass

      # Diagnostics timing starts
      t_diag_start = time.perf_counter()
      explain_sql = f"EXPLAIN ANALYZE {sql}"
      try:
        cur.execute(explain_sql, params)
        exp_rows = cur.fetchall()
        txt = []
        for r in exp_rows:
          val = next(iter(r.values())) if isinstance(r, dict) else str(r)
          if val is not None:
            txt.append(str(val))
        perf["explain_type"] = "analyze"
        perf["explain"] = "\n".join(txt) if txt else None
      except Exception:
        try:
          cur.execute(f"EXPLAIN FORMAT=JSON {sql}", params)
          exp = cur.fetchone()
          val = next(iter(exp.values())) if exp else None
          perf["explain_type"] = "json"
          perf["explain"] = val
        except Exception:
          perf["explain_type"] = None
          perf["explain"] = None

      # Query cache stats
      try:
        cur.execute("SHOW STATUS LIKE 'Qcache_%'")
        qrows = cur.fetchall() or []
        perf["query_cache"] = {r.get('Variable_name'): r.get('Value') for r in qrows} or {"supported": False}
      except Exception:
        perf["query_cache"] = {"supported": False}
      perf["diagnostics_ms"] = round((time.perf_counter() - t_diag_start) * 1000.0, 2)
  perf["total_ms"] = round((time.perf_counter() - t_total_start) * 1000.0, 2)
  # For backward compatibility ms returns execution time only
  return rows, perf["execution_ms"], perf

# Mongo connection for optional data source
mongo_client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
mongo_db = mongo_client.get_database(os.getenv("MONGO_DB", "football_nonrelationaldb"))

def run_mongo(fn):
  t0 = time.perf_counter()
  result = fn(mongo_db)
  ms = round((time.perf_counter() - t0) * 1000.0, 2)
  return result, ms

@app.route("/")
def index():
    return render_template("index.html")

# Player Form over N
@app.route("/player/form")
def player_form_page():
    return render_template("player_form.html")

@app.get("/api/player/form")
def api_player_form():
    player_id = int(request.args.get("player_id"))
    comp = request.args.get("competition_id")
    season = request.args.get("season")
    n = int(request.args.get("n", 5))
    sql = """
      SELECT a.game_id, DATE_FORMAT(g.date, '%%Y-%%m-%%d') AS date_str, a.minutes_played, a.goals, a.assists,
             a.player_club_id AS player_club_id,
             g.home_club_id, hc.name AS home_name,
             g.away_club_id, ac.name AS away_name
      FROM appearance a
      JOIN game g ON g.game_id=a.game_id
      JOIN club hc ON hc.club_id=g.home_club_id
      JOIN club ac ON ac.club_id=g.away_club_id
      WHERE a.player_id=%s AND g.competition_id=%s AND g.season=%s
      ORDER BY g.date DESC LIMIT %s
    """
    rows, ms = run_sql(sql, (player_id, comp, season, n))
    return jsonify(dict(ms=ms, rows=rows))

# Top scorers by league-season with pagination
@app.route("/top-scorers")
def top_scorers_page():
    return render_template("top_scorers.html")

@app.get("/api/top-scorers")
def api_top_scorers():
    comp = request.args.get("competition_id")
    season = request.args.get("season")
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    offset = (page - 1) * page_size
    sql = """
      SELECT a.player_id, p.name as player_name, pb.image_url, SUM(a.goals) AS goals
      FROM appearance a 
      JOIN game g ON g.game_id=a.game_id
      JOIN player p ON p.player_id=a.player_id
      JOIN player_bio pb ON pb.player_id=a.player_id
      WHERE g.competition_id=%s AND g.season=%s
      GROUP BY a.player_id, p.name, pb.image_url
      ORDER BY goals DESC
      LIMIT %s OFFSET %s
    """
    rows, ms = run_sql(sql, (comp, season, page_size, offset))
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size))

# Match view
@app.route("/match")
def match_page():
    return render_template("match.html")

# Match details
@app.get("/api/match")
def api_match():
    gid = int(request.args.get("game_id"))
    sql_game = """
    SELECT
      g.game_id,
      g.competition_id,
      c.name AS competition_name,
      g.season,
      g.round,
      DATE_FORMAT(g.date, '%%Y-%%m-%%d') AS date_str,
      g.home_club_id,
      hc.name AS home_name,
      g.home_club_goals,
      g.away_club_id,
      ac.name AS away_name,
      g.away_club_goals,
      g.home_club_position,
      g.away_club_position,
      g.stadium,
      g.referee,
      g.attendance,
      g.home_club_formation,
      g.away_club_formation,
      g.home_club_manager_name,
      g.away_club_manager_name
    FROM game g
    JOIN club hc ON hc.club_id = g.home_club_id
    JOIN club ac ON ac.club_id = g.away_club_id
    JOIN competition c ON c.competition_id = g.competition_id
    WHERE g.game_id = %s

    """
    game, _ = run_sql(sql_game, (gid,))
    sql_ev = """
    SELECT
      ge.game_event_id,
      ge.game_id,
      ge.minute,
      ge.type AS event_type,
      ge.club_id,
      CASE WHEN ge.club_id = g.home_club_id THEN 'home' ELSE 'away' END AS side,
      ge.player_id,
      p1.name AS player_name,
      ge.player_assist_id,
      p2.name AS assist_name,
      ge.player_in_id,
      p3.name AS player_in_name,
      ge.description
    FROM game_events ge
    JOIN game g ON g.game_id = ge.game_id
    LEFT JOIN player p1 ON p1.player_id = ge.player_id
    LEFT JOIN player p2 ON p2.player_id = ge.player_assist_id
    LEFT JOIN player p3 ON p3.player_id = ge.player_in_id
    WHERE ge.game_id = %s
    ORDER BY ge.minute ASC, ge.game_event_id ASC 

    """
    events, ms2 = run_sql(sql_ev, (gid,))
    return jsonify(dict(game=(game[0] if game else None), events=events))

# Club page
@app.route("/club")
def club_page():
    return render_template("club.html")

# Club profile
@app.get("/api/club/<int:cid>/profile")
def api_club_profile(cid):
    sql = """
      WITH club_totals AS (
        SELECT c.club_id,
               c.name,
               c.average_age,
               c.stadium_name,
               c.stadium_seats,
               COALESCE(SUM(p.market_value_eur), 0) AS total_market_value_eur
        FROM club c
        LEFT JOIN player p
          ON p.current_club_id = c.club_id
         AND p.market_value_eur IS NOT NULL
        GROUP BY c.club_id, c.name, c.average_age, c.stadium_name, c.stadium_seats
      )
      SELECT ct.club_id,
             ct.name,
             ct.average_age,
             ct.stadium_name,
             ct.stadium_seats,
             ct.total_market_value_eur,
             DENSE_RANK() OVER (ORDER BY ct.total_market_value_eur DESC) AS market_value_rank,
             COUNT(*) OVER () AS clubs_ranked
      FROM club_totals ct
      WHERE ct.club_id=%s
    """
    rows, ms = run_sql(sql, (cid,))
    return jsonify(dict(ms=ms, row=(rows[0] if rows else None)))

# Club players (current squad by market value)
@app.get("/api/club/<int:cid>/players")
def api_club_players(cid):
    sql = """
      SELECT p.player_id, p.name, p.position, p.sub_position,
             p.market_value_eur, pb.image_url
      FROM player p
      LEFT JOIN player_bio pb ON pb.player_id = p.player_id
      WHERE p.current_club_id=%s
      ORDER BY p.market_value_eur DESC
      LIMIT 200
    """
    rows, ms = run_sql(sql, (cid,))
    return jsonify(dict(ms=ms, rows=rows))

# Club recent matches
@app.get("/api/club/<int:cid>/matches")
def api_club_matches(cid):
    limit_n = min(max(int(request.args.get("limit", 12)), 1), 100)
    comp = request.args.get("competition_id")
    base = """
      SELECT g.game_id,
             DATE_FORMAT(g.date, '%%Y-%%m-%%d') AS date_str,
             g.home_club_id, hc.name AS home_name,
             g.away_club_id, ac.name AS away_name,
             g.home_club_goals, g.away_club_goals,
             g.competition_id, c.name AS league_name
      FROM game g
      JOIN club hc ON hc.club_id = g.home_club_id
      JOIN club ac ON ac.club_id = g.away_club_id
      JOIN competition c ON c.competition_id = g.competition_id
      WHERE (g.home_club_id=%s OR g.away_club_id=%s)
    """
    params = [cid, cid]
    if comp:
      base += " AND g.competition_id=%s"
      params.append(comp)
    base += " ORDER BY g.date DESC LIMIT %s"
    params.append(limit_n)
    rows, ms = run_sql(base, tuple(params))
    return jsonify(dict(ms=ms, rows=rows))

# Club competitions for dropdown (with type)
@app.get("/api/club/<int:cid>/competitions")
def api_club_competitions(cid):
    sql = """
      SELECT DISTINCT g.competition_id, c.name AS competition_name, c.type
      FROM game g
      JOIN competition c ON c.competition_id = g.competition_id
      WHERE g.home_club_id=%s OR g.away_club_id=%s
      ORDER BY c.name
    """
    rows, ms = run_sql(sql, (cid, cid))
    return jsonify(dict(ms=ms, rows=rows))

# Clubs total market value ranking
@app.get("/api/clubs/market-ranking")
def api_clubs_market_ranking():
    limit_n = min(max(int(request.args.get("limit", 100)), 1), 500)
    comp = request.args.get("competition_id")
    if comp:
        sql = """
          WITH club_totals AS (
            SELECT c.club_id,
                   c.name,
                   COALESCE(SUM(p.market_value_eur), 0) AS total_market_value_eur
            FROM club c
            LEFT JOIN player p
              ON p.current_club_id = c.club_id
             AND p.market_value_eur IS NOT NULL
            GROUP BY c.club_id, c.name
          ),
          club_in_league AS (
            SELECT DISTINCT g.home_club_id AS club_id
            FROM game g WHERE g.competition_id=%s
            UNION
            SELECT DISTINCT g.away_club_id AS club_id
            FROM game g WHERE g.competition_id=%s
          )
          SELECT ct.club_id,
                 ct.name,
                 ct.total_market_value_eur,
                 DENSE_RANK() OVER (ORDER BY ct.total_market_value_eur DESC) AS market_value_rank
          FROM club_totals ct
          JOIN club_in_league l ON l.club_id = ct.club_id
          ORDER BY ct.total_market_value_eur DESC
          LIMIT %s
        """
        rows, ms = run_sql(sql, (comp, comp, limit_n))
    else:
        sql = """
          WITH club_totals AS (
            SELECT c.club_id,
                   c.name,
                   COALESCE(SUM(p.market_value_eur), 0) AS total_market_value_eur
            FROM club c
            LEFT JOIN player p
              ON p.current_club_id = c.club_id
             AND p.market_value_eur IS NOT NULL
            GROUP BY c.club_id, c.name
          )
          SELECT club_id,
                 name,
                 total_market_value_eur,
                 DENSE_RANK() OVER (ORDER BY total_market_value_eur DESC) AS market_value_rank
          FROM club_totals
          ORDER BY total_market_value_eur DESC
          LIMIT %s
        """
        rows, ms = run_sql(sql, (limit_n,))
    return jsonify(dict(ms=ms, rows=rows))

# Club transfer ROI view
@app.route("/club/roi")
def club_roi_page():
    return render_template("club_roi.html")

# Clubs list
@app.get("/api/clubs")
def api_clubs():
    sql = "SELECT club_id, name FROM club ORDER BY name"
    rows, ms = run_sql(sql)
    return jsonify(dict(ms=ms, rows=rows))

# Seasons a club bought players
@app.get("/api/clubs/<int:club_id>/seasons")
def api_club_seasons(club_id):
    sql = """
      SELECT DISTINCT transfer_season AS season
      FROM transfer
      WHERE to_club_id=%s
      ORDER BY season DESC
    """
    rows, ms = run_sql(sql, (club_id,))
    return jsonify(dict(ms=ms, rows=rows))

# Club search
@app.get("/api/clubs/search")
def api_clubs_search():
    q = request.args.get("q","").strip()
    sql = """
      SELECT club_id, name
      FROM club
      WHERE name LIKE %s
      ORDER BY name
      LIMIT 20
    """
    rows, ms = run_sql(sql, (f"%{q}%",))
    return jsonify(dict(ms=ms, rows=rows))

# Club transfer ROI data
@app.get("/api/club/roi")
def api_club_roi():
    club_id = int(request.args.get("club_id"))
    season  = request.args.get("season")
    sort_by = request.args.get("sort_by","post_minutes")
    order   = "DESC" if request.args.get("order","desc").lower()=="desc" else "ASC"
    cols = {"post_minutes","post_goals","post_assists","eur_per_minutes","eur_per_contrib","transfer_fee","market_value_in_eur"}
    if sort_by not in cols: sort_by = "post_minutes"
    sql = f"""
      SELECT player_id, transfer_season, transfer_fee, market_value_in_eur,
             post_minutes, post_goals, post_assists,
             eur_per_minutes, eur_per_contrib
      FROM view_club_transfer_roi
      WHERE club_id=%s AND transfer_season=%s AND transfer_fee IS NOT NULL AND transfer_fee > 0
      ORDER BY {sort_by} {order}
      LIMIT 200
    """
    rows, ms = run_sql(sql, (club_id, season))
    if rows:
        ids = tuple({r["player_id"] for r in rows})
        in_clause = ",".join(["%s"]*len(ids))
        psql = f"SELECT player_id, name FROM player WHERE player_id IN ({in_clause})"
        plist, _ = run_sql(psql, ids)
        name_map = {p["player_id"]: p["name"] for p in plist}
        for r in rows: r["player_name"] = name_map.get(r["player_id"])
    return jsonify(dict(ms=ms, rows=rows))

# Competitions list
@app.get("/api/competitions")
def api_competitions():
    sql = """
      SELECT DISTINCT c.competition_id, c.name AS competition_name, c.type
      FROM competition c
      ORDER BY c.name
    """
    rows, ms = run_sql(sql)
    return jsonify(dict(ms=ms, rows=rows))

# Seasons for a competition (for Top Scorers season dropdown)
@app.get("/api/competitions/<comp_id>/seasons")
def api_competition_seasons(comp_id):
    sql = """
      SELECT DISTINCT g.season
      FROM game g
      WHERE g.competition_id = %s
      ORDER BY g.season DESC
    """
    rows, ms = run_sql(sql, (comp_id,))
    return jsonify(dict(ms=ms, rows=rows))

# Matches by date
@app.get("/api/matches/by-date")
def api_matches_by_date():
    sel_date = request.args.get("date")
    sql = """
      SELECT
        g.competition_id                 AS league_id,
        g.game_id,
        g.home_club_id,  hc.name         AS home_name,
        g.away_club_id,  ac.name         AS away_name,
        g.home_club_goals, g.away_club_goals,
        c.name                            AS league_name
      FROM game g
      JOIN club hc ON hc.club_id = g.home_club_id
      JOIN club ac ON ac.club_id = g.away_club_id
      JOIN competition c ON c.competition_id = g.competition_id
      WHERE g.date = %s
      ORDER BY g.competition_id, g.game_id
    """
    rows, ms, perf = run_sql_ex(sql, (sel_date,))
    return jsonify(dict(ms=ms, rows=rows, date=sel_date, source="sql", perf=perf))

@app.get("/api/mongo/matches/by-date")
def api_mongo_matches_by_date():
    sel_date = request.args.get("date")
    def _q(db):
        cur = db.games.find({"date": sel_date}, {
            "competition_id": 1, "_id": 1,
            "home.club_id": 1, "home.name": 1, "home.goals": 1,
            "away.club_id": 1, "away.name": 1, "away.goals": 1
        })
        docs = list(cur)
        out = []
        for d in docs:
            home = d.get("home") or {}
            away = d.get("away") or {}
            out.append({
                "league_id": d.get("competition_id"),
                "game_id": d.get("_id"),
                "home_club_id": home.get("club_id"),
                "home_name": home.get("name"),
                "home_club_goals": home.get("goals"),
                "away_club_id": away.get("club_id"),
                "away_name": away.get("name"),
                "away_club_goals": away.get("goals"),
                "league_name": None
            })
        return out
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, date=sel_date, source="mongo"))

# Max match date
@app.get("/api/matches/max-date")
def api_matches_max_date():
  sql = "SELECT MAX(date) AS max_date FROM game"
  rows, ms, perf = run_sql_ex(sql)
  max_date = rows[0]['max_date'].strftime("%Y-%m-%d") if rows and rows[0]['max_date'] else None
  return jsonify(dict(ms=ms, max_date=max_date, source="sql", perf=perf))

@app.get("/api/mongo/matches/max-date")
def api_mongo_matches_max_date():
    def _q(db):
        # dates are strings YYYY-MM-DD, max lex order matches chronological
        doc = db.games.aggregate([{ "$group": { "_id": None, "max": { "$max": "$date" } } }])
        res = list(doc)
        return res[0].get("max") if res else None
    max_date, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, max_date=max_date, source="mongo"))


# Top market value players
@app.get("/api/players/top-market")
def api_top_market():
    limit_k  = min(max(int(request.args.get("k", 10)), 1), 50)
    sql = """
      SELECT p.player_id, p.name, p.market_value_eur, p.current_club_id
      FROM player p
      WHERE p.market_value_eur IS NOT NULL
      ORDER BY p.market_value_eur DESC
      LIMIT %s
    """
    rows, ms, perf = run_sql_ex(sql, (limit_k,))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Player search
@app.get("/api/players/search")
def api_players_search():
    q = request.args.get("q","").strip()
    sql = """
      SELECT player_id, name
      FROM player
      WHERE name LIKE %s
      ORDER BY name
      LIMIT 20
    """
    rows, ms, perf = run_sql_ex(sql, (f"%{q}%",))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Competitions for a player
@app.get("/api/players/<int:pid>/competitions")
def api_player_competitions(pid):
    sql = """
      SELECT DISTINCT g.competition_id, c.name AS competition_name, c.type AS competition_type
      FROM appearance a
      JOIN game g ON g.game_id = a.game_id
      JOIN competition c ON c.competition_id = g.competition_id
      WHERE a.player_id = %s
      ORDER BY g.competition_id
    """
    rows, ms = run_sql(sql, (pid,))
    return jsonify(dict(ms=ms, rows=rows))

# Seasons for a player in a competition
@app.get("/api/players/<int:pid>/seasons")
def api_player_seasons(pid):
    comp = request.args.get("competition_id")
    sql = """
      SELECT DISTINCT g.season
      FROM appearance a
      JOIN game g ON g.game_id = a.game_id
      WHERE a.player_id = %s AND g.competition_id = %s
      ORDER BY g.season DESC
    """
    rows, ms = run_sql(sql, (pid, comp))
    return jsonify(dict(ms=ms, rows=rows))

@app.route("/player/profile")
def player_profile_page():
    return render_template("player_profile.html")

# Player profile
@app.get("/api/player/<int:pid>/profile")
def api_player_profile(pid):
    sql = """
      SELECT p.player_id, p.name, p.position, p.sub_position,
             p.current_club_id, c.name AS current_club_name,
             p.market_value_eur, p.highest_market_value_eur,
             pb.image_url, pb.height_in_cm, pb.dob, pb.country_of_citizenship, pb.foot, pb.city_of_birth, pb.agent_name, pb.contract_expiration_date
      FROM player p
      LEFT JOIN club c ON c.club_id=p.current_club_id
      JOIN player_bio pb ON pb.player_id=p.player_id
      WHERE p.player_id=%s
    """
    rows, ms, perf = run_sql_ex(sql, (pid,))
    return jsonify(dict(ms=ms, row=rows[0] if rows else None, perf=perf))

# Player season summary
@app.get("/api/player/<int:pid>/season-summary")
def api_player_season_summary(pid):
    sql = """
      SELECT g.competition_id, g.season,
             COUNT(*) AS apps,
             SUM(a.minutes_played) AS minutes,
             SUM(a.goals) AS goals,
             SUM(a.assists) AS assists,
             SUM(a.yellow_cards) AS yellows,
             SUM(a.red_cards) AS reds
      FROM appearance a JOIN game g ON g.game_id=a.game_id
      WHERE a.player_id=%s
      GROUP BY g.competition_id, g.season
      ORDER BY g.season DESC, g.competition_id
    """
    rows, ms, perf = run_sql_ex(sql, (pid,))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Player matches
@app.get("/api/player/<int:pid>/matches")
def api_player_matches(pid):
    comp = request.args.get("competition_id")
    season = request.args.get("season")
    limit_n = int(request.args.get("n", 10))
    sql = """
      SELECT DATE_FORMAT(g.date, '%%Y-%%m-%%d') AS date_str, g.competition_id, g.season,
             a.minutes_played, a.goals, a.assists, a.yellow_cards, a.red_cards,
             g.home_club_id, hc.name AS home_name, ac.name AS away_name, g.away_club_id, g.home_club_goals, g.away_club_goals
      FROM appearance a 
      JOIN game g ON g.game_id=a.game_id
      JOIN club hc ON hc.club_id=g.home_club_id
      JOIN club ac ON ac.club_id=g.away_club_id
      WHERE a.player_id=%s AND g.competition_id=%s AND g.season=%s
      ORDER BY g.date DESC
      LIMIT %s
    """
    rows, ms, perf = run_sql_ex(sql, (pid, comp, season, limit_n))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Mongo: Player matches (from player_seasons.latest_matches)
@app.get("/api/mongo/player/<int:pid>/matches")
def api_mongo_player_matches(pid):
  comp = request.args.get("competition_id")
  season = request.args.get("season")
  limit_n = int(request.args.get("n", 10))

  def _q(db):
    doc = db.player_seasons.find_one(
      {"player_id": int(pid), "competition_id": comp, "season": season},
      {"latest_matches": 1, "matches": 1}
    )
    arr = []
    if doc:
      arr = doc.get("latest_matches") or doc.get("matches") or []
    out = []

    # helper: first non-None value (allows 0)
    def first_not_none(*vals):
      for v in vals:
        if v is not None:
          return v
      return None

    for m in arr[:limit_n]:
      home_id = m.get("home_club_id")
      away_id = m.get("away_club_id")
      home_name = m.get("home_name") or (f"Club {home_id}" if home_id is not None else None)
      away_name = m.get("away_name") or (f"Club {away_id}" if away_id is not None else None)
      out.append({
        "date_str": m.get("date_str") or m.get("date"),
        "competition_id": comp,
        "season": season,
        "minutes_played": first_not_none(m.get("minutes_played"), m.get("min"), m.get("minutes")),
        "goals": first_not_none(m.get("goals"), m.get("g")),
        "assists": first_not_none(m.get("assists"), m.get("a")),
        "yellow_cards": first_not_none(m.get("yellow_cards"), m.get("yc"), m.get("yellow")),
        "red_cards": first_not_none(m.get("red_cards"), m.get("rc"), m.get("red")),
        "home_club_id": home_id,
        "home_name": home_name,
        "away_name": away_name,
        "away_club_id": away_id,
        "home_club_goals": first_not_none(m.get("home_club_goals"), m.get("home_goals"), m.get("home_score"), m.get("hg")),
        "away_club_goals": first_not_none(m.get("away_club_goals"), m.get("away_goals"), m.get("away_score"), m.get("ag")),
      })
    return out

  rows, ms = run_mongo(_q)
  return jsonify(dict(ms=ms, rows=rows))

# Player career transfers
@app.get("/api/player/<int:pid>/career")
def api_player_career(pid):
    sql = """
      SELECT t.transfer_date, t.transfer_season, t.from_club_id, fc.name AS from_club,
             t.to_club_id, tc.name AS to_club, t.transfer_fee, t.market_value_in_eur
      FROM transfer t
      LEFT JOIN club fc ON fc.club_id=t.from_club_id
      LEFT JOIN club tc ON tc.club_id=t.to_club_id
      WHERE t.player_id=%s AND t.transfer_fee IS NOT NULL
      ORDER BY t.transfer_date DESC
    """
    rows, ms = run_sql(sql, (pid,))
    return jsonify(dict(ms=ms, rows=rows))

# Market value comparison by category
@app.get("/api/market-compare")
def api_market_compare():
  category = (request.args.get("category") or "").strip().lower()
  value = request.args.get("value")
  limit_n = min(max(int(request.args.get("limit", 100)), 1), 200)

  allowed = {"age", "citizenship", "club", "position", "agent", "city"}
  if category not in allowed or value is None or value == "":
    return jsonify(dict(ms=0, rows=[], error="invalid-params")), 400

  # Build SQL according to category
  base_select = """
    SELECT p.player_id, p.name, p.market_value_eur
    FROM player p
    JOIN player_bio pb ON pb.player_id = p.player_id
  """
  where = []
  params = []

  if category == "age":
    # Expect integer age
    try:
      age_val = int(value)
    except ValueError:
      return jsonify(dict(ms=0, rows=[], error="invalid-age")), 400
    where.append("pb.dob IS NOT NULL AND TIMESTAMPDIFF(YEAR, pb.dob, CURDATE()) = %s")
    params.append(age_val)
  elif category == "citizenship":
    where.append("pb.country_of_citizenship = %s")
    params.append(value)
  elif category == "club":
    # Expect current club id integer
    try:
      club_id = int(value)
    except ValueError:
      return jsonify(dict(ms=0, rows=[], error="invalid-club")), 400
    where.append("p.current_club_id = %s")
    params.append(club_id)
  elif category == "position":
    where.append("p.position = %s")
    params.append(value)
  elif category == "agent":
    where.append("pb.agent_name = %s")
    params.append(value)
  elif category == "city":
    where.append("pb.city_of_birth = %s")
    params.append(value)

  where.append("p.market_value_eur IS NOT NULL")
  sql = f"""
    {base_select}
    WHERE {' AND '.join(where)}
    ORDER BY p.market_value_eur DESC
    LIMIT %s
  """
  params.append(limit_n)

  rows, ms = run_sql(sql, tuple(params))
  return jsonify(dict(ms=ms, rows=rows, category=category, value=value, limit=limit_n))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)
