import os, time
from flask import Flask, render_template, request, jsonify, send_from_directory
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

# Create player page
@app.route("/player/create")
def player_create_page():
    return render_template("create_player.html")

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

# Create match page
@app.route("/match/create")
def match_create_page():
    return render_template("create_match.html")

# Create match (POST)
@app.post("/api/match")
def api_create_match():
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get("date"):
            return jsonify({"error": "Date is required"}), 400
        if not data.get("home_club_id"):
            return jsonify({"error": "Home club ID is required"}), 400
        if not data.get("away_club_id"):
            return jsonify({"error": "Away club ID is required"}), 400
        if not data.get("competition_id"):
            return jsonify({"error": "Competition ID is required"}), 400
        if not data.get("season"):
            return jsonify({"error": "Season is required"}), 400
        
        home_id = int(data.get("home_club_id"))
        away_id = int(data.get("away_club_id"))
        
        # Validate clubs are different
        if home_id == away_id:
            return jsonify({"error": "Home and Away clubs must be different"}), 400
        
        # Validate scores are non-negative
        home_goals = int(data.get("home_club_goals", 0)) if data.get("home_club_goals") else 0
        away_goals = int(data.get("away_club_goals", 0)) if data.get("away_club_goals") else 0
        
        if home_goals < 0 or away_goals < 0:
            return jsonify({"error": "Goals cannot be negative"}), 400
        
        # Get the next game_id (max + 1)
        sql_max_id = "SELECT COALESCE(MAX(game_id), 0) + 1 AS next_id FROM game"
        rows, _ = run_sql(sql_max_id)
        game_id = rows[0]["next_id"] if rows else 1
        
        sql_insert = """
            INSERT INTO game 
            (game_id, date, match_time, competition_id, season, round,
             home_club_id, away_club_id,
             home_club_goals, away_club_goals,
             home_club_position, away_club_position,
             stadium, referee, attendance,
             home_club_manager_name, away_club_manager_name,
             home_club_formation, away_club_formation)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        match_time = data.get("match_time") or None
        home_pos = int(data.get("home_club_position")) if data.get("home_club_position") else None
        away_pos = int(data.get("away_club_position")) if data.get("away_club_position") else None
        attendance = int(data.get("attendance")) if data.get("attendance") else None
        
        run_sql(sql_insert, (
            game_id,
            data.get("date"),
            match_time,
            data.get("competition_id"),
            data.get("season"),
            data.get("round") or None,
            home_id,
            away_id,
            home_goals,
            away_goals,
            home_pos,
            away_pos,
            data.get("stadium") or None,
            data.get("referee") or None,
            attendance,
            data.get("home_club_manager_name") or None,
            data.get("away_club_manager_name") or None,
            data.get("home_club_formation") or None,
            data.get("away_club_formation") or None
        ))
        
        return jsonify({"game_id": game_id, "success": True}), 201
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Edit match page
@app.route("/match/edit")
def match_edit_page():
    return render_template("edit_match.html")

# Get match data for editing
@app.get("/api/match/<int:game_id>/edit")
def api_match_edit(game_id):
    sql = """
      SELECT g.game_id, DATE_FORMAT(g.date, '%%Y-%%m-%%d') AS date_str, 
             TIME_FORMAT(COALESCE(g.match_time, '00:00:00'), '%%H:%%i') AS match_time,
             g.competition_id, g.season, g.round,
             g.home_club_id, g.away_club_id,
             g.home_club_goals, g.away_club_goals,
             g.home_club_position, g.away_club_position,
             g.stadium, g.referee, g.attendance,
             g.home_club_manager_name, g.away_club_manager_name,
             g.home_club_formation, g.away_club_formation
      FROM game g
      WHERE g.game_id = %s
    """
    rows, ms = run_sql(sql, (game_id,))
    return jsonify(dict(ms=ms, row=rows[0] if rows else None))

# Update match (POST)
@app.post("/api/match/<int:game_id>/update")
def api_update_match(game_id):
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get("date"):
            return jsonify({"error": "Date is required"}), 400
        if not data.get("home_club_id"):
            return jsonify({"error": "Home club ID is required"}), 400
        if not data.get("away_club_id"):
            return jsonify({"error": "Away club ID is required"}), 400
        
        home_id = int(data.get("home_club_id"))
        away_id = int(data.get("away_club_id"))
        
        # Validate clubs are different
        if home_id == away_id:
            return jsonify({"error": "Home and Away clubs must be different"}), 400
        
        # Validate scores are non-negative
        home_goals = int(data.get("home_club_goals", 0)) if data.get("home_club_goals") else None
        away_goals = int(data.get("away_club_goals", 0)) if data.get("away_club_goals") else None
        
        if home_goals is not None and home_goals < 0:
            return jsonify({"error": "Goals cannot be negative"}), 400
        if away_goals is not None and away_goals < 0:
            return jsonify({"error": "Goals cannot be negative"}), 400
        
        match_time = data.get("match_time") or None
        home_pos = int(data.get("home_club_position")) if data.get("home_club_position") else None
        away_pos = int(data.get("away_club_position")) if data.get("away_club_position") else None
        attendance = int(data.get("attendance")) if data.get("attendance") else None
        
        run_sql(sql_update, (
            data.get("date"),
            match_time,
            data.get("competition_id"),
            data.get("season"),
            data.get("round") or None,
            home_id,
            away_id,
            home_goals,
            away_goals,
            home_pos,
            away_pos,
            data.get("stadium") or None,
            data.get("referee") or None,
            attendance,
            data.get("home_club_manager_name") or None,
            data.get("away_club_manager_name") or None,
            data.get("home_club_formation") or None,
            data.get("away_club_formation") or None,
            game_id
        ))
        
        return jsonify({"game_id": game_id, "success": True}), 200
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

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
      TIME_FORMAT(COALESCE(g.match_time, '00:00:00'), '%%H:%%i') AS match_time,
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
            "away.club.id": 1, "away.name": 1, "away.goals": 1
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

# Upload player image
@app.post("/api/upload/player-image")
def api_upload_player_image():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
        
        # Validate file is an image
        if not file.content_type.startswith('image/'):
            return jsonify({"error": "File must be an image"}), 400
        
        # Create uploads directory if it doesn't exist
        upload_dir = os.path.join(os.path.dirname(__file__), 'uploads')
        os.makedirs(upload_dir, exist_ok=True)
        
        # Generate unique filename
        import uuid
        ext = os.path.splitext(file.filename)[1]
        filename = f"{uuid.uuid4()}{ext}"
        filepath = os.path.join(upload_dir, filename)
        
        # Save file
        file.save(filepath)
        
        # Return URL path (adjust based on your static file serving setup)
        image_url = f"/uploads/{filename}"
        
        return jsonify({"image_url": image_url, "success": True}), 200
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Create player (POST)
@app.post("/api/player")
def api_create_player():
    try:
        data = request.get_json()
        
        # Validate required field
        if not data.get("name"):
            return jsonify({"error": "Name is required"}), 400
        
        # Get the next player_id (max + 1)
        sql_max_id = "SELECT COALESCE(MAX(player_id), 0) + 1 AS next_id FROM player"
        rows, _ = run_sql(sql_max_id)
        player_id = rows[0]["next_id"] if rows else 1
        
        # Insert into player table
        sql_player = """
            INSERT INTO player 
            (player_id, name, position, sub_position, current_club_id, 
             market_value_eur, highest_market_value_eur, first_name, last_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        current_club_id = data.get("current_club_id")
        if current_club_id == "": current_club_id = None
        else: current_club_id = int(current_club_id) if current_club_id else None
        
        market_value = data.get("market_value_eur")
        if market_value == "": market_value = None
        else: market_value = int(market_value) if market_value else None
        
        highest_market_value = data.get("highest_market_value_eur")
        if highest_market_value == "": highest_market_value = None
        else: highest_market_value = int(highest_market_value) if highest_market_value else None
        
        run_sql(sql_player, (
            player_id,
            data.get("name"),
            data.get("position") or None,
            data.get("sub_position") or None,
            current_club_id,
            market_value,
            highest_market_value,
            data.get("first_name") or None,  # Add this
            data.get("last_name") or None    # Add this
        ))
        
        # Insert into player_bio table
        sql_bio = """
            INSERT INTO player_bio 
            (player_id, height_in_cm, dob, country_of_citizenship, foot, 
             city_of_birth, country_of_birth, image_url, agent_name, contract_expiration_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        dob = data.get("dob")
        if dob == "": dob = None
        
        contract_exp = data.get("contract_expiration_date")
        if contract_exp == "": contract_exp = None
        
        height = data.get("height_in_cm")
        if height == "": height = None
        else: height = int(height) if height else None
        
        run_sql(sql_bio, (
            player_id,
            height,
            dob or None,
            data.get("country_of_citizenship") or None,
            data.get("foot") or None,
            data.get("city_of_birth") or None,
            data.get("country_of_birth") or None,
            data.get("image_url") or None,
            data.get("agent_name") or None,
            contract_exp or None
        ))
        
        return jsonify({"player_id": player_id, "success": True}), 201
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

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

# Players list (SQL)
@app.get("/api/players")
def api_players_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    search = request.args.get("search", "").strip()
    offset = (page - 1) * page_size

    sql = """
      SELECT p.player_id, p.name, p.position, p.sub_position,
             p.market_value_eur, c.name AS current_club_name,
             pb.image_url, pb.dob, pb.country_of_citizenship
      FROM player p
      LEFT JOIN club c ON c.club_id = p.current_club_id
      LEFT JOIN player_bio pb ON pb.player_id = p.player_id
    """
    params = []

    if search:
        sql += " WHERE p.name LIKE %s"
        params.append(f"%{search}%")

    count_sql = "SELECT COUNT(*) as total FROM player p"
    if search:
        count_sql += " WHERE p.name LIKE %s"

    count_rows, _ = run_sql(count_sql, tuple(params) if search else ())
    total = count_rows[0]["total"] if count_rows else 0

    sql += " ORDER BY p.market_value_eur DESC LIMIT %s OFFSET %s"
    params.extend([page_size, offset])

    rows, ms, perf = run_sql_ex(sql, tuple(params))
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size, total=total, perf=perf))

# Players list (MongoDB)
@app.get("/api/mongo/players")
def api_mongo_players_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    search = request.args.get("search", "").strip()
    skip = (page - 1) * page_size

    def _q(db):
        query = {}
        if search:
            query["name"] = {"$regex": search, "$options": "i"}
        
        total = db.players.count_documents(query)
        cur = db.players.find(query, {
            "player_id": 1, "name": 1, "position": 1, "sub_position": 1,
            "market_value_eur": 1, "current_club_name": 1,
            "image_url": 1, "dob": 1, "country_of_citizenship": 1
        }).sort("market_value_eur", -1).skip(skip).limit(page_size)
        
        rows = list(cur)
        return rows, total

    rows, ms = run_mongo(lambda db: _q(db))
    rows_list, total = rows
    
    # Convert MongoDB _id to match expected structure
    for r in rows_list:
        if "_id" in r:
            del r["_id"]
    
    return jsonify(dict(ms=ms, rows=rows_list, page=page, page_size=page_size, total=total))

@app.route("/players")
def players_page():
    return render_template("player_table.html")

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    upload_dir = os.path.join(os.path.dirname(__file__), 'uploads')
    return send_from_directory(upload_dir, filename)

@app.route("/player/edit")
def player_edit_page():
    return render_template("edit_player.html")

# Get player data for editing
@app.get("/api/player/<int:pid>/edit")
def api_player_edit(pid):
    sql = """
      SELECT p.player_id, p.name, p.first_name, p.last_name, p.position, p.sub_position,
             p.current_club_id, c.name AS current_club_name,
             p.market_value_eur, p.highest_market_value_eur,
             pb.image_url, pb.height_in_cm, pb.dob, pb.country_of_citizenship, pb.foot, 
             pb.city_of_birth, pb.country_of_birth, pb.agent_name, pb.contract_expiration_date
      FROM player p
      LEFT JOIN club c ON c.club_id=p.current_club_id
      LEFT JOIN player_bio pb ON pb.player_id=p.player_id
      WHERE p.player_id=%s
    """
    rows, ms = run_sql(sql, (pid,))
    
    if rows:
        row = rows[0]
        # Format dates as YYYY-MM-DD strings for HTML date inputs
        if row.get('dob') and hasattr(row['dob'], 'strftime'):
            row['dob'] = row['dob'].strftime('%Y-%m-%d')
        if row.get('contract_expiration_date') and hasattr(row['contract_expiration_date'], 'strftime'):
            row['contract_expiration_date'] = row['contract_expiration_date'].strftime('%Y-%m-%d')
        return jsonify(dict(ms=ms, row=row))
    
    return jsonify(dict(ms=ms, row=None))

# Update player (POST)
@app.post("/api/player/<int:pid>/update")
def api_update_player(pid):
    try:
        data = request.get_json()
        
        if not data.get("name"):
            return jsonify({"error": "Name is required"}), 400
        
        # Update player table
        sql_player = """
            UPDATE player 
            SET name=%s, first_name=%s, last_name=%s, position=%s, sub_position=%s,
                current_club_id=%s, market_value_eur=%s, highest_market_value_eur=%s
            WHERE player_id=%s
        """
        
        current_club_id = data.get("current_club_id")
        if current_club_id == "": current_club_id = None
        else: current_club_id = int(current_club_id) if current_club_id else None
        
        market_value = data.get("market_value_eur")
        if market_value == "": market_value = None
        else: market_value = int(market_value) if market_value else None
        
        highest_market_value = data.get("highest_market_value_eur")
        if highest_market_value == "": highest_market_value = None
        else: highest_market_value = int(highest_market_value) if highest_market_value else None
        
        run_sql(sql_player, (
            data.get("name"),
            data.get("first_name") or None,
            data.get("last_name") or None,
            data.get("position") or None,
            data.get("sub_position") or None,
            current_club_id,
            market_value,
            highest_market_value,
            pid
        ))
        
        # Update player_bio table
        sql_bio = """
            UPDATE player_bio 
            SET height_in_cm=%s, dob=%s, country_of_citizenship=%s, foot=%s,
                city_of_birth=%s, country_of_birth=%s, image_url=%s, 
                agent_name=%s, contract_expiration_date=%s
            WHERE player_id=%s
        """
        
        dob = data.get("dob")
        if dob == "": dob = None
        
        contract_exp = data.get("contract_expiration_date")
        if contract_exp == "": contract_exp = None
        
        height = data.get("height_in_cm")
        if height == "": height = None
        else: height = int(height) if height else None
        
        run_sql(sql_bio, (
            height,
            dob or None,
            data.get("country_of_citizenship") or None,
            data.get("foot") or None,
            data.get("city_of_birth") or None,
            data.get("country_of_birth") or None,
            data.get("image_url") or None,
            data.get("agent_name") or None,
            contract_exp or None,
            pid
        ))
        
        return jsonify({"player_id": pid, "success": True}), 200
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Delete player
@app.delete("/api/player/<int:pid>")
def api_delete_player(pid):
    try:
        # First, get the image URL to delete the file
        sql_get_image = "SELECT pb.image_url FROM player_bio pb WHERE pb.player_id=%s"
        image_rows, _ = run_sql(sql_get_image, (pid,))
        
        # Delete from player_bio first (foreign key constraint)
        sql_bio = "DELETE FROM player_bio WHERE player_id=%s"
        run_sql(sql_bio, (pid,))
        
        # Delete from player
        sql_player = "DELETE FROM player WHERE player_id=%s"
        run_sql(sql_player, (pid,))
        
        # Delete the image file if it exists
        if image_rows and image_rows[0].get('image_url'):
            image_url = image_rows[0]['image_url']
            # Extract filename from URL (e.g., "/uploads/abc123.jpg" -> "abc123.jpg")
            if image_url.startswith('/uploads/'):
                filename = image_url.replace('/uploads/', '')
                filepath = os.path.join(os.path.dirname(__file__), 'uploads', filename)
                try:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                except Exception as file_err:
                    print(f"Warning: Could not delete image file {filepath}: {file_err}")
        
        return jsonify({"success": True, "message": f"Player {pid} deleted"}), 200
    
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

@app.route("/clubs")
def clubs_page():
    return render_template("club_table.html")

# Clubs list (SQL)
@app.get("/api/clubs/list")
def api_clubs_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    search = request.args.get("search", "").strip()
    offset = (page - 1) * page_size

    sql = """
      SELECT c.club_id, c.name, c.domestic_competition_id, c.squad_size, c.average_age,
             c.stadium_name, c.stadium_seats,
             COALESCE(SUM(p.market_value_eur), 0) AS total_market_value_eur,
             COUNT(p.player_id) AS player_count
      FROM club c
      LEFT JOIN player p ON p.current_club_id = c.club_id
    """
    params = []

    if search:
        sql += " WHERE c.name LIKE %s"
        params.append(f"%{search}%")

    sql += " GROUP BY c.club_id, c.name, c.domestic_competition_id, c.squad_size, c.average_age, c.stadium_name, c.stadium_seats"

    count_sql = "SELECT COUNT(*) as total FROM club c"
    if search:
        count_sql += " WHERE c.name LIKE %s"

    count_rows, _ = run_sql(count_sql, tuple(params) if search else ())
    total = count_rows[0]["total"] if count_rows else 0

    sql += " ORDER BY c.name LIMIT %s OFFSET %s"
    params.extend([page_size, offset])

    rows, ms, perf = run_sql_ex(sql, tuple(params))
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size, total=total, perf=perf))


# Clubs list (MongoDB)
@app.get("/api/mongo/clubs/list")
def api_mongo_clubs_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    search = request.args.get("search", "").strip()
    skip = (page - 1) * page_size

    def _q(db):
        query = {}
        if search:
            query["name"] = {"$regex": search, "$options": "i"}
        
        total = db.clubs.count_documents(query)
        cur = db.clubs.find(query, {
            "club_id": 1, "name": 1, "squad_size": 1, "average_age": 1,
            "stadium_name": 1, "stadium_seats": 1, "total_market_value_eur": 1,
            "player_count": 1
        }).sort("name", 1).skip(skip).limit(page_size)
        
        rows = list(cur)
        return rows, total

    rows, ms = run_mongo(lambda db: _q(db))
    rows_list, total = rows
    
    # Clean up MongoDB _id field
    for r in rows_list:
        if "_id" in r:
            del r["_id"]
    
    return jsonify(dict(ms=ms, rows=rows_list, page=page, page_size=page_size, total=total))

# Create club page
@app.route("/club/create")
def club_create_page():
    return render_template("create_club.html")

# Create club (POST)
@app.post("/api/club")
def api_create_club():
    try:
        data = request.get_json()
        
        # Validate required field
        if not data.get("name"):
            return jsonify({"error": "Club name is required"}), 400
        
        # Get the next club_id (max + 1)
        sql_max_id = "SELECT COALESCE(MAX(club_id), 0) + 1 AS next_id FROM club"
        rows, _ = run_sql(sql_max_id)
        club_id = rows[0]["next_id"] if rows else 1
        
        # Insert into club table
        sql_club = """
            INSERT INTO club 
            (club_id, name, domestic_competition_id, squad_size, average_age, 
             stadium_name, stadium_seats)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        domestic_competition_id = data.get("domestic_competition_id")
        if domestic_competition_id == "": domestic_competition_id = None
        else: domestic_competition_id = int(domestic_competition_id) if domestic_competition_id else None
        
        squad_size = data.get("squad_size")
        if squad_size == "": squad_size = None
        else: squad_size = int(squad_size) if squad_size else None
        
        average_age = data.get("average_age")
        if average_age == "": average_age = None
        else: average_age = float(average_age) if average_age else None
        
        stadium_seats = data.get("stadium_seats")
        if stadium_seats == "": stadium_seats = None
        else: stadium_seats = int(stadium_seats) if stadium_seats else None
        
        run_sql(sql_club, (
            club_id,
            data.get("name"),
            domestic_competition_id,
            squad_size,
            average_age,
            data.get("stadium_name") or None,
            stadium_seats
        ))
        
        return jsonify({"club_id": club_id, "success": True}), 201
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Edit club page
@app.route("/club/edit")
def club_edit_page():
    return render_template("edit_club.html")

# Get club data for editing
@app.get("/api/club/<int:club_id>/edit")
def api_club_edit(club_id):
    sql = """
      SELECT club_id, name, domestic_competition_id, squad_size, average_age,
             stadium_name, stadium_seats
      FROM club
      WHERE club_id=%s
    """
    rows, ms = run_sql(sql, (club_id,))
    return jsonify(dict(ms=ms, row=rows[0] if rows else None))

# Update club (POST)
@app.post("/api/club/<int:club_id>/update")
def api_update_club(club_id):
    try:
        data = request.get_json()
        
        if not data.get("name"):
            return jsonify({"error": "Club name is required"}), 400
        
        sql_club = """
            UPDATE club 
            SET name=%s, domestic_competition_id=%s, squad_size=%s, average_age=%s,
                stadium_name=%s, stadium_seats=%s
            WHERE club_id=%s
        """
        
        domestic_competition_id = data.get("domestic_competition_id")
        if domestic_competition_id == "": domestic_competition_id = None
        else: domestic_competition_id = int(domestic_competition_id) if domestic_competition_id else None
        
        squad_size = data.get("squad_size")
        if squad_size == "": squad_size = None
        else: squad_size = int(squad_size) if squad_size else None
        
        average_age = data.get("average_age")
        if average_age == "": average_age = None
        else: average_age = float(average_age) if average_age else None
        
        stadium_seats = data.get("stadium_seats")
        if stadium_seats == "": stadium_seats = None
        else: stadium_seats = int(stadium_seats) if stadium_seats else None
        
        run_sql(sql_club, (
            data.get("name"),
            domestic_competition_id,
            squad_size,
            average_age,
            data.get("stadium_name") or None,
            stadium_seats,
            club_id
        ))
        
        return jsonify({"club_id": club_id, "success": True}), 200
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Upload club logo
@app.post("/api/upload/club-logo")
def api_upload_club_logo():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
        
        # Validate file is an image
        if not file.content_type.startswith('image/'):
            return jsonify({"error": "File must be an image"}), 400
        
        # Create uploads directory if it doesn't exist
        upload_dir = os.path.join(os.path.dirname(__file__), 'uploads')
        os.makedirs(upload_dir, exist_ok=True)
        
        # Generate unique filename
        import uuid
        ext = os.path.splitext(file.filename)[1]
        filename = f"{uuid.uuid4()}{ext}"
        filepath = os.path.join(upload_dir, filename)
        
        # Save file
        file.save(filepath)
        
        # Return URL path
        image_url = f"/uploads/{filename}"
        
        return jsonify({"image_url": image_url, "success": True}), 200
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Delete club
@app.delete("/api/club/<int:club_id>")
def api_delete_club(club_id):
    try:
        # Delete from club
        sql_delete = "DELETE FROM club WHERE club_id=%s"
        run_sql(sql_delete, (club_id,))
        
        return jsonify({"success": True, "message": f"Club {club_id} deleted"}), 200
    
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

@app.route("/games")
def games_page():
    return render_template("game_table.html")

# Games list
@app.get("/api/games/list")
def api_games_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    date = request.args.get("date", "").strip()
    competition = request.args.get("competition_id", "").strip()
    season = request.args.get("season", "").strip()
    offset = (page - 1) * page_size

    sql = """
      SELECT g.game_id, DATE_FORMAT(g.date, '%%Y-%%m-%%d') AS date_str,
             g.competition_id, c.name AS league_name,
             g.round, g.season,
             g.home_club_id, hc.name AS home_name,
             g.away_club_id, ac.name AS away_name,
             g.home_club_goals, g.away_club_goals,
             g.stadium, g.attendance
      FROM game g
      JOIN competition c ON c.competition_id = g.competition_id
      JOIN club hc ON hc.club_id = g.home_club_id
      JOIN club ac ON ac.club_id = g.away_club_id
      WHERE 1=1
    """
    params = []

    if date:
        sql += " AND g.date = %s"
        params.append(date)
    if competition:
        sql += " AND g.competition_id = %s"
        params.append(competition)
    if season:
        sql += " AND g.season = %s"
        params.append(season)

    count_sql = "SELECT COUNT(*) as total FROM game g WHERE 1=1"
    if date:
        count_sql += " AND g.date = %s"
    if competition:
        count_sql += " AND g.competition_id = %s"
    if season:
        count_sql += " AND g.season = %s"

    count_rows, _ = run_sql(count_sql, tuple(params))
    total = count_rows[0]["total"] if count_rows else 0

    sql += " ORDER BY g.date DESC LIMIT %s OFFSET %s"
    params.extend([page_size, offset])

    rows, ms, perf = run_sql_ex(sql, tuple(params))
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size, total=total, perf=perf))

# Delete match
@app.delete("/api/match/<int:game_id>")
def api_delete_match(game_id):
    try:
        # Delete from match
        sql_delete = "DELETE FROM game WHERE game_id=%s"
        run_sql(sql_delete, (game_id,))
        
        return jsonify({"success": True, "message": f"Match {game_id} deleted"}), 200
    
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Games seasons
@app.get("/api/games/seasons")
def api_games_seasons():
    sql = """
      SELECT DISTINCT g.season
      FROM game g
      ORDER BY g.season DESC
    """
    rows, ms = run_sql(sql)
    return jsonify(dict(ms=ms, rows=rows))

@app.route("/appearances")
def appearances_page():
    return render_template("appearance_table.html")

@app.route("/appearance/create")
def appearance_create_page():
    return render_template("create_appearance.html")

@app.route("/appearance/edit")
def appearance_edit_page():
    return render_template("edit_appearance.html")

# Get appearances list with pagination and search
@app.get("/api/appearances/list")
def api_appearances_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    search = request.args.get("search", "").strip()
    offset = (page - 1) * page_size

    sql = """
      SELECT a.appearance_id, a.game_id, a.player_id, a.player_club_id, 
             a.player_current_club_id, a.date,
             a.yellow_cards, a.red_cards, a.goals, a.assists, a.minutes_played,
             p.name AS player_name, c.name AS club_name
      FROM appearance a
      LEFT JOIN player p ON p.player_id = a.player_id
      LEFT JOIN club c ON c.club_id = a.player_club_id
      WHERE 1=1
    """
    params = []

    if search:
        sql += " AND (a.game_id LIKE %s OR p.name LIKE %s OR c.name LIKE %s)"
        search_param = f"%{search}%"
        params.extend([search_param, search_param, search_param])

    count_sql = "SELECT COUNT(*) as total FROM appearance a LEFT JOIN player p ON p.player_id = a.player_id LEFT JOIN club c ON c.club_id = a.player_club_id WHERE 1=1"
    if search:
        count_sql += " AND (a.game_id LIKE %s OR p.name LIKE %s OR c.name LIKE %s)"

    count_rows, _ = run_sql(count_sql, tuple(params))
    total = count_rows[0]["total"] if count_rows else 0

    sql += " ORDER BY a.date DESC LIMIT %s OFFSET %s"
    params.extend([page_size, offset])

    rows, ms, perf = run_sql_ex(sql, tuple(params))
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size, total=total, perf=perf))

# Get single appearance
@app.get("/api/appearance/<appearance_id>")
def api_appearance_get(appearance_id):
    sql = """
      SELECT a.appearance_id, a.game_id, a.player_id, a.player_club_id, 
             a.player_current_club_id, a.date,
             a.yellow_cards, a.red_cards, a.goals, a.assists, a.minutes_played
      FROM appearance a
      WHERE a.appearance_id = %s
    """
    rows, ms = run_sql(sql, (appearance_id,))
    return jsonify(dict(ms=ms, row=rows[0] if rows else None))

# Create appearance
@app.post("/api/appearance")
def api_appearance_create():
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get("game_id"):
            return jsonify({"error": "Game ID is required"}), 400
        if not data.get("player_id"):
            return jsonify({"error": "Player ID is required"}), 400
        if not data.get("date"):
            return jsonify({"error": "Date is required"}), 400
        if not data.get("player_club_id"):
            return jsonify({"error": "Player Club ID is required"}), 400
        
        # Generate appearance_id as a composite of game_id and player_id
        game_id = int(data.get("game_id"))
        player_id = int(data.get("player_id"))
        appearance_id = f"{game_id}_{player_id}"
        
        sql_insert = """
            INSERT INTO appearance 
            (appearance_id, game_id, player_id, player_club_id, player_current_club_id, 
             date, yellow_cards, red_cards, goals, assists, minutes_played)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        run_sql(sql_insert, (
            appearance_id,
            game_id,
            player_id,
            int(data.get("player_club_id")),
            int(data.get("player_current_club_id")) if data.get("player_current_club_id") else None,
            data.get("date"),
            int(data.get("yellow_cards", 0)),
            int(data.get("red_cards", 0)),
            int(data.get("goals", 0)),
            int(data.get("assists", 0)),
            int(data.get("minutes_played")) if data.get("minutes_played") else None
        ))
        
        return jsonify({"appearance_id": appearance_id, "success": True}), 201
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Update appearance
@app.post("/api/appearance/<appearance_id>/update")
def api_appearance_update(appearance_id):
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get("game_id"):
            return jsonify({"error": "Game ID is required"}), 400
        if not data.get("player_id"):
            return jsonify({"error": "Player ID is required"}), 400
        if not data.get("date"):
            return jsonify({"error": "Date is required"}), 400
        if not data.get("player_club_id"):
            return jsonify({"error": "Player Club ID is required"}), 400
        
        sql_update = """
            UPDATE appearance 
            SET game_id=%s, player_id=%s, player_club_id=%s, player_current_club_id=%s,
                date=%s, yellow_cards=%s, red_cards=%s, goals=%s, assists=%s, minutes_played=%s
            WHERE appearance_id=%s
        """
        
        run_sql(sql_update, (
            int(data.get("game_id")),
            int(data.get("player_id")),
            int(data.get("player_club_id")),
            int(data.get("player_current_club_id")) if data.get("player_current_club_id") else None,
            data.get("date"),
            int(data.get("yellow_cards", 0)),
            int(data.get("red_cards", 0)),
            int(data.get("goals", 0)),
            int(data.get("assists", 0)),
            int(data.get("minutes_played")) if data.get("minutes_played") else None,
            appearance_id
        ))
        
        return jsonify({"appearance_id": appearance_id, "success": True}), 200
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Delete appearance
@app.delete("/api/appearance/<appearance_id>")
def api_appearance_delete(appearance_id):
    try:
        sql_delete = "DELETE FROM appearance WHERE appearance_id=%s"
        run_sql(sql_delete, (appearance_id,))
        
        return jsonify({"success": True, "message": f"Appearance {appearance_id} deleted"}), 200
    
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Transfers page
@app.route("/transfers")
def transfers_page():
    return render_template("transfer_table.html")

@app.route("/transfer/create")
def transfer_create_page():
    return render_template("create_transfer.html")

@app.route("/transfer/edit")
def transfer_edit_page():
    return render_template("edit_transfer.html")

# Get transfers list with pagination and search
@app.get("/api/transfers/list")
def api_transfers_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    search = request.args.get("search", "").strip()
    offset = (page - 1) * page_size

    sql = """
      SELECT t.transfer_id, t.player_id, t.transfer_date, t.transfer_season,
             t.from_club_id, t.to_club_id, t.transfer_fee, t.market_value_in_eur,
             p.name AS player_name, 
             fc.name AS from_club_name, 
             tc.name AS to_club_name
      FROM transfer t
      JOIN player p ON p.player_id = t.player_id
      LEFT JOIN club fc ON fc.club_id = t.from_club_id
      JOIN club tc ON tc.club_id = t.to_club_id
      WHERE 1=1
    """
    params = []

    if search:
        sql += " AND (p.name LIKE %s OR fc.name LIKE %s OR tc.name LIKE %s)"
        search_param = f"%{search}%"
        params.extend([search_param, search_param, search_param])

    count_sql = "SELECT COUNT(*) as total FROM transfer t JOIN player p ON p.player_id = t.player_id LEFT JOIN club fc ON fc.club_id = t.from_club_id JOIN club tc ON tc.club_id = t.to_club_id WHERE 1=1"
    if search:
        count_sql += " AND (p.name LIKE %s OR fc.name LIKE %s OR tc.name LIKE %s)"

    count_rows, _ = run_sql(count_sql, tuple(params))
    total = count_rows[0]["total"] if count_rows else 0

    sql += " ORDER BY t.transfer_date DESC LIMIT %s OFFSET %s"
    params.extend([page_size, offset])

    rows, ms, perf = run_sql_ex(sql, tuple(params))
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size, total=total, perf=perf))

# Get single transfer
@app.get("/api/transfer/<int:transfer_id>")
def api_transfer_get(transfer_id):
    sql = """
      SELECT t.transfer_id, t.player_id, t.transfer_date, t.transfer_season,
             t.from_club_id, t.to_club_id, t.transfer_fee, t.market_value_in_eur
      FROM transfer t
      WHERE t.transfer_id = %s
    """
    rows, ms = run_sql(sql, (transfer_id,))
    return jsonify(dict(ms=ms, row=rows[0] if rows else None))

# Create transfer
@app.post("/api/transfer")
def api_transfer_create():
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get("player_id"):
            return jsonify({"error": "Player ID is required"}), 400
        if not data.get("transfer_date"):
            return jsonify({"error": "Transfer date is required"}), 400
        if not data.get("to_club_id"):
            return jsonify({"error": "To club ID is required"}), 400
        
        player_id = int(data.get("player_id"))
        transfer_date = data.get("transfer_date")
        
        # Check for existing transfer for this player (within last 30 days or future)
        sql_check = """
          SELECT t.transfer_id, t.transfer_date, p.name AS player_name, tc.name AS to_club_name
          FROM transfer t
          JOIN player p ON p.player_id = t.player_id
          JOIN club tc ON tc.club_id = t.to_club_id
          WHERE t.player_id = %s AND t.transfer_date >= DATE_SUB(%s, INTERVAL 30 DAY)
          ORDER BY t.transfer_date DESC
          LIMIT 1
        """
        
        existing_transfers, _ = run_sql(sql_check, (player_id, transfer_date))
        
        if existing_transfers:
            existing = existing_transfers[0]
            return jsonify({
                "error": f"Player '{existing['player_name']}' already has a pending transfer to '{existing['to_club_name']}' on {existing['transfer_date']}. Cannot create multiple transfers for the same player within 30 days."
            }), 400
        
        # Get the next transfer_id (max + 1)
        sql_max_id = "SELECT COALESCE(MAX(transfer_id), 0) + 1 AS next_id FROM transfer"
        rows, _ = run_sql(sql_max_id)
        transfer_id = rows[0]["next_id"] if rows else 1
        
        sql_insert = """
            INSERT INTO transfer 
            (transfer_id, player_id, transfer_date, transfer_season, from_club_id, to_club_id, transfer_fee, market_value_in_eur)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        run_sql(sql_insert, (
            transfer_id,
            player_id,
            transfer_date,
            data.get("transfer_season") or None,
            int(data.get("from_club_id")) if data.get("from_club_id") else None,
            int(data.get("to_club_id")),
            int(data.get("transfer_fee")) if data.get("transfer_fee") else None,
            int(data.get("market_value_in_eur")) if data.get("market_value_in_eur") else None
        ))
        
        return jsonify({"transfer_id": transfer_id, "success": True}), 201
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Update transfer
@app.post("/api/transfer/<int:transfer_id>/update")
def api_transfer_update(transfer_id):
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get("player_id"):
            return jsonify({"error": "Player ID is required"}), 400
        if not data.get("transfer_date"):
            return jsonify({"error": "Transfer date is required"}), 400
        if not data.get("to_club_id"):
            return jsonify({"error": "To club ID is required"}), 400
        
        sql_update = """
            UPDATE transfer 
            SET player_id=%s, transfer_date=%s, transfer_season=%s, from_club_id=%s, to_club_id=%s, transfer_fee=%s, market_value_in_eur=%s
            WHERE transfer_id=%s
        """
        
        run_sql(sql_update, (
            int(data.get("player_id")),
            data.get("transfer_date"),
            data.get("transfer_season") or None,
            int(data.get("from_club_id")) if data.get("from_club_id") else None,
            int(data.get("to_club_id")),
            int(data.get("transfer_fee")) if data.get("transfer_fee") else None,
            int(data.get("market_value_in_eur")) if data.get("market_value_in_eur") else None,
            transfer_id
        ))
        
        return jsonify({"transfer_id": transfer_id, "success": True}), 200
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Delete transfer
@app.delete("/api/transfer/<int:transfer_id>")
def api_transfer_delete(transfer_id):
    try:
        sql_delete = "DELETE FROM transfer WHERE transfer_id=%s"
        run_sql(sql_delete, (transfer_id,))
        
        return jsonify({"success": True, "message": f"Transfer {transfer_id} deleted"}), 200
    
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Game Events routes
@app.route("/game-events")
def game_events_page():
    return render_template("game_events_table.html")

@app.route("/game-event/create")
def game_event_create_page():
    return render_template("create_game_event.html")

@app.route("/game-event/edit")
def game_event_edit_page():
    return render_template("edit_game_event.html")

# Get game events list with pagination and search
@app.get("/api/game-events/list")
def api_game_events_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    game_id = request.args.get("game_id", "").strip()
    event_type = request.args.get("type", "").strip()
    offset = (page - 1) * page_size

    sql = """
      SELECT ge.game_event_id, ge.game_id, COALESCE(ge.minute, -1) as minute, ge.type, ge.club_id,
             ge.player_id, ge.description, ge.player_in_id, ge.player_assist_id,
             c.name AS club_name,
             p.name AS player_name,
             p_in.name AS player_in_name,
             p_ast.name AS assist_name
      FROM game_events ge
      LEFT JOIN club c ON c.club_id = ge.club_id
      LEFT JOIN player p ON p.player_id = ge.player_id
      LEFT JOIN player p_in ON p_in.player_id = ge.player_in_id
      LEFT JOIN player p_ast ON p_ast.player_id = ge.player_assist_id
      WHERE 1=1
    """
    params = []

    if game_id:
        sql += " AND ge.game_id = %s"
        params.append(int(game_id))
    if event_type:
        sql += " AND ge.type = %s"
        params.append(event_type)

    count_sql = "SELECT COUNT(*) as total FROM game_events ge WHERE 1=1"
    if game_id:
        count_sql += " AND ge.game_id = %s"
    if event_type:
        count_sql += " AND ge.type = %s"

    count_rows, _ = run_sql(count_sql, tuple(params))
    total = count_rows[0]["total"] if count_rows else 0

    sql += " ORDER BY COALESCE(ge.minute, 999) ASC, ge.game_event_id ASC LIMIT %s OFFSET %s"
    params.extend([page_size, offset])

    rows, ms, perf = run_sql_ex(sql, tuple(params))
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size, total=total, perf=perf))

# Get single game event
@app.get("/api/game-event/<event_id>")
def api_game_event_get(event_id):
    sql = """
      SELECT ge.game_event_id, ge.game_id, ge.minute, ge.type, ge.club_id,
             ge.player_id, ge.description, ge.player_in_id, ge.player_assist_id
      FROM game_events ge
      WHERE ge.game_event_id = %s
    """
    rows, ms = run_sql(sql, (event_id,))
    return jsonify(dict(ms=ms, row=rows[0] if rows else None))

# Create game event
@app.post("/api/game-event")
def api_game_event_create():
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get("game_id"):
            return jsonify({"error": "Game ID is required"}), 400
        if not data.get("type"):
            return jsonify({"error": "Event type is required"}), 400
        
        # Generate event_id (use game_id + minute + random suffix)
        import uuid
        game_id = int(data.get("game_id"))
        minute = int(data.get("minute", 0))
        event_id = f"{game_id}_{minute}_{str(uuid.uuid4())[:8]}"
        
        sql_insert = """
            INSERT INTO game_events 
            (game_event_id, game_id, minute, type, club_id, player_id, description, player_in_id, player_assist_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        run_sql(sql_insert, (
            event_id,
            game_id,
            int(data.get("minute")) if data.get("minute") else None,
            data.get("type"),
            int(data.get("club_id")) if data.get("club_id") else None,
            int(data.get("player_id")) if data.get("player_id") else None,
            data.get("description") or None,
            int(data.get("player_in_id")) if data.get("player_in_id") else None,
            int(data.get("player_assist_id")) if data.get("player_assist_id") else None
        ))
        
        return jsonify({"game_event_id": event_id, "success": True}), 201
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Update game event
@app.post("/api/game-event/<event_id>/update")
def api_game_event_update(event_id):
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get("game_id"):
            return jsonify({"error": "Game ID is required"}), 400
        if not data.get("type"):
            return jsonify({"error": "Event type is required"}), 400
        
        sql_update = """
            UPDATE game_events 
            SET game_id=%s, minute=%s, type=%s, club_id=%s, player_id=%s, description=%s, player_in_id=%s, player_assist_id=%s
            WHERE game_event_id=%s
        """
        
        run_sql(sql_update, (
            int(data.get("game_id")),
            int(data.get("minute")) if data.get("minute") else None,
            data.get("type"),
            int(data.get("club_id")) if data.get("club_id") else None,
            int(data.get("player_id")) if data.get("player_id") else None,
            data.get("description") or None,
            int(data.get("player_in_id")) if data.get("player_in_id") else None,
            int(data.get("player_assist_id")) if data.get("player_assist_id") else None,
            event_id
        ))
        
        return jsonify({"game_event_id": event_id, "success": True}), 200
        
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

# Delete game event
@app.delete("/api/game-event/<event_id>")
def api_game_event_delete(event_id):
    try:
        sql_delete = "DELETE FROM game_events WHERE game_event_id=%s"
        run_sql(sql_delete, (event_id,))
        
        return jsonify({"success": True, "message": f"Event {event_id} deleted"}), 200
    
    except Exception as err:
        return jsonify({"error": str(err), "details": repr(err)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)
