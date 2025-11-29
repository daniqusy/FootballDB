import os, time
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_from_directory
from dotenv import load_dotenv
import pymysql
from pymongo import MongoClient
from pymongo import ReturnDocument

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
        rows_examined, rows_sent, explain_type, explain,
        execution_ms, diagnostics_ms, total_ms.
    (Query cache stats removed for simplicity.)
    """
    perf = {
        "rows_examined": None,
        "rows_sent": None,
        "explain_type": None,
        "explain": None,
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

            perf["diagnostics_ms"] = round((time.perf_counter() - t_diag_start) * 1000.0, 2)
        perf["total_ms"] = round((time.perf_counter() - t_total_start) * 1000.0, 2)
        # Fallback: if rows_examined is 0 or None, try to estimate from EXPLAIN JSON
        if not perf.get("rows_examined"):
            try:
                import json
                exp = perf.get("explain")
                if exp and perf.get("explain_type") == "json":
                    ej = json.loads(exp) if isinstance(exp, str) else (exp if isinstance(exp, dict) else None)
                    def sum_rows(node):
                        if node is None:
                            return 0
                        total = 0
                        if isinstance(node, dict):
                            for k in ("rows", "rows_examined_per_scan", "rows_produced_per_join"):
                                v = node.get(k)
                                if isinstance(v, (int, float)):
                                    total += int(v)
                            for v in node.values():
                                total += sum_rows(v)
                        elif isinstance(node, list):
                            for v in node:
                                total += sum_rows(v)
                        return total
                    est = sum_rows(ej)
                    if isinstance(est, (int, float)) and est > 0:
                        perf["rows_examined"] = int(est)
            except Exception:
                pass
        # Produce a display version of the SQL with parameters bound (for UI only)
        try:
            def _fmt_param(v):
                if v is None:
                    return 'NULL'
                if isinstance(v, (int, float)):
                    return str(v)
                s = str(v).replace("'", "''")
                return f"'{s}'"
            parts = sql.split('%s')
            bound_fragments = []
            for i, part in enumerate(parts):
                bound_fragments.append(part)
                if i < len(params):
                    bound_fragments.append(_fmt_param(params[i]))
            perf["query"] = ''.join(bound_fragments)
        except Exception:
            perf["query"] = sql
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

# Helper: extract execution stats totals from Mongo explain output (executionStats verbosity)
def mongo_exec_stats_totals(explain_obj):
    try:
        es = explain_obj.get("executionStats") or {}
        totals = {
            "totalDocsExamined": es.get("totalDocsExamined"),
            "totalKeysExamined": es.get("totalKeysExamined"),
            "nReturned": es.get("nReturned"),
            "executionTimeMillis": es.get("executionTimeMillis"),
        }
        # For aggregation explains, these might live under inputStage or children; attempt fallback drill
        def drill(node, acc):
            if not isinstance(node, dict):
                return
            for k in ("totalDocsExamined", "totalKeysExamined", "nReturned", "executionTimeMillis"):
                v = node.get(k)
                if acc.get(k) is None and isinstance(v, (int, float)):
                    acc[k] = v
            for child_k in ("inputStage", "outerStage", "innerStage", "stage", "winningPlan"):
                if child_k in node:
                    drill(node.get(child_k), acc)
            # Explore children arrays
            for v in node.values():
                if isinstance(v, list):
                    for it in v:
                        drill(it, acc)
                elif isinstance(v, dict):
                    drill(v, acc)
        drill(explain_obj, totals)
        return {k: v for k, v in totals.items() if v is not None}
    except Exception:
        return {}

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
    rows, ms, perf = run_sql_ex(sql, (player_id, comp, season, n))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Mongo: Player Form over N
@app.get("/api/mongo/player/form")
def api_mongo_player_form():
    try:
        player_id = int(request.args.get("player_id"))
        comp = request.args.get("competition_id")
        season = request.args.get("season")
        n = int(request.args.get("n", 5))
    except Exception:
        return jsonify(dict(ms=0, rows=[], source="mongo", error="invalid-params")), 400

    want_perf = (request.args.get("perf") == "1")

    def _q(db):
        # Fetch games for this competition/season first
        gcur = db.games.find({"competition_id": comp, "season": season}, {
            "_id": 1, "date": 1,
            "home.club_id": 1, "home.name": 1,
            "away.club_id": 1, "away.name": 1,
        })
        game_map = {}
        game_ids = []
        for g in gcur:
            gid = g.get("_id")
            if gid is None:
                continue
            game_ids.append(gid)
            h = g.get("home") or {}
            a = g.get("away") or {}
            game_map[gid] = {
                "date_str": g.get("date"),
                "home_club_id": h.get("club_id"),
                "home_name": h.get("name"),
                "away_club_id": a.get("club_id"),
                "away_name": a.get("name"),
            }
        if not game_ids:
            return []
        # Pull appearances for this player restricted to those games
        acur = db.appearances.find({
            "player_id": int(player_id),
            "game_id": {"$in": game_ids}
        }, {
            "game_id": 1,
            "player_club_id": 1,
            "minutes_played": 1,
            "goals": 1,
            "assists": 1,
            "date": 1,
        })
        out = []
        for a in acur:
            gid = a.get("game_id")
            g = game_map.get(gid) or {}
            out.append({
                "game_id": gid,
                "date_str": g.get("date_str") or a.get("date"),
                "minutes_played": a.get("minutes_played"),
                "goals": a.get("goals"),
                "assists": a.get("assists"),
                "player_club_id": a.get("player_club_id"),
                "home_club_id": g.get("home_club_id"),
                "home_name": g.get("home_name"),
                "away_club_id": g.get("away_club_id"),
                "away_name": g.get("away_name"),
            })
        # Sort by date desc (YYYY-MM-DD lexical works) and limit n
        out.sort(key=lambda r: (r.get("date_str") or ""), reverse=True)
        return out[:n]

    rows, exec_ms = run_mongo(_q)
    # Perf details
    import json
    perf = {
        "query": json.dumps({
            "games.find": {"competition_id": comp, "season": season},
            "appearances.find": {"player_id": player_id, "game_id": "IN(<games>)"},
            "limit": n
        }, ensure_ascii=False, indent=2),
        "stats": {"docs_returned": len(rows)}
    }
    if want_perf:
        try:
            # Explain appearances find with $in on game_ids is complex to build without re-running game fetch;
            # Run a simplified explain on player_id filter as pragmatic signal.
            exp = mongo_db.command({
                "explain": {
                    "find": "appearances",
                    "filter": {"player_id": int(player_id)}
                },
                "verbosity": "executionStats"
            })
            perf["explain"] = exp
            perf["stats"].update(mongo_exec_stats_totals(exp))
        except Exception:
            perf["explain"] = None
    else:
        perf["explain"] = None

    return jsonify(dict(ms=exec_ms, rows=rows, perf=perf, source="mongo"))

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
            WITH per AS (
                SELECT a.player_id, a.player_club_id, SUM(a.goals) AS goals
                FROM appearance a
                JOIN game g ON g.game_id = a.game_id
                WHERE g.competition_id = %s AND g.season = %s
                GROUP BY a.player_id, a.player_club_id
            ), best AS (
          SELECT player_id, player_club_id, goals,
              ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY goals DESC) AS rn
                FROM per
            )
         SELECT b.player_id, p.name AS player_name, pb.image_url,
             b.goals, c.name AS club_name
            FROM best b
            JOIN player p ON p.player_id = b.player_id
            JOIN player_bio pb ON pb.player_id = b.player_id
            LEFT JOIN club c ON c.club_id = b.player_club_id
            WHERE b.rn = 1 AND b.goals > 0
            ORDER BY b.goals DESC
            LIMIT %s OFFSET %s
        """
    rows, ms, perf = run_sql_ex(sql, (comp, season, page_size, offset))
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size, perf=perf))

# Mongo: Top scorers by league-season with pagination (from player_seasons)
@app.get("/api/mongo/top-scorers")
def api_mongo_top_scorers():
    comp = request.args.get("competition_id")
    season = request.args.get("season")
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    skip = (page - 1) * page_size

    def _q(db):
        query = {"competition_id": comp, "season": season}
        # Count total distinct players in player_seasons for this comp+season
        total = db.player_seasons.count_documents({**query, "totals.goals": {"$gt": 0}})
        cur = db.player_seasons.find({**query, "totals.goals": {"$gt": 0}}, {
            "player_id": 1, "totals.goals": 1
        }).sort([["totals.goals", -1]]).skip(skip).limit(page_size)
        rows = []
        for ps in cur:
            pid = ps.get("player_id")
            goals = ((ps.get("totals") or {}).get("goals") or 0)
            pl = db.players.find_one({"player_id": pid}, {"name":1, "image_url":1, "current_club_name":1}) or {}
            rows.append({
                "player_id": pid,
                "player_name": pl.get("name"),
                "image_url": pl.get("image_url"),
                "club_name": pl.get("current_club_name"),
                "goals": goals
            })
        return rows, total
    (rows, total), ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size, total=total, source="mongo"))

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
    rows, ms, perf = run_sql_ex(sql, (game_id,))
    return jsonify(dict(ms=ms, row=rows[0] if rows else None, perf=perf))

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
        
        sql_update = """
            UPDATE game 
            SET date=%s, match_time=%s, competition_id=%s, season=%s, round=%s,
                home_club_id=%s, away_club_id=%s,
                home_club_goals=%s, away_club_goals=%s,
                home_club_position=%s, away_club_position=%s,
                stadium=%s, referee=%s, attendance=%s,
                home_club_manager_name=%s, away_club_manager_name=%s,
                home_club_formation=%s, away_club_formation=%s
            WHERE game_id=%s
        """

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
    game_rows, ms_game, perf_game = run_sql_ex(sql_game, (gid,))
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
    event_rows, ms_events, perf_events = run_sql_ex(sql_ev, (gid,))
    total_ms = round((ms_game or 0) + (ms_events or 0), 2)
    perf = {"game": perf_game, "events": perf_events}
    return jsonify(dict(game=(game_rows[0] if game_rows else None), events=event_rows, ms=total_ms, ms_parts=dict(game=ms_game, events=ms_events), perf=perf, source="sql"))

# Mongo: Match details
@app.get("/api/mongo/match")
def api_mongo_match():
    gid = int(request.args.get("game_id"))
    want_perf = (request.args.get("perf") == "1")
    def _q(db):
        doc = db.games.find_one({"_id": gid})
        return doc
    doc, ms = run_mongo(_q)
    if not doc:
        return jsonify(dict(ms=ms, game=None, events=[] , source="mongo"))
    # Transform game document to SQL-like shape
    game = {
        "game_id": doc.get("_id"),
        "competition_id": doc.get("competition_id"),
        "competition_name": doc.get("competition_name"),
        "season": doc.get("season"),
        "round": doc.get("round"),
        "date_str": doc.get("date"),
        "home_club_id": (doc.get("home") or {}).get("club_id"),
        "home_name": (doc.get("home") or {}).get("name"),
        "home_club_goals": (doc.get("home") or {}).get("goals"),
        "away_club_id": (doc.get("away") or {}).get("club_id"),
        "away_name": (doc.get("away") or {}).get("name"),
        "away_club_goals": (doc.get("away") or {}).get("goals"),
        "home_club_position": (doc.get("home") or {}).get("position"),
        "away_club_position": (doc.get("away") or {}).get("position"),
        "stadium": doc.get("stadium"),
        "referee": doc.get("referee"),
        "attendance": doc.get("attendance"),
        "home_club_formation": (doc.get("home") or {}).get("formation"),
        "away_club_formation": (doc.get("away") or {}).get("formation"),
        "match_time": doc.get("match_time"),
        "home_club_manager_name": (doc.get("home") or {}).get("manager_name"),
        "away_club_manager_name": (doc.get("away") or {}).get("manager_name"),
    }
    # Transform events
    ev_rows = []
    for ev in doc.get("events", []) or []:
        club_id = ev.get("club_id")
        side = "home" if club_id == game.get("home_club_id") else "away"
        ev_rows.append({
            "game_event_id": None,  # Not stored in Mongo doc
            "game_id": game.get("game_id"),
            "minute": ev.get("minute"),
            "event_type": ev.get("type"),
            "club_id": club_id,
            "side": side,
            "player_id": ev.get("player_id"),
            "player_name": ev.get("player_name"),
            "player_assist_id": ev.get("assist_id"),
            "assist_name": ev.get("assist_name"),
            "player_in_id": ev.get("sub_in_id"),
            "player_in_name": ev.get("player_in_name"),
            "description": ev.get("event_desc"),
        })
    # Perf object (basic, explain only if requested)
    import json
    perf = {
        "query": json.dumps({"find": "games", "filter": {"_id": gid}}, ensure_ascii=False, indent=2),
        "stats": {"events_count": len(ev_rows)}
    }
    if want_perf:
        try:
            exp = mongo_db.command({
                "explain": {"find": "games", "filter": {"_id": gid}, "limit": 1},
                "verbosity": "executionStats"
            })
            perf["explain"] = exp
            stats_extra = mongo_exec_stats_totals(exp)
            perf["stats"].update(stats_extra)
        except Exception:
            perf["explain"] = None
    else:
        perf["explain"] = None
    return jsonify(dict(ms=ms, game=game, events=ev_rows, perf=perf, source="mongo"))

# Club page
@app.route("/club")
def club_page():
    return render_template("club.html")

# Club profile
@app.get("/api/club/<int:cid>/profile")
def api_club_profile(cid):
        # Compute totals for all clubs first (subquery), then filter outside so window functions see full set
        sql = """
            SELECT * FROM (
                SELECT c.club_id,
                             c.name,
                             c.average_age,
                             c.stadium_name,
                             c.stadium_seats,
                             COALESCE(SUM(p.market_value_eur), 0) AS total_market_value_eur,
                             DENSE_RANK() OVER (ORDER BY COALESCE(SUM(p.market_value_eur),0) DESC) AS market_value_rank,
                             COUNT(*) OVER () AS clubs_ranked
                FROM club c
                LEFT JOIN player p
                    ON p.current_club_id = c.club_id
                 AND p.market_value_eur IS NOT NULL
                GROUP BY c.club_id, c.name, c.average_age, c.stadium_name, c.stadium_seats
            ) t
            WHERE t.club_id=%s
        """
        rows, ms, perf = run_sql_ex(sql, (cid,))
        return jsonify(dict(ms=ms, row=(rows[0] if rows else None), perf=perf))

# Mongo: Club profile
@app.get("/api/mongo/club/<int:cid>/profile")
def api_mongo_club_profile(cid):
    want_perf = (request.args.get("perf") == "1")
    def _q(db):
        doc = db.clubs.find_one({"club_id": int(cid)}, {
            "club_id": 1, "name": 1, "average_age": 1,
            "stadium_name": 1, "stadium_seats": 1,
            "total_market_value_eur": 1, "squad_size": 1,
            "player_count": 1
        })
        if not doc:
            return None
        mv = doc.get("total_market_value_eur")
        total = db.clubs.count_documents({}) or 0
        rank = None
        if mv is not None:
            # Count clubs with strictly greater market value to derive rank
            rank = (db.clubs.count_documents({"total_market_value_eur": {"$gt": mv}}) or 0) + 1
        doc.pop("_id", None)
        doc["market_value_rank"] = rank
        doc["clubs_ranked"] = total
        return doc
    row, ms = run_mongo(_q)
    # Perf object
    import json
    perf = {
        "query": json.dumps({"find": "clubs", "filter": {"club_id": int(cid)}}, ensure_ascii=False, indent=2),
        "stats": {"has_row": bool(row), "club_id": cid}
    }
    if want_perf:
        try:
            exp = mongo_db.command({
                "explain": {"find": "clubs", "filter": {"club_id": int(cid)}, "limit": 1},
                "verbosity": "executionStats"
            })
            perf["explain"] = exp
            perf["stats"].update(mongo_exec_stats_totals(exp))
        except Exception:
            perf["explain"] = None
    else:
        perf["explain"] = None
    return jsonify(dict(ms=ms, row=row, perf=perf, source="mongo"))

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
    rows, ms, perf = run_sql_ex(sql, (cid,))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Mongo: Club players
@app.get("/api/mongo/club/<int:cid>/players")
def api_mongo_club_players(cid):
    want_perf = (request.args.get("perf") == "1")
    def _q(db):
        # Include players even if market_value_eur is null for parity with SQL endpoint
        cur = db.players.find({"current_club_id": int(cid)}, {
            "player_id": 1, "name": 1, "position": 1, "sub_position": 1,
            "market_value_eur": 1
        }).sort("market_value_eur", -1).limit(200)
        out = []
        for d in cur:
            d.pop("_id", None)
            out.append(d)
        return out
    rows, ms = run_mongo(_q)
    import json
    perf = {
        "query": json.dumps({"find": "players", "filter": {"current_club_id": int(cid)}, "sort": {"market_value_eur": -1}, "limit": 200}, ensure_ascii=False, indent=2),
        "stats": {"docs_returned": len(rows)}
    }
    if want_perf:
        try:
            exp = mongo_db.command({
                "explain": {
                    "find": "players",
                    "filter": {"current_club_id": int(cid)},
                    "sort": {"market_value_eur": -1},
                    "limit": 200
                },
                "verbosity": "executionStats"
            })
            perf["explain"] = exp
            perf["stats"].update(mongo_exec_stats_totals(exp))
        except Exception:
            perf["explain"] = None
    else:
        perf["explain"] = None
    return jsonify(dict(ms=ms, rows=rows, perf=perf, source="mongo"))

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
    rows, ms, perf = run_sql_ex(base, tuple(params))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Mongo: Club recent matches
@app.get("/api/mongo/club/<int:cid>/matches")
def api_mongo_club_matches(cid):
    limit_n = min(max(int(request.args.get("limit", 12)), 1), 100)
    comp = request.args.get("competition_id")
    want_perf = (request.args.get("perf") == "1")
    def _q(db):
        q = {"$or": [{"home.club_id": int(cid)}, {"away.club_id": int(cid)}]}
        if comp:
            q["competition_id"] = comp
        cur = db.games.find(q, {
            "_id": 1, "date": 1,
            "home.club_id": 1, "home.name": 1, "home.goals": 1,
            "away.club_id": 1, "away.name": 1, "away.goals": 1,
            "competition_id": 1, "competition_name": 1
        }).sort("date", -1).limit(limit_n)
        out = []
        for g in cur:
            home = g.get("home") or {}
            away = g.get("away") or {}
            out.append({
                "game_id": g.get("_id"),
                "date_str": g.get("date"),
                "home_club_id": home.get("club_id"),
                "home_name": home.get("name"),
                "away_club_id": away.get("club_id"),
                "away_name": away.get("name"),
                "home_club_goals": home.get("goals"),
                "away_club_goals": away.get("goals"),
                "competition_id": g.get("competition_id"),
                "league_name": g.get("competition_name")
            })
        return out
    rows, ms = run_mongo(_q)
    import json
    perf = {
        "query": json.dumps({"find": "games", "filter": ({"$or": [{"home.club_id": int(cid)}, {"away.club_id": int(cid)}], **({"competition_id": comp} if comp else {})}), "sort": {"date": -1}, "limit": limit_n}, ensure_ascii=False, indent=2),
        "stats": {"docs_returned": len(rows)}
    }
    if want_perf:
        try:
            exp = mongo_db.command({
                "explain": {
                    "find": "games",
                    "filter": ({"$or": [{"home.club_id": int(cid)}, {"away.club_id": int(cid)}], **({"competition_id": comp} if comp else {})}),
                    "sort": {"date": -1},
                    "limit": limit_n
                },
                "verbosity": "executionStats"
            })
            perf["explain"] = exp
            perf["stats"].update(mongo_exec_stats_totals(exp))
        except Exception:
            perf["explain"] = None
    else:
        perf["explain"] = None
    return jsonify(dict(ms=ms, rows=rows, perf=perf, source="mongo"))

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
    rows, ms, perf = run_sql_ex(sql, (cid, cid))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Mongo: Club competitions
@app.get("/api/mongo/club/<int:cid>/competitions")
def api_mongo_club_competitions(cid):
    want_perf = (request.args.get("perf") == "1")
    def _q(db):
        cur = db.games.find({"$or": [{"home.club_id": int(cid)}, {"away.club_id": int(cid)}]}, {
            "competition_id": 1, "competition_name": 1
        })
        comps = {}
        # Fetch club document to identify its domestic league
        club_doc = db.clubs.find_one({"club_id": int(cid)}, {"domestic_competition_id":1}) or {}
        domestic_comp = club_doc.get("domestic_competition_id")
        for g in cur:
            cidv = g.get("competition_id")
            if cidv is None: continue
            if cidv not in comps:
                comps[cidv] = {
                    "competition_id": cidv,
                    "competition_name": g.get("competition_name"),
                    # Mark the club's domestic league for front-end preference
                    "type": "domestic-league" if domestic_comp and cidv == domestic_comp else None
                }
        out = list(comps.values())
        out.sort(key=lambda r: (r.get("competition_name") or str(r.get("competition_id"))))
        return out
    rows, ms = run_mongo(_q)
    import json
    perf = {
        "query": json.dumps({"distinct_competitions_for_club": cid}, ensure_ascii=False, indent=2),
        "stats": {"competitions": len(rows)}
    }
    if want_perf:
        try:
            exp = mongo_db.command({
                "explain": {
                    "aggregate": "games",
                    "pipeline": [
                        {"$match": {"$or": [{"home.club_id": int(cid)}, {"away.club_id": int(cid)}]}},
                        {"$group": {"_id": "$competition_id", "competition_name": {"$first": "$competition_name"}}}
                    ],
                    "cursor": {}
                },
                "verbosity": "executionStats"
            })
            perf["explain"] = exp
            perf["stats"].update(mongo_exec_stats_totals(exp))
        except Exception:
            perf["explain"] = None
    else:
        perf["explain"] = None
    return jsonify(dict(ms=ms, rows=rows, perf=perf, source="mongo"))

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
        rows, ms, perf = run_sql_ex(sql, (comp, comp, limit_n))
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
        rows, ms, perf = run_sql_ex(sql, (limit_n,))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Mongo: Clubs total market value ranking
@app.get("/api/mongo/clubs/market-ranking")
def api_mongo_clubs_market_ranking():
    limit_n = min(max(int(request.args.get("limit", 100)), 1), 500)
    comp = request.args.get("competition_id")
    def _q(db):
        # If a competition_id (domestic league) is provided, filter by a club's domestic_competition_id
        if comp:
            filt = {"domestic_competition_id": comp}
        else:
            filt = {"total_market_value_eur": {"$ne": None}}
        cur = db.clubs.find(filt, {"club_id":1, "name":1, "total_market_value_eur":1}).sort("total_market_value_eur", -1).limit(limit_n)
        rows = []
        rank = 1
        for d in cur:
            rows.append({
                "club_id": d.get("club_id"),
                "name": d.get("name"),
                "total_market_value_eur": d.get("total_market_value_eur"),
                "market_value_rank": rank
            })
            rank += 1
        return rows
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo"))

# Club transfer ROI view
@app.route("/club/roi")
def club_roi_page():
    return render_template("club_roi.html")

# Clubs list
@app.get("/api/clubs")
def api_clubs():
    sql = "SELECT club_id, name FROM club ORDER BY name"
    rows, ms, perf = run_sql_ex(sql)
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Seasons a club bought players
@app.get("/api/clubs/<int:club_id>/seasons")
def api_club_seasons(club_id):
    sql = """
      SELECT DISTINCT transfer_season AS season
      FROM transfer
      WHERE to_club_id=%s
      ORDER BY season DESC
    """
    rows, ms, perf = run_sql_ex(sql, (club_id,))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Mongo: Seasons a club bought players
@app.get("/api/mongo/clubs/<int:club_id>/seasons")
def api_mongo_club_seasons(club_id):
    def _q(db):
        rows = db.transfers.distinct("transfer_season", {"to.club_id": club_id})
        rows = [r for r in rows if r]
        try:
            rows.sort(reverse=True)
        except Exception:
            pass
        return [{"season": r} for r in rows]
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo"))

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
    rows, ms, perf = run_sql_ex(sql, (f"%{q}%",))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

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
    rows, ms, perf = run_sql_ex(sql, (club_id, season))
    if rows:
        ids = tuple({r["player_id"] for r in rows})
        in_clause = ",".join(["%s"]*len(ids))
        psql = f"SELECT player_id, name FROM player WHERE player_id IN ({in_clause})"
        plist, _, perf_names = run_sql_ex(psql, ids)
        name_map = {p["player_id"]: p["name"] for p in plist}
        for r in rows: r["player_name"] = name_map.get(r["player_id"])
    return jsonify(dict(ms=ms, rows=rows, perf=perf, perf_names=perf_names))

# Mongo: Club transfer ROI data (basic from transfers collection)
@app.get("/api/mongo/club/roi")
def api_mongo_club_roi():
    club_id = int(request.args.get("club_id"))
    season  = request.args.get("season")
    sort_by = request.args.get("sort_by","post_minutes")
    order   = -1 if request.args.get("order","desc").lower()=="desc" else 1
    debug = request.args.get("debug") == "1"

    def _q(db):
        flt = {"to.club_id": club_id}
        if season:
            flt["transfer_season"] = season
        transfers = list(db.transfers.find(flt, {
            "_id": 1,
            "player_id": 1,
            "player_name": 1,
            "transfer_fee": 1,
            "market_value_in_eur": 1,
            "transfer_season": 1,
            "transfer_date": 1,
        }))
        transfers = [t for t in transfers if (t.get("transfer_fee") or 0) > 0]
        if not transfers:
            return []

        player_ids = [t.get("player_id") for t in transfers if t.get("player_id") is not None]
        apps_by_player = {}
        game_ids = set()
        if player_ids:
            ap_cur = db.appearances.find({
                "player_id": {"$in": player_ids},
                "$or": [
                    {"player_club_id": club_id},
                    {"player_current_club_id": club_id}
                ]
            }, {
                "game_id": 1,
                "player_id": 1,
                "minutes_played": 1,
                "goals": 1,
                "assists": 1,
                "date": 1,
            })
            for ap in ap_cur:
                pid = ap.get("player_id")
                if pid is None:
                    continue
                apps_by_player.setdefault(pid, []).append(ap)
                gid = ap.get("game_id")
                if gid is not None:
                    game_ids.add(gid)
        game_seasons = {}
        if game_ids:
            for gdoc in db.games.find({"_id": {"$in": list(game_ids)}}, {"_id":1, "season":1}):
                game_seasons[gdoc.get("_id")] = gdoc.get("season")

        from datetime import datetime
        def parse_date(s):
            try:
                return datetime.strptime(s, "%Y-%m-%d").date()
            except Exception:
                return None

        rows = []
        for t in transfers:
            pid = t.get("player_id")
            fee = t.get("transfer_fee") or 0
            tdate = parse_date(t.get("transfer_date"))
            aps = apps_by_player.get(pid, [])
            mins = goals = assists = 0
            # Post-transfer pass
            for ap in aps:
                gid = ap.get("game_id")
                if season and game_seasons.get(gid) != season:
                    continue
                ap_date = parse_date(ap.get("date"))
                if tdate and ap_date and ap_date < tdate:
                    continue
                mins += int(ap.get("minutes_played") or 0)
                goals += int(ap.get("goals") or 0)
                assists += int(ap.get("assists") or 0)
            # Fallback include whole season if still zero
            if mins == 0 and aps:
                mins = goals = assists = 0
                for ap in aps:
                    gid = ap.get("game_id")
                    if season and game_seasons.get(gid) != season:
                        continue
                    mins += int(ap.get("minutes_played") or 0)
                    goals += int(ap.get("goals") or 0)
                    assists += int(ap.get("assists") or 0)
            contrib = goals + assists
            eur_per_minutes = round(fee / mins, 3) if mins else None
            eur_per_contrib = round(fee / contrib, 3) if contrib else None
            row = {
                "player_id": pid,
                "player_name": t.get("player_name"),
                "transfer_season": t.get("transfer_season"),
                "transfer_fee": fee,
                "market_value_in_eur": t.get("market_value_in_eur"),
                "post_minutes": mins,
                "post_goals": goals,
                "post_assists": assists,
                "eur_per_minutes": eur_per_minutes,
                "eur_per_contrib": eur_per_contrib,
            }
            if debug:
                row["_debug_apps"] = len(aps)
                row["_debug_transfer_date"] = t.get("transfer_date")
            rows.append(row)

        key_map = {
            "transfer_fee": lambda r: r.get("transfer_fee") or 0,
            "market_value_in_eur": lambda r: r.get("market_value_in_eur") or 0,
            "post_minutes": lambda r: r.get("post_minutes") or -1,
            "post_goals": lambda r: r.get("post_goals") or -1,
            "post_assists": lambda r: r.get("post_assists") or -1,
            "eur_per_minutes": lambda r: r.get("eur_per_minutes") or -1,
            "eur_per_contrib": lambda r: r.get("eur_per_contrib") or -1,
        }
        key_fn = key_map.get(sort_by, key_map["transfer_fee"])
        rows.sort(key=key_fn, reverse=(order==-1))
        return rows

    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo", debug=debug))

# Competitions list
@app.get("/api/competitions")
def api_competitions():
    sql = """
      SELECT DISTINCT c.competition_id, c.name AS competition_name, c.type
      FROM competition c
      ORDER BY c.name
    """
    rows, ms, perf = run_sql_ex(sql)
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Mongo: Competitions list (distinct from games)
@app.get("/api/mongo/competitions")
def api_mongo_competitions():
    def _q(db):
        # get distinct competition_id and first name seen
        cur = db.games.aggregate([
            {"$group": {"_id": "$competition_id", "competition_name": {"$first": "$competition_name"}}},
            {"$sort": {"competition_name": 1}}
        ])
        out = []
        for d in cur:
            out.append({
                "competition_id": d.get("_id"),
                "competition_name": d.get("competition_name"),
                "type": None
            })
        return out
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo"))

# Seasons for a competition (for Top Scorers season dropdown)
@app.get("/api/competitions/<comp_id>/seasons")
def api_competition_seasons(comp_id):
    sql = """
      SELECT DISTINCT g.season
      FROM game g
      WHERE g.competition_id = %s
      ORDER BY g.season DESC
    """
    rows, ms, perf = run_sql_ex(sql, (comp_id,))
    return jsonify(dict(ms=ms, rows=rows, perf=perf))

# Mongo: Seasons for a competition
@app.get("/api/mongo/competitions/<comp_id>/seasons")
def api_mongo_competition_seasons(comp_id):
    def _q(db):
        cur = db.games.find({"competition_id": comp_id}, {"season":1})
        seasons = sorted({d.get("season") for d in cur if d.get("season")}, reverse=True)
        return [{"season": s} for s in seasons]
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo"))

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
    # Provide full SQL text with parameter bound as a literal for display purposes
    # Note: This is for UI only; execution still uses parameterized query
    try:
        # Safely render date literal with single quotes
        bound_sql = sql.replace("%s", f"'{sel_date}'")
    except Exception:
        bound_sql = sql
    perf_out = dict(perf)
    perf_out["query"] = bound_sql
    return jsonify(dict(ms=ms, rows=rows, date=sel_date, source="sql", perf=perf_out))

@app.get("/api/mongo/matches/by-date")
def api_mongo_matches_by_date():
    sel_date = request.args.get("date")
    # Compute heavy perf diagnostics (explain) only when explicitly requested
    want_perf = (request.args.get("perf") == "1")
    # Manually time only the aggregate execution; gather perf separately
    # Build pipeline so we can call explain for richer details
    pipeline = [
        {"$match": {"date": sel_date}},
        {"$project": {
            "competition_id": 1, "competition_name": 1, "_id": 1,
            "home": 1, "away": 1
        }},
        {"$sort": {"competition_id": 1, "_id": 1}}
    ]
    # Execute aggregation for rows (timed)
    t_exec_start = time.perf_counter()
    try:
        agg_cur = mongo_db.games.aggregate(pipeline, allowDiskUse=False, maxTimeMS=1500)
        docs = list(agg_cur)
    except Exception as err:
        docs = []
    exec_ms = round((time.perf_counter() - t_exec_start) * 1000.0, 2)
    # Transform output
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
            "league_name": d.get("competition_name")
        })
    # Perf details (not counted in ms)
    try:
        import json
        query_text = json.dumps(pipeline, ensure_ascii=False, indent=2)
    except Exception:
        query_text = str(pipeline)
    explain = None
    if want_perf:
        try:
            explain = mongo_db.command({
                "explain": {"aggregate": "games", "pipeline": pipeline, "cursor": {}},
                "verbosity": "executionStats"
            })
        except Exception:
            explain = None
    stats = {"docs_returned": len(out)}
    stats.update(mongo_exec_stats_totals(explain or {}))
    perf = {"explain": explain, "stats": stats, "query": query_text}
    # Return execution time for query only
    return jsonify(dict(ms=exec_ms, rows=out, date=sel_date, source="mongo", perf=perf))

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

# Mongo: Top market value players
@app.get("/api/mongo/players/top-market")
def api_mongo_top_market():
    limit_k = min(max(int(request.args.get("k", 10)), 1), 50)
    want_perf = (request.args.get("perf") == "1")
    # Time only the find + sort + limit execution
    t_exec_start = time.perf_counter()
    try:
        cursor = mongo_db.players.find(
            {"market_value_eur": {"$ne": None}},
            {"player_id": 1, "name": 1, "market_value_eur": 1, "current_club_id": 1}
        ).sort("market_value_eur", -1).limit(limit_k)
        docs = list(cursor)
    except Exception:
        docs = []
    exec_ms = round((time.perf_counter() - t_exec_start) * 1000.0, 2)
    rows = []
    for d in docs:
        d.pop("_id", None)
        rows.append({
            "player_id": d.get("player_id"),
            "name": d.get("name"),
            "market_value_eur": d.get("market_value_eur"),
            "current_club_id": d.get("current_club_id"),
        })
    # Perf details (excluded from ms)
    perf = {}
    # Query description text
    import json
    query_desc = {
        "filter": {"market_value_eur": {"$ne": None}},
        "projection": ["player_id", "name", "market_value_eur", "current_club_id"],
        "sort": {"market_value_eur": -1},
        "limit": limit_k,
    }
    try:
        perf["query"] = json.dumps(query_desc, ensure_ascii=False, indent=2)
    except Exception:
        perf["query"] = str(query_desc)
    perf["stats"] = {"docs_returned": len(rows)}
    if want_perf:
        try:
            exp = mongo_db.command({
                "explain": {
                    "find": "players",
                    "filter": {"market_value_eur": {"$ne": None}},
                    "projection": {"player_id": 1, "name": 1, "market_value_eur": 1, "current_club_id": 1},
                    "sort": {"market_value_eur": -1},
                    "limit": limit_k
                },
                "verbosity": "executionStats"
            })
            perf["explain"] = exp
            perf["stats"].update(mongo_exec_stats_totals(exp))
        except Exception:
            perf["explain"] = None
    return jsonify(dict(ms=exec_ms, rows=rows, source="mongo", perf=perf))

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

# Mongo: Player search
@app.get("/api/mongo/players/search")
def api_mongo_players_search():
    q = request.args.get("q", "").strip()
    def _q(db):
        query = {}
        if q:
            query["name"] = {"$regex": q, "$options": "i"}
        cur = db.players.find(query, {"player_id":1, "name":1}).sort("name", 1).limit(20)
        out = []
        for d in cur:
            out.append({"player_id": d.get("player_id"), "name": d.get("name")})
        return out
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo"))

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

# Mongo: Player career transfers
@app.get("/api/mongo/player/<int:pid>/career")
def api_mongo_player_career(pid):
    def _q(db):
        cur = db.transfers.find({"player_id": int(pid), "transfer_fee": {"$ne": None}}, {
            "transfer_date": 1, "transfer_season": 1, "from.club_id": 1, "from.name": 1,
            "to.club_id": 1, "to.name": 1, "transfer_fee": 1, "market_value_in_eur": 1
        }).sort("transfer_date", -1)
        out = []
        for d in cur:
            out.append({
                "transfer_date": d.get("transfer_date"),
                "transfer_season": d.get("transfer_season"),
                "from_club_id": (d.get("from") or {}).get("club_id"),
                "from_club": (d.get("from") or {}).get("name"),
                "to_club_id": (d.get("to") or {}).get("club_id"),
                "to_club": (d.get("to") or {}).get("name"),
                "transfer_fee": d.get("transfer_fee"),
                "market_value_in_eur": d.get("market_value_in_eur")
            })
        return out
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo"))

# Mongo: Player competitions
@app.get("/api/mongo/players/<int:pid>/competitions")
def api_mongo_player_competitions(pid):
    def _q(db):
        # Distinct competition IDs for this player
        cur = db.player_seasons.find({"player_id": int(pid)}, {"competition_id": 1})
        comp_ids = sorted({d.get("competition_id") for d in cur if d.get("competition_id")})
        if not comp_ids:
            return []
        # Determine player's domestic league via current club
        player_doc = db.players.find_one({"player_id": int(pid)}, {"current_club_id":1}) or {}
        current_club_id = player_doc.get("current_club_id")
        domestic_comp = None
        if current_club_id is not None:
            club_doc = db.clubs.find_one({"club_id": current_club_id}, {"domestic_competition_id":1}) or {}
            domestic_comp = club_doc.get("domestic_competition_id")
        # Fetch names from games (denormalized) – one sample per competition
        name_map = {}
        for g in db.games.find({"competition_id": {"$in": comp_ids}}, {"competition_id":1, "competition_name":1}):
            cid = g.get("competition_id")
            if cid is not None and cid not in name_map:
                name_map[cid] = g.get("competition_name")
        out = []
        for cid in comp_ids:
            out.append({
                "competition_id": cid,
                "competition_name": name_map.get(cid) or cid,
                "competition_type": "domestic-league" if domestic_comp and cid == domestic_comp else None
            })
        return out
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo"))

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

# Mongo: Seasons for a player in a competition
@app.get("/api/mongo/players/<int:pid>/seasons")
def api_mongo_player_seasons(pid):
    comp = request.args.get("competition_id")
    if not comp:
        return jsonify(dict(error="competition_id required")), 400
    def _q(db):
        cur = db.player_seasons.find({"player_id": int(pid), "competition_id": comp}, {"season":1})
        seasons = sorted({d.get("season") for d in cur if d.get("season")}, reverse=True)
        return [{"season": s} for s in seasons]
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo"))

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

# Mongo: Player profile
@app.get("/api/mongo/player/<int:pid>/profile")
def api_mongo_player_profile(pid):
    def _q(db):
        doc = db.players.find_one({"player_id": int(pid)}, {
            "player_id": 1, "name": 1, "position": 1, "sub_position": 1,
            "current_club_id": 1, "current_club_name": 1,
            "market_value_eur": 1, "highest_market_value_eur": 1,
            "image_url": 1, "height_in_cm": 1, "dob": 1, "country_of_citizenship": 1,
            "foot": 1, "city_of_birth": 1, "agent_name": 1, "contract_expiration_date": 1
        }) or None
        if not doc:
            return None
        doc.pop("_id", None)
        return doc
    row, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, row=row, source="mongo"))

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

# Mongo: Player season summary
@app.get("/api/mongo/player/<int:pid>/season-summary")
def api_mongo_player_season_summary(pid):
    def _q(db):
        cur = db.player_seasons.find({"player_id": int(pid)}, {"competition_id": 1, "season": 1, "totals": 1})
        out = []
        for d in cur:
            t = d.get("totals", {})
            out.append({
                "competition_id": d.get("competition_id"),
                "season": d.get("season"),
                "apps": t.get("apps") or 0,
                "minutes": t.get("minutes") or 0,
                "goals": t.get("goals") or 0,
                "assists": t.get("assists") or 0,
                "yellows": t.get("yc") or 0,
                "reds": t.get("rc") or 0
            })
        out.sort(key=lambda r: (r.get("season"), r.get("competition_id")), reverse=True)
        return out
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo"))

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

# Mongo: market compare
@app.get("/api/mongo/market-compare")
def api_mongo_market_compare():
    category = (request.args.get("category") or "").strip().lower()
    value = request.args.get("value")
    limit_n = min(max(int(request.args.get("limit", 100)), 1), 200)
    allowed = {"age", "citizenship", "club", "position", "agent", "city"}
    if category not in allowed or value is None or value == "":
        return jsonify(dict(ms=0, rows=[], error="invalid-params")), 400
    from datetime import date
    today = date.today()
    def compute_age(dob_str):
        try:
            if not dob_str:
                return None
            y, m, d = map(int, dob_str.split('-'))
            age = today.year - y - ((today.month, today.day) < (m, d))
            return age
        except Exception:
            return None
    def _q(db):
        cur = db.players.find({}, {"player_id":1,"name":1,"market_value_eur":1,"dob":1,"country_of_citizenship":1,"current_club_id":1,"position":1,"agent_name":1,"city_of_birth":1})
        out = []
        for d in cur:
            if category == 'age':
                try:
                    target_age = int(value)
                except ValueError:
                    continue
                age = compute_age(d.get('dob'))
                if age is None or age != target_age:
                    continue
            elif category == 'citizenship':
                if d.get('country_of_citizenship') != value:
                    continue
            elif category == 'club':
                try:
                    club_id = int(value)
                except ValueError:
                    continue
                if d.get('current_club_id') != club_id:
                    continue
            elif category == 'position':
                if d.get('position') != value:
                    continue
            elif category == 'agent':
                if d.get('agent_name') != value:
                    continue
            elif category == 'city':
                if d.get('city_of_birth') != value:
                    continue
            mv = d.get('market_value_eur')
            if mv is None:
                continue
            out.append({"player_id": d.get("player_id"), "name": d.get("name"), "market_value_eur": mv})
        out.sort(key=lambda r: (r.get('market_value_eur') or 0), reverse=True)
        return out[:limit_n]
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, category=category, value=value, limit=limit_n, source='mongo'))

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

# Mongo: Games list
@app.get("/api/mongo/games/list")
def api_mongo_games_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    date = request.args.get("date", "").strip()
    competition = request.args.get("competition_id", "").strip()
    season = request.args.get("season", "").strip()
    skip = (page - 1) * page_size

    def _q(db):
        # Build query filters
        query = {}
        if date:
            query["date"] = date
        if competition:
            query["competition_id"] = competition
        if season:
            query["season"] = season
        total = db.games.count_documents(query)

        # We project nested home/away subdocuments if present. Older ETL versions flattened names/goals.
        projection = {
            "game_id": 1, "date": 1, "competition_id": 1, "competition_name": 1,
            "round": 1, "season": 1,
            # nested structure
            "home": 1, "away": 1,
            # fallback flat fields (if any legacy docs exist)
            "home_club_id": 1, "away_club_id": 1,
            "home_club_goals": 1, "away_club_goals": 1,
            "home_club_name": 1, "away_club_name": 1,
            "stadium": 1, "attendance": 1
        }
        cur = db.games.find(query, projection).sort("date", -1).skip(skip).limit(page_size)
        out = []
        for g in cur:
            home = g.get("home") or {}
            away = g.get("away") or {}
            # Determine IDs
            home_id = home.get("club_id") or g.get("home_club_id")
            away_id = away.get("club_id") or g.get("away_club_id")
            # Names fallback to IDs
            home_name = home.get("name") or g.get("home_club_name") or home_id
            away_name = away.get("name") or g.get("away_club_name") or away_id
            # Goals fallback to flat fields
            home_goals = home.get("goals") if home.get("goals") is not None else g.get("home_club_goals")
            away_goals = away.get("goals") if away.get("goals") is not None else g.get("away_club_goals")

            out.append({
                "game_id": g.get("game_id"),
                "date_str": g.get("date"),
                "competition_id": g.get("competition_id"),
                "league_name": g.get("competition_name"),
                "round": g.get("round"),
                "season": g.get("season"),
                "home_club_id": home_id,
                "home_name": home_name,
                "away_club_id": away_id,
                "away_name": away_name,
                "home_club_goals": home_goals,
                "away_club_goals": away_goals,
                "stadium": g.get("stadium"),
                "attendance": g.get("attendance")
            })
        return out, total
    (rows, total), ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size, total=total, source="mongo"))

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

# Mongo: Games seasons (distinct seasons from games collection)
@app.get("/api/mongo/games/seasons")
def api_mongo_games_seasons():
    def _q(db):
        cur = db.games.find({}, {"season": 1})
        seasons = sorted({d.get("season") for d in cur if d.get("season")}, reverse=True)
        return [{"season": s} for s in seasons]
    rows, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, source="mongo"))

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

# Mongo: Appearances list (optional read model)
@app.get("/api/mongo/appearances/list")
def api_mongo_appearances_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    search = request.args.get("search", "").strip()
    skip = (page - 1) * page_size
    debug_flag = request.args.get("debug") == "1"

    def _q(db):
        # Support multiple possible collection names for robustness.
        coll_name = None
        for cand in ["appearances", "appearance", "player_appearances"]:
            if cand in db.list_collection_names():
                coll_name = cand
                break
        if not coll_name:
            return [], 0, {"reason": "no_collection"}
        coll = db[coll_name]

        # Build search query with synonym matching for player/club names
        query = {}
        if search:
            or_clauses = []
            # Numeric game_id match
            try:
                g_id = int(search)
                or_clauses.append({"game_id": g_id})
                or_clauses.append({"gameId": g_id})
            except ValueError:
                pass
            regex_clause = {"$regex": search, "$options": "i"}
            for name_field in ["player_name", "name", "playerName"]:
                or_clauses.append({name_field: regex_clause})
            for club_field in ["club_name", "club", "team_name", "clubName"]:
                or_clauses.append({club_field: regex_clause})
            if or_clauses:
                query["$or"] = or_clauses

        total = coll.count_documents(query)

        # Determine sort field – prefer 'date', else fallback to other date synonyms.
        sort_field = None
        for sf in ["date", "match_date", "date_str", "game_date", "transfer_date"]:
            if coll.find_one({sf: {"$exists": True}}):
                sort_field = sf
                break
        if not sort_field:
            sort_field = "appearance_id"  # fallback deterministic ordering

        projection = {
            "appearance_id": 1,
            "game_id": 1,
            "gameId": 1,
            "match_id": 1,
            "player_id": 1,
            "playerId": 1,
            "player_club_id": 1,
            "club_id": 1,
            "clubId": 1,
            "player_current_club_id": 1,
            "date": 1,
            "date_str": 1,
            "match_date": 1,
            "game_date": 1,
            "yellow_cards": 1,
            "yellow": 1,
            "yc": 1,
            "red_cards": 1,
            "red": 1,
            "rc": 1,
            "goals": 1,
            "goal": 1,
            "goals_scored": 1,
            "assists": 1,
            "assist": 1,
            "minutes_played": 1,
            "minutes": 1,
            "mins": 1,
            "time_played": 1,
            "player_name": 1,
            "name": 1,
            "playerName": 1,
            "club_name": 1,
            "club": 1,
            "team_name": 1,
            "clubName": 1,
            "stats": 1
        }

        cur = coll.find(query, projection).sort(sort_field, -1).skip(skip).limit(page_size)
        rows = []
        first_doc_keys = None
        for d in cur:
            if first_doc_keys is None:
                first_doc_keys = list(d.keys())
            stats = d.get("stats") or {}
            def pick(*keys):
                for k in keys:
                    v = d.get(k)
                    if v is not None and v != "":
                        return v
                return None
            appearance_id = pick("appearance_id", "_id")
            game_id = pick("game_id", "gameId", "match_id")
            player_id = pick("player_id", "playerId")
            player_club_id = pick("player_club_id", "club_id", "clubId")
            player_current_club_id = d.get("player_current_club_id")
            date_val = pick("date", "date_str", "match_date", "game_date")
            yellow = pick("yellow_cards", "yellow", "yc") or stats.get("yellow_cards")
            red = pick("red_cards", "red", "rc") or stats.get("red_cards")
            goals = pick("goals", "goal", "goals_scored") or stats.get("goals")
            assists = pick("assists", "assist") or stats.get("assists")
            minutes_played = pick("minutes_played", "minutes", "mins", "time_played") or stats.get("minutes_played")
            player_name = pick("player_name", "name", "playerName")
            club_name = pick("club_name", "club", "team_name", "clubName")
            rows.append({
                "appearance_id": appearance_id,
                "game_id": game_id,
                "player_id": player_id,
                "player_club_id": player_club_id,
                "player_current_club_id": player_current_club_id,
                "date": date_val,
                "yellow_cards": yellow,
                "red_cards": red,
                "goals": goals,
                "assists": assists,
                "minutes_played": minutes_played,
                "player_name": player_name,
                "club_name": club_name
            })
        meta = {"collection": coll_name, "sort_field": sort_field}
        if debug_flag:
            meta["first_doc_keys"] = first_doc_keys
        return rows, total, meta
    (rows, total, meta), ms = run_mongo(_q)
    payload = dict(ms=ms, rows=rows, page=page, page_size=page_size, total=total, source="mongo")
    if debug_flag:
        payload["meta"] = meta
    return jsonify(payload)

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

# Mongo: transfers list with pagination & search
@app.get("/api/mongo/transfers/list")
def api_mongo_transfers_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    search = request.args.get("search", "").strip()
    offset = (page - 1) * page_size

    def _q(db):
        flt = {}
        if search:
            regex = {"$regex": search, "$options": "i"}
            flt["$or"] = [
                {"player_name": regex},
                {"from.name": regex},
                {"to.name": regex},
            ]
        total = db.transfers.count_documents(flt)
        docs = db.transfers.find(flt).sort("transfer_date", -1).skip(offset).limit(page_size)
        rows = []
        for d in docs:
            rows.append({
                "transfer_id": d.get("_id"),
                "player_id": d.get("player_id"),
                "player_name": d.get("player_name"),
                "from_club_id": (d.get("from") or {}).get("club_id"),
                "from_club_name": d.get("from_club_name") or (d.get("from") or {}).get("name"),
                "to_club_id": (d.get("to") or {}).get("club_id"),
                "to_club_name": d.get("to_club_name") or (d.get("to") or {}).get("name"),
                "transfer_date": d.get("transfer_date"),
                "transfer_season": d.get("transfer_season"),
                "transfer_fee": d.get("transfer_fee"),
                "market_value_in_eur": d.get("market_value_in_eur"),
            })
        return {"rows": rows, "total": total}

    data, ms = run_mongo(_q)
    return jsonify(dict(ms=ms, source="mongo", rows=data["rows"], total=data["total"], page=page, page_size=page_size))

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

# Mongo: delete transfer document
@app.delete("/api/mongo/transfer/<int:transfer_id>")
def api_mongo_transfer_delete(transfer_id):
    def _del(db):
        res = db.transfers.delete_one({"_id": transfer_id})
        return {"deleted": res.deleted_count}
    data, ms = run_mongo(_del)
    if data.get("deleted") == 0:
        return jsonify({"error": "Transfer not found", "ms": ms, "source": "mongo"}), 404
    return jsonify({"success": True, "transfer_id": transfer_id, "ms": ms, "source": "mongo"})

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

# Mongo: Game events list (unwinds events embedded in games collection)
@app.get("/api/mongo/game-events/list")
def api_mongo_game_events_list():
    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 20)), 1), 100)
    game_id = request.args.get("game_id", "").strip()
    event_type = request.args.get("type", "").strip()
    skip = (page - 1) * page_size

    # Map UI event categories to underlying type predicates
    category_filters = {
        "Goals": lambda t: "goal" in (t or "").lower(),
        "Cards": lambda t: "card" in (t or "").lower(),
        "Substitutions": lambda t: "sub" in (t or "").lower(),
        "Shootout": lambda t: "shoot" in (t or "").lower() or "pen" in (t or "").lower(),
    }

    def _q(db):
        if "games" not in db.list_collection_names():
            return [], 0
        base_query = {}
        if game_id:
            try:
                base_query["_id"] = int(game_id)
            except ValueError:
                base_query["_id"] = game_id  # if string IDs ever used

        # Fetch matching games first (small subset if game_id provided)
        cur = db.games.find(base_query, {"_id":1, "date":1, "home":1, "away":1, "events":1})
        all_events = []
        for g in cur:
            gid = g.get("_id")
            home = g.get("home") or {}
            away = g.get("away") or {}
            home_id = home.get("club_id")
            away_id = away.get("club_id")
            home_name = home.get("name")
            away_name = away.get("name")
            events = g.get("events") or []
            for idx, ev in enumerate(events):
                t = ev.get("type") or ev.get("event_type")
                # Category filter
                if event_type and event_type in category_filters:
                    if not category_filters[event_type](t):
                        continue
                club_id = ev.get("club_id")
                # Resolve club name using home/away context
                club_name = None
                if club_id == home_id:
                    club_name = home_name
                elif club_id == away_id:
                    club_name = away_name
                # Assist / sub naming
                assist_name = ev.get("assist_name")
                player_in_name = ev.get("player_in_name")
                row = {
                    # Synthesize stable event id (game_event_id missing in embedded doc)
                    "game_event_id": ev.get("game_event_id") or f"{gid}_{idx}",
                    "game_id": gid,
                    "minute": ev.get("minute"),
                    "type": t,
                    "club_id": club_id,
                    "club_name": club_name,
                    "player_id": ev.get("player_id"),
                    "player_name": ev.get("player_name"),
                    "player_in_id": ev.get("sub_in_id") or ev.get("player_in_id"),
                    "player_in_name": player_in_name,
                    "player_assist_id": ev.get("assist_id") or ev.get("player_assist_id"),
                    "assist_name": assist_name,
                    "description": ev.get("event_desc") or ev.get("description")
                }
                all_events.append(row)

        total = len(all_events)
        # Sort similar to SQL: minute ascending (nulls last), then synthetic id
        def sort_key(r):
            m = r.get("minute")
            if m is None:
                m_val = 999999
            else:
                try:
                    m_val = int(m)
                except Exception:
                    m_val = 999999
            return (m_val, str(r.get("game_event_id")))
        all_events.sort(key=sort_key)
        page_rows = all_events[skip: skip + page_size]
        return page_rows, total

    (rows, total), ms = run_mongo(_q)
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size, total=total, source="mongo"))

# Helper to generate next sequence for Mongo-only game events
def mongo_next_sequence(name):
    doc = mongo_db.counters.find_one_and_update(
        {"_id": name}, {"$inc": {"seq": 1}}, upsert=True, return_document=ReturnDocument.AFTER
    )
    return doc.get("seq")

# Mongo: Get single game event
@app.get("/api/mongo/game-event/<event_id>")
def api_mongo_game_event_get(event_id):
    # event_id could be numeric or string
    try:
        # try int casting for consistent type matching
        eid_int = int(event_id)
        id_query_vals = [eid_int, event_id]
    except ValueError:
        id_query_vals = [event_id]
    game_doc = mongo_db.games.find_one({"events.game_event_id": {"$in": id_query_vals}}, {"_id":1, "events":1})
    if not game_doc:
        return jsonify({"error": "Event not found"}), 404
    for ev in game_doc.get("events", []):
        if ev.get("game_event_id") in id_query_vals:
            ev_out = ev.copy()
            ev_out["game_id"] = game_doc.get("_id")
            return jsonify(dict(row=ev_out))
    return jsonify({"error": "Event not found"}), 404

# Mongo: Create game event (append to game's events array)
@app.post("/api/mongo/game-event")
def api_mongo_game_event_create():
    data = request.get_json() or {}
    game_id = data.get("game_id")
    if game_id is None:
        return jsonify({"error": "game_id required"}), 400
    # Accept int or str game id (depending on ETL)
    try:
        game_id_cast = int(game_id)
    except (TypeError, ValueError):
        game_id_cast = game_id
    game_doc = mongo_db.games.find_one({"_id": game_id_cast}, {"_id":1})
    if not game_doc:
        return jsonify({"error": "Game not found"}), 404

    new_id = mongo_next_sequence("game_event_id")
    # Optionally enrich names from Mongo players collection
    def _player_name(pid):
        if pid is None: return None
        doc = mongo_db.players.find_one({"player_id": pid}, {"name":1})
        return doc.get("name") if doc else None

    event_doc = {
        "game_event_id": new_id,
        "minute": data.get("minute"),
        "type": data.get("type"),
        "club_id": data.get("club_id"),
        "player_id": data.get("player_id"),
        "player_name": data.get("player_name") or _player_name(data.get("player_id")),
        "sub_in_id": data.get("player_in_id"),
        "player_in_name": data.get("player_in_name") or _player_name(data.get("player_in_id")),
        "assist_id": data.get("player_assist_id"),
        "assist_name": data.get("assist_name") or _player_name(data.get("player_assist_id")),
        "event_desc": data.get("description") or data.get("event_desc")
    }
    mongo_db.games.update_one({"_id": game_id_cast}, {"$push": {"events": event_doc}})
    event_doc["game_id"] = game_id_cast
    return jsonify(dict(row=event_doc)), 201

# Mongo: Update game event
@app.post("/api/mongo/game-event/<event_id>/update")
def api_mongo_game_event_update(event_id):
    data = request.get_json() or {}
    try:
        eid_int = int(event_id)
        id_query_vals = [eid_int, event_id]
    except ValueError:
        id_query_vals = [event_id]
    game_doc = mongo_db.games.find_one({"events.game_event_id": {"$in": id_query_vals}}, {"_id":1})
    if not game_doc:
        return jsonify({"error": "Event not found"}), 404
    game_id = game_doc.get("_id")

    # Build update fields (only set provided keys)
    set_ops = {}
    mapping = {
        "minute": "minute",
        "type": "type",
        "club_id": "club_id",
        "player_id": "player_id",
        "player_name": "player_name",
        "player_in_id": "sub_in_id",
        "player_in_name": "player_in_name",
        "player_assist_id": "assist_id",
        "assist_name": "assist_name",
        "description": "event_desc",
        "event_desc": "event_desc"
    }
    for inp_key, field in mapping.items():
        if inp_key in data:
            set_ops[f"events.$.{field}"] = data.get(inp_key)
    if not set_ops:
        return jsonify({"error": "No fields to update"}), 400

    res = mongo_db.games.update_one({"_id": game_id, "events.game_event_id": {"$in": id_query_vals}}, {"$set": set_ops})
    if res.modified_count == 0:
        return jsonify({"error": "Update failed"}), 500
    return jsonify({"success": True, "game_event_id": event_id})

# Mongo: Delete game event
@app.delete("/api/mongo/game-event/<event_id>")
def api_mongo_game_event_delete(event_id):
    try:
        eid_int = int(event_id)
        id_query_vals = [eid_int, event_id]
    except ValueError:
        id_query_vals = [event_id]
    game_doc = mongo_db.games.find_one({"events.game_event_id": {"$in": id_query_vals}}, {"_id":1})
    if not game_doc:
        return jsonify({"error": "Event not found"}), 404
    res = mongo_db.games.update_one({"_id": game_doc.get("_id")}, {"$pull": {"events": {"game_event_id": {"$in": id_query_vals}}}})
    if res.modified_count == 0:
        return jsonify({"error": "Delete failed"}), 500
    return jsonify({"success": True, "deleted": event_id})

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
