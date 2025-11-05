import os, time
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
import pymysql

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
      SELECT a.game_id, g.date, a.minutes_played, a.goals, a.assists
      FROM appearance a JOIN game g ON g.game_id=a.game_id
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
      SELECT a.player_id, p.name as player_name, SUM(a.goals) AS goals
      FROM appearance a 
      JOIN game g ON g.game_id=a.game_id
      JOIN player p ON p.player_id=a.player_id
      WHERE g.competition_id=%s AND g.season=%s
      GROUP BY a.player_id, p.name
      ORDER BY goals DESC
      LIMIT %s OFFSET %s
    """
    rows, ms = run_sql(sql, (comp, season, page_size, offset))
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size))

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

@app.get("/api/club/roi")
def api_club_roi():
    club_id = int(request.args.get("club_id"))
    season  = request.args.get("season")
    sort_by = request.args.get("sort_by","post_minutes")
    order   = "DESC" if request.args.get("order","desc").lower()=="desc" else "ASC"
    cols = {"post_minutes","post_goals","post_assists","minutes_per_eur","contrib_per_eur","transfer_fee","market_value_in_eur"}
    if sort_by not in cols: sort_by = "post_minutes"
    sql = f"""
      SELECT player_id, transfer_season, transfer_fee, market_value_in_eur,
             post_minutes, post_goals, post_assists,
             minutes_per_eur, contrib_per_eur
      FROM view_club_transfer_roi
      WHERE club_id=%s AND transfer_season=%s
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
      SELECT DISTINCT competition_id
      FROM game
      ORDER BY competition_id
    """
    rows, ms = run_sql(sql)
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
        g.home_club_goals, g.away_club_goals
      FROM game g
      JOIN club hc ON hc.club_id = g.home_club_id
      JOIN club ac ON ac.club_id = g.away_club_id
      WHERE g.date = %s
      ORDER BY g.competition_id, g.game_id
    """
    rows, ms = run_sql(sql, (sel_date,))
    return jsonify(dict(ms=ms, rows=rows, date=sel_date))

# Max match date
@app.get("/api/matches/max-date")
def api_matches_max_date():
    sql = "SELECT MAX(date) AS max_date FROM game"
    rows, ms = run_sql(sql)
    max_date = rows[0]['max_date'].strftime("%Y-%m-%d") if rows and rows[0]['max_date'] else None
    return jsonify(dict(ms=ms, max_date=max_date))


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
    rows, ms = run_sql(sql, (limit_k,))
    return jsonify(dict(ms=ms, rows=rows))

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
    rows, ms = run_sql(sql, (f"%{q}%",))
    return jsonify(dict(ms=ms, rows=rows))

# Competitions for a player
@app.get("/api/players/<int:pid>/competitions")
def api_player_competitions(pid):
    sql = """
      SELECT DISTINCT g.competition_id
      FROM appearance a
      JOIN game g ON g.game_id = a.game_id
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




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)
