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
    return render_template("base.html")

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
      SELECT a.player_id, SUM(a.goals) AS goals
      FROM appearance a JOIN game g ON g.game_id=a.game_id
      WHERE g.competition_id=%s AND g.season=%s
      GROUP BY a.player_id
      ORDER BY goals DESC
      LIMIT %s OFFSET %s
    """
    rows, ms = run_sql(sql, (comp, season, page_size, offset))
    return jsonify(dict(ms=ms, rows=rows, page=page, page_size=page_size))

# Club transfer ROI view
@app.route("/club/roi")
def club_roi_page():
    return render_template("club_roi.html")

@app.get("/api/club/roi")
def api_club_roi():
    club_id = int(request.args.get("club_id"))
    season = request.args.get("season")
    sql = """
      SELECT * FROM view_club_transfer_roi
      WHERE club_id=%s AND transfer_season=%s
      ORDER BY post_minutes DESC LIMIT 50
    """
    rows, ms = run_sql(sql, (club_id, season))
    return jsonify(dict(ms=ms, rows=rows))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)
