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
  g.season,
  DATE_FORMAT(g.date, '%Y-%m-%d') AS date_str,
  g.home_club_id,
  hc.name AS home_name,
  g.home_club_goals,
  g.away_club_id,
  ac.name AS away_name,
  g.away_club_goals,
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
      SELECT DISTINCT c.competition_id, c.name AS competition_name
      FROM competition c
      ORDER BY c.name
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
      SELECT DISTINCT g.competition_id, c.name AS competition_name
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
    rows, ms = run_sql(sql, (pid,))
    return jsonify(dict(ms=ms, row=rows[0] if rows else None))

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
    rows, ms = run_sql(sql, (pid,))
    return jsonify(dict(ms=ms, rows=rows))

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
    rows, ms = run_sql(sql, (pid, comp, season, limit_n))
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)
