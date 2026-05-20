from flask import Flask, render_template, request, jsonify
import pandas as pd
import os, json
import joblib
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col as scol, lower

app = Flask(__name__)

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CrashIntel Road Accident Analysis") \
    .config("spark.ui.enabled", "false") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")

CSV_PATH = os.path.join(os.getcwd(), "traffic_accidents.csv")

# ── Load Models ───────────────────────────────────────────────────────────────
# Model 1: Severity (original)
model         = None
label_encoder = None
feature_names = None
metrics_data  = None

# Model 2: Fatal Prediction
model_fatal        = None
feature_names_fatal = None
metrics_fatal      = None



if os.path.exists('model.pkl'):
    model         = joblib.load('model.pkl')
    label_encoder = joblib.load('label_encoder.pkl')
    feature_names = joblib.load('feature_names.pkl')
    print("✅ Severity model loaded")
else:
    print("⚠️  model.pkl not found")

if os.path.exists('model_fatal.pkl'):
    model_fatal        = joblib.load('model_fatal.pkl')
    feature_names_fatal = joblib.load('feature_names_fatal.pkl')
    print("✅ Fatal prediction model loaded")
else:
    print("⚠️  model_fatal.pkl not found")



if os.path.exists('metrics.json'):
    with open('metrics.json') as f: metrics_data = json.load(f)
if os.path.exists('metrics_fatal.json'):
    with open('metrics_fatal.json') as f: metrics_fatal = json.load(f)


CACHE_PATH = os.path.join(os.getcwd(), "stats_cache.json")

# ── PySpark Analysis ───────────────────────────────────────────────────────────
def run_pyspark_analysis():
    if os.path.exists(CACHE_PATH):
        print("✅ Loading stats from cache (stats_cache.json) — skipping PySpark...")
        with open(CACHE_PATH) as f:
            return json.load(f)

    if not os.path.exists(CSV_PATH):
        print(f"WARNING: {CSV_PATH} not found.")
        return None

    print("Running PySpark analysis on traffic_accidents.csv (first run — will cache results)...")
    df_spark = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    df_spark.cache()

    total = df_spark.count()

    severity_rows = df_spark.groupBy("most_severe_injury").count().orderBy("count", ascending=False).collect()
    severity = [{"name": str(r["most_severe_injury"]), "count": int(r["count"])}
                for r in severity_rows if r["most_severe_injury"] is not None]

    hour_rows = df_spark.groupBy("crash_hour").count().orderBy("crash_hour").collect()
    hour_map  = {r["crash_hour"]: int(r["count"]) for r in hour_rows if r["crash_hour"] is not None}
    hour = [{"hour": f"{h}:00", "count": hour_map.get(h, 0)} for h in range(24)]

    day_rows  = df_spark.groupBy("crash_day_of_week").count().orderBy("crash_day_of_week").collect()
    day_map   = {r["crash_day_of_week"]: int(r["count"]) for r in day_rows if r["crash_day_of_week"] is not None}
    day_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    day = [{"day": day_names[i], "count": day_map.get(i+1, 0)} for i in range(7)]

    month_rows  = df_spark.groupBy("crash_month").count().orderBy("crash_month").collect()
    month_map   = {r["crash_month"]: int(r["count"]) for r in month_rows if r["crash_month"] is not None}
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    month = [{"month": month_names[i], "count": month_map.get(i+1, 0)} for i in range(12)]

    weather_rows = df_spark.groupBy("weather_condition").count().orderBy("count", ascending=False).collect()
    weather = [{"name": str(r["weather_condition"]), "count": int(r["count"])}
               for r in weather_rows if r["weather_condition"] is not None][:7]

    road_rows = df_spark.groupBy("roadway_surface_cond").count().orderBy("count", ascending=False).collect()
    road = [{"name": str(r["roadway_surface_cond"]), "count": int(r["count"])}
            for r in road_rows if r["roadway_surface_cond"] is not None][:5]

    peak_row  = df_spark.groupBy("crash_hour").count().orderBy("count", ascending=False).first()
    peak_hour = int(peak_row["crash_hour"]) if peak_row else 0
    avg_per_day = round(total / 365, 1)
    top_weather = weather[0]["name"] if weather else "N/A"
    top_road    = road[0]["name"]    if road    else "N/A"

    fatal_count = 0
    if "most_severe_injury" in df_spark.columns:
        fatal_count = int(df_spark.filter(
            lower(scol("most_severe_injury")).contains("fatal") |
            lower(scol("most_severe_injury")).contains("incapacitat")
        ).count())

    table_data = df_spark.limit(2000).toPandas().fillna("N/A").to_dict(orient='records')
    df_spark.unpersist()
    print("PySpark analysis complete.")

    result = {
        "total":    total,
        "severity": severity,
        "hour":     hour,
        "day":      day,
        "month":    month,
        "weather":  weather,
        "road":     road,
        "table":    table_data,
        "summary": {
            "total":       total,
            "avg_per_day": avg_per_day,
            "peak_hour":   f"{peak_hour}:00",
            "fatal_count": fatal_count,
            "top_weather": top_weather,
            "top_road":    top_road,
        }
    }

    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(result, f)
        print("✅ Cached to stats_cache.json — next startup will be instant")
    except Exception as e:
        print(f"⚠️  Cache save failed: {e}")

    return result

STATS = run_pyspark_analysis()

# ── Helper: build feature row ──────────────────────────────────────────────────
def build_row(data):
    return {
        'crash_hour':             int(data.get('hour', 8)),
        'crash_day_of_week':      int(data.get('day', 1)),
        'crash_month':            int(data.get('month', 6)),
        'num_units':              int(str(data.get('units','2')).replace('+','')),
        'weather_condition':      data.get('weather', 'Clear'),
        'lighting_condition':     data.get('lighting', 'Daylight'),
        'traffic_control_device': data.get('traffic', 'Traffic Signal'),
        'roadway_surface_cond':   data.get('road', 'Dry'),
    }

def align_features(df_enc, feat_list):
    for col in feat_list:
        if col not in df_enc.columns:
            df_enc[col] = 0
    return df_enc[feat_list]

# ── Insight Generator ──────────────────────────────────────────────────────────
def generate_severity_insight(data, label, level):
    h       = int(data.get('hour', 8))
    weather = data.get('weather', 'Clear')
    road    = data.get('road', 'Dry')
    light   = data.get('lighting', 'Daylight')
    units   = int(str(data.get('units','2')).replace('+',''))

    risk_factors = []
    if h >= 22 or h <= 4:
        risk_factors.append("late-night driving")
    elif 16 <= h <= 19:
        risk_factors.append("rush-hour congestion")
    if weather in ['Rain','Snow','Sleet','Fog']:
        risk_factors.append(f"{weather.lower()} weather conditions")
    if road in ['Snow/Slush','Ice','Wet']:
        risk_factors.append(f"{road.lower()} road surface")
    if light in ['Dark - No Controls','Dark - Unknown Lighting']:
        risk_factors.append("poor visibility at night")
    if units >= 3:
        risk_factors.append("multi-vehicle involvement")

    pl = label.lower()
    if level == 'high':
        if risk_factors:
            sentence1 = f"The combination of {', '.join(risk_factors[:2])} significantly raises injury severity in this scenario."
        else:
            sentence1 = "This scenario presents multiple compounding risk factors that indicate serious injury potential."
        sentence2 = "Immediate emergency response readiness is strongly recommended under these conditions."
    elif level == 'moderate':
        if risk_factors:
            sentence1 = f"Factors like {risk_factors[0]} contribute to a moderate severity outcome in this scenario."
        else:
            sentence1 = "Conditions suggest a moderate likelihood of injury — standard safety precautions apply."
        sentence2 = "Drivers should reduce speed and maintain greater following distance to lower injury risk."
    else:
        if risk_factors:
            sentence1 = f"Despite {risk_factors[0]}, overall conditions point toward a lower-severity outcome."
        else:
            sentence1 = "Current conditions are relatively favourable, indicating a lower expected injury severity."
        sentence2 = "Remain alert — even low-risk scenarios can escalate with unexpected hazards on the road."

    return f"{sentence1} {sentence2}"

def generate_fatal_insight(data, fatal_prob, level):
    h       = int(data.get('hour', 8))
    weather = data.get('weather', 'Clear')
    road    = data.get('road', 'Dry')
    light   = data.get('lighting', 'Daylight')

    risk_factors = []
    if h >= 22 or h <= 4:
        risk_factors.append("nighttime hours")
    if weather in ['Snow','Fog','Sleet']:
        risk_factors.append(f"{weather.lower()} weather")
    if road in ['Ice','Snow/Slush']:
        risk_factors.append(f"{road.lower()} road conditions")
    if light in ['Dark - No Controls','Dark - Unknown Lighting']:
        risk_factors.append("unlit roadway")

    if level == 'high':
        if risk_factors:
            sentence1 = f"The presence of {' and '.join(risk_factors[:2])} substantially increases the probability of a fatal outcome."
        else:
            sentence1 = "The input conditions are associated with a high statistical likelihood of fatality."
        sentence2 = "Strict adherence to speed limits, seatbelt use, and hazard avoidance is critical in this scenario."
    elif level == 'moderate':
        if risk_factors:
            sentence1 = f"Conditions such as {risk_factors[0]} elevate fatal risk above baseline levels."
        else:
            sentence1 = "There is a moderate chance of a fatal outcome — situational awareness is essential."
        sentence2 = "Reducing speed by 10–15 km/h and increasing stopping distance can meaningfully cut fatality risk."
    else:
        if risk_factors:
            sentence1 = f"While {risk_factors[0]} is present, overall conditions keep fatal probability low."
        else:
            sentence1 = "The given conditions are associated with a low likelihood of fatal injury."
        sentence2 = "Standard safe-driving practices are sufficient, though vigilance should always be maintained."

    return f"{sentence1} {sentence2}"

# ── Routes ─────────────────────────────────────────────────────────────────────
@app.route('/')
def landing():      return render_template('landing.html')

@app.route('/dashboard')
def dashboard():    return render_template('dashboard.html')

@app.route('/predict')
def predict_page(): return render_template('predict.html')

@app.route('/metrics')
def metrics_page(): return render_template('metrics.html')

@app.route('/about')
def about_page():   return render_template('about.html')

@app.route('/data')
def data_page():    return render_template('data.html')

# ── API: Stats ─────────────────────────────────────────────────────────────────
@app.route('/api/stats')
def api_stats():
    if STATS is None:
        return jsonify({'error': 'traffic_accidents.csv not found'}), 404
    return jsonify(STATS)

@app.route('/api/charts')
def api_charts():
    if STATS is None:
        return jsonify({'error': 'traffic_accidents.csv not found'}), 404
    return jsonify({k: v for k, v in STATS.items() if k != 'table'})

@app.route('/api/table')
def api_table():
    if STATS is None:
        return jsonify({'data': [], 'total': 0})
    table = STATS.get('table', [])
    from flask import request as req
    try:
        page     = int(req.args.get('page', 1))
        per_page = int(req.args.get('per_page', 50))
    except:
        page, per_page = 1, 50
    start = (page - 1) * per_page
    end   = start + per_page
    return jsonify({
        'data':     table[start:end],
        'total':    len(table),
        'page':     page,
        'per_page': per_page,
        'pages':    (len(table) + per_page - 1) // per_page
    })

# ── API: Metrics ───────────────────────────────────────────────────────────────
@app.route('/api/metrics')
def api_metrics():
    if metrics_data is None:
        return jsonify({'error': 'metrics.json not found.'}), 404
    return jsonify(metrics_data)

@app.route('/api/metrics/fatal')
def api_metrics_fatal():
    if metrics_fatal is None:
        return jsonify({'error': 'metrics_fatal.json not found.'}), 404
    return jsonify(metrics_fatal)



# ── API: Predict Severity (original) ─────────────────────────────────────────
@app.route('/api/predict', methods=['POST'])
def api_predict():
    data = request.get_json()

    if model is not None and label_encoder is not None and feature_names is not None:
        try:
            row    = build_row(data)
            df_row = pd.DataFrame([row])
            df_enc = pd.get_dummies(df_row)
            df_enc = align_features(df_enc, feature_names)

            pred_idx   = model.predict(df_enc)[0]
            pred_label = label_encoder.inverse_transform([pred_idx])[0]
            proba      = float(model.predict_proba(df_enc).max() * 100)

            pl = pred_label.lower()
            if 'fatal' in pl or 'incapacitat' in pl:
                level, icon, risk = 'high',     '🔴', 'High Risk'
            elif 'non' in pl or 'report' in pl:
                level, icon, risk = 'moderate',  '🟡', 'Moderate Risk'
            else:
                level, icon, risk = 'low',       '🟢', 'Low Risk'

            return jsonify({
                'label':      pred_label,
                'risk':       risk,
                'level':      level,
                'icon':       icon,
                'score':      min(10, round(proba / 10)),
                'confidence': round(proba, 1),
                'insight':    generate_severity_insight(data, pred_label, level),
                'model_used': 'Logistic Regression — Severity'
            })
        except Exception as e:
            print(f"Severity model error: {e}")

    # Heuristic fallback
    score = 0
    h = int(data.get('hour', 8))
    if h >= 22 or h <= 4:    score += 3
    elif 16 <= h <= 19:       score += 1
    if data.get('weather')  in ['Rain','Snow','Sleet','Fog']:                   score += 2
    if data.get('road')     in ['Snow/Slush','Ice']:                            score += 2
    if data.get('lighting') in ['Dark - No Controls','Dark - Unknown Lighting']: score += 2
    if data.get('traffic')  in ['No Controls','None']:                          score += 1
    try:    u = int(str(data.get('units','1')).replace('+',''))
    except: u = 1
    if u >= 3: score += 2
    elif u == 2: score += 1

    if score >= 8:
        r = {'label':'Fatal / Incapacitating Injury','risk':'High Risk','level':'high','icon':'🔴','score':score,'confidence':None,'model_used':'heuristic'}
    elif score >= 5:
        r = {'label':'Non-Incapacitating Injury','risk':'Moderate Risk','level':'moderate','icon':'🟡','score':score,'confidence':None,'model_used':'heuristic'}
    else:
        r = {'label':'No Indication of Injury','risk':'Low Risk','level':'low','icon':'🟢','score':score,'confidence':None,'model_used':'heuristic'}
    r['insight'] = generate_severity_insight(data, r['label'], r['level'])
    return jsonify(r)

# ── API: Predict Fatal ────────────────────────────────────────────────────────
@app.route('/api/predict/fatal', methods=['POST'])
def api_predict_fatal():
    data = request.get_json()

    if model_fatal is not None and feature_names_fatal is not None:
        try:
            row    = build_row(data)
            df_row = pd.DataFrame([row])
            df_enc = pd.get_dummies(df_row)
            df_enc = align_features(df_enc, feature_names_fatal)

            pred      = model_fatal.predict(df_enc)[0]
            probas    = model_fatal.predict_proba(df_enc)[0]
            fatal_prob = round(float(probas[1]) * 100, 1)
            safe_prob  = round(float(probas[0]) * 100, 1)

            is_fatal = bool(pred == 1)
            if fatal_prob >= 40:
                level, icon = 'high', '🔴'
            elif fatal_prob >= 20:
                level, icon = 'moderate', '🟡'
            else:
                level, icon = 'low', '🟢'

            return jsonify({
                'fatal':       is_fatal,
                'label':       'LIKELY FATAL' if is_fatal else 'NO FATALITY EXPECTED',
                'fatal_prob':  fatal_prob,
                'safe_prob':   safe_prob,
                'level':       level,
                'icon':        icon,
                'score':       min(10, round(fatal_prob / 10)),
                'insight':     generate_fatal_insight(data, fatal_prob, level),
                'model_used':  'Logistic Regression — Fatal Prediction'
            })
        except Exception as e:
            print(f"Fatal model error: {e}")

    # Heuristic fallback
    h = int(data.get('hour', 8))
    score = 0
    if h >= 22 or h <= 4: score += 3
    if data.get('weather') in ['Snow','Ice','Fog']: score += 2
    if data.get('road') in ['Ice','Snow/Slush']: score += 2
    if data.get('lighting') in ['Dark - No Controls']: score += 2
    fatal_prob = min(95, score * 10)
    level = 'high' if fatal_prob >= 40 else ('moderate' if fatal_prob >= 20 else 'low')
    return jsonify({
        'fatal':      fatal_prob >= 40,
        'label':      'LIKELY FATAL' if fatal_prob >= 40 else 'NO FATALITY EXPECTED',
        'fatal_prob': fatal_prob,
        'safe_prob':  100 - fatal_prob,
        'level':      level,
        'icon':       '🔴' if level=='high' else ('🟡' if level=='moderate' else '🟢'),
        'score':      min(10, round(fatal_prob/10)),
        'insight':    generate_fatal_insight(data, fatal_prob, level),
        'model_used': 'heuristic'
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000, threaded=True, use_reloader=False)
