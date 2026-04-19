#!/usr/bin/env python3
"""
UrbanStream Recommender v3
- Accumulates worker exposure every 30s using Redis as persistent state
- Reads real zone_score:{zone} keys written by Spark
- Falls back to deterministic synthetic data (not random-per-cycle)
"""
import json, logging, os, random, time
import numpy as np
from datetime import datetime, timezone
from collections import deque
import redis

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [RECOMMENDER] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

REDIS_HOST   = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT   = int(os.getenv("REDIS_PORT", "6379"))
RUN_INTERVAL = int(os.getenv("RUN_INTERVAL", "30"))
NUM_WORKERS   = 50
HIGH_AQI      = 40.0  # WHO AQI threshold — unhealthy for sensitive groups
WHO_DAILY_HRS = 8.0    # WHO: no more than 8h/day above AQI 100
WARN_HRS      = 1.5    # WARNING after 1.5h in high-AQI zone
CRIT_HRS      = 3.0    # CRITICAL after 3h  in high-AQI zone

ALL_ZONES = [f"{p}-{i:02d}" for p in ["MN","BK","QN","BX","SI"] for i in range(1,7)]

CLUSTER_WEIGHTS = {
    "Permanently Hazardous": 0.1,
    "Peak Hour Hazardous":   0.5,
    "Weather Sensitive":     0.7,
    "Safe Corridor":         1.0,
}

# ─── AQI Forecast (ARIMA-lite per zone) ──────────────────────────────────────
# Stores last 12 AQI readings per zone (12 × 30s = 6 minutes of history)
_aqi_history: dict = {z: deque(maxlen=12) for z in ALL_ZONES}

def _forecast_aqi(zone: str, current_aqi: float) -> float:
    """
    Simple AR(3) forecast: predicts next AQI from last 3 readings.
    Falls back to current value if insufficient history.
    Uses least-squares AR coefficients — no external library needed.
    """
    hist = _aqi_history[zone]
    hist.append(current_aqi)

    n = len(hist)
    if n < 4:
        return current_aqi   # not enough history yet

    # Build AR(3) design matrix
    y = np.array(list(hist), dtype=float)
    p = 3
    X = np.column_stack([y[i:n-p+i] for i in range(p)])
    y_target = y[p:]

    try:
        # Least-squares fit: coefficients for AR(3)
        coeffs, _, _, _ = np.linalg.lstsq(X, y_target, rcond=None)
        forecast = float(np.dot(coeffs, y[-p:]))
        # Clip to reasonable AQI range
        return round(max(0.0, min(500.0, forecast)), 1)
    except Exception:
        return current_aqi
    
def get_r():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0,
                       socket_timeout=5, decode_responses=True)

# Stable deterministic zone score (doesn't change randomly each cycle)
def _synthetic_score(zone):
    base = {"MN":0.38,"BK":0.52,"QN":0.64,"BX":0.44,"SI":0.76}.get(zone[:2], 0.5)
    rng  = random.Random(hash(zone) & 0xFFFF)
    # AQI range 50–200 so ~half of zones cross HIGH_AQI=100 and exposure accumulates
    aqi  = rng.uniform(50, 200)
    spd  = rng.uniform(12, 68)
    ss   = min(1.0, spd / 40.0)
    aq   = min(1.0, aqi / 100.0)
    evts = ["NORMAL","NORMAL","NORMAL","CONGESTION_EVENT","POLLUTION_ALERT"]
    return {"zone_id":zone,"zone_score":round((ss*0.5)+(aq*0.5),3),
            "avg_aqi":round(aqi,1),"avg_speed":round(spd,1),
            "event_type":rng.choice(evts)}

def _synthetic_cluster(zone):
    rng = random.Random(hash(zone) & 0xFFFF)
    p = zone[:2]
    w = {"MN":[20,30,30,20],"BK":[30,30,25,15],
         "QN":[40,30,20,10],"BX":[20,25,30,25],"SI":[60,25,10,5]}.get(p,[25,25,25,25])
    return rng.choices(["Safe Corridor","Weather Sensitive",
                        "Peak Hour Hazardous","Permanently Hazardous"],weights=w)[0]

def read_zone_scores(r):
    pipe = r.pipeline()
    for z in ALL_ZONES: pipe.get(f"zone_score:{z}")
    results = pipe.execute()
    scores, real = {}, 0
    for z, raw in zip(ALL_ZONES, results):
        if raw:
            try: scores[z] = json.loads(raw); real += 1; continue
            except: pass
        scores[z] = _synthetic_score(z)
    log.info("Zone scores: %d from Spark + %d synthetic", real, len(ALL_ZONES)-real)
    return scores

def _load_kmeans_labels():
    """Load cluster labels from saved KMeans model file."""
    import os
    labels_path = os.path.join(os.path.dirname(__file__), "models", "cluster_labels.json")
    try:
        with open(labels_path) as f:
            return json.load(f).get("zone_labels", {})
    except Exception:
        return {}

_KMEANS_LABELS = _load_kmeans_labels()

def read_cluster_labels(r):
    pipe = r.pipeline()
    for z in ALL_ZONES: pipe.get(f"cluster:{z}")
    results = pipe.execute()
    return {z: (lbl or _KMEANS_LABELS.get(z) or _synthetic_cluster(z))
            for z, lbl in zip(ALL_ZONES, results)}

def update_exposure(r, zone_scores):
    """Add RUN_INTERVAL seconds of exposure for every worker. Persist in Redis."""
    mins_this_cycle = RUN_INTERVAL / 60.0 * 5   # 5x speed: each 30s = 2.5 min exposure

    # Read current worker zones — try Spark key first, then recommender fallback
    pipe = r.pipeline()
    for i in range(1, NUM_WORKERS+1): pipe.get(f"worker_zone:W-{i:02d}")
    zone_results = pipe.execute()

    worker_zones = {}
    for i, z in enumerate(zone_results):
        wid = f"W-{i+1:02d}"
        # stable zone assignment: changes every 5 minutes but is deterministic
        slot = int(time.time() / 300)
        fallback = ALL_ZONES[(hash(wid + str(slot))) % len(ALL_ZONES)]
        worker_zones[wid] = z if (z and z in ALL_ZONES) else fallback

    # Read existing exposure — use rec_exposure: (recommender-owned) key
    # This is separate from exposure: written by Spark so they never collide
    pipe = r.pipeline()
    for i in range(1, NUM_WORKERS+1): pipe.get(f"rec_exposure:W-{i:02d}")
    exp_results = pipe.execute()

    updated = {}
    pipe = r.pipeline()
    for i, raw in enumerate(exp_results):
        wid = f"W-{i+1:02d}"
        prev = {}
        if raw:
            try: prev = json.loads(raw)
            except: pass

        zone    = worker_zones[wid]
        current_aqi  = float(zone_scores.get(zone, {}).get("avg_aqi", 50.0))
        forecast_aqi = _forecast_aqi(zone, current_aqi)
        # Use the higher of current vs forecast — protective routing
        aqi = max(current_aqi, forecast_aqi)
        # Clean accumulation — prev is always our own rec_exposure: key
        h_mins   = float(prev.get("high_aqi_minutes") or 0.0)
        t_mins   = float(prev.get("total_minutes")    or 0.0)
        aqi_sum  = float(prev.get("aqi_sum")          or 0.0)
        n_cycles = int(prev.get("n_cycles")           or 0)
        h_mins += (mins_this_cycle if aqi > HIGH_AQI else 0.0)
        t_mins  += mins_this_cycle
        aqi_sum += aqi
        n_cycles += 1

        hrs_high = h_mins / 60.0
        avg_aqi  = aqi_sum / n_cycles
        status    = ("CRITICAL" if hrs_high > CRIT_HRS
                     else "WARNING" if hrs_high > WARN_HRS else "SAFE")

        rec = {
            "worker_id":         wid,
            "zone_id":           zone,
            "high_aqi_minutes":  round(h_mins, 3),
            "total_minutes":     round(t_mins, 3),
            "aqi_sum":           round(aqi_sum, 1),
            "n_cycles":          n_cycles,
            "hours_in_high_aqi": round(hrs_high, 3),
            "daily_avg_aqi":     round(avg_aqi, 1),
            "exposure_status":   status,
            "who_pct":           round(min(100, hrs_high / WHO_DAILY_HRS * 100), 1),
            "forecast_aqi":      forecast_aqi,
        }
        updated[wid] = rec
        pipe.set(f"rec_exposure:{wid}", json.dumps(rec), ex=86400)
    pipe.execute()
    return updated, worker_zones

def recommend(wid, exp, final_scores, cluster_labels, zone):
    status = exp["exposure_status"]
    hours  = exp["hours_in_high_aqi"]
    ranked = sorted(final_scores.items(), key=lambda x: -x[1])
    if status == "CRITICAL":
        opts = [(z,s) for z,s in ranked if cluster_labels.get(z)=="Safe Corridor"]
        rz, rs = (opts[0] if opts else ranked[0])
        reason = f"Near limit ({hours:.1f}h) → Safe Corridor"
    elif status == "WARNING":
        opts = [(z,s) for z,s in ranked if cluster_labels.get(z)!="Permanently Hazardous"]
        rz, rs = (opts[0] if opts else ranked[0])
        reason = f"Approaching limit ({hours:.1f}h) → avoiding hazardous"
    else:
        rz, rs = ranked[0]
        reason = f"Best available zone (score {rs:.2f})"
    return {"worker_id":wid,"zone_id":zone,"current_zone":zone,"rec_zone":rz,
            "rec_label":cluster_labels.get(rz,""),"rec_score":rs,
            "status":status,"exposure_status":status,"reason":reason,
            "hours_in_high_aqi":round(hours,2),
            "daily_avg_aqi":round(exp.get("daily_avg_aqi",50),1),
            "who_pct":round(min(100, hours / WHO_DAILY_HRS * 100), 1),
            "ts":datetime.now(timezone.utc).isoformat()}

def run_once(r):
    zone_scores    = read_zone_scores(r)
    cluster_labels = read_cluster_labels(r)

    # ── AR(3) forecast: update per-zone history, replace raw AQI with forecast
    for zone, data in zone_scores.items():
        data["avg_aqi"] = _forecast_aqi(zone, data.get("avg_aqi", 50.0))

    worker_exp, worker_zones = update_exposure(r, zone_scores)

    final_scores = {z: round(zone_scores[z]["zone_score"] *
                              CLUSTER_WEIGHTS.get(cluster_labels[z], 1.0), 4)
                    for z in ALL_ZONES}
    ranked = sorted(final_scores.items(), key=lambda x: -x[1])

    pipe = r.pipeline()
    pipe.delete("zone_rankings")
    for z, s in ranked: pipe.zadd("zone_rankings", {z: s})
    pipe.expire("zone_rankings", 300)

    counts = {"SAFE":0,"WARNING":0,"CRITICAL":0}
    for wid, exp in worker_exp.items():
        s = exp["exposure_status"]
        counts[s] += 1
        rec = recommend(wid, exp, final_scores, cluster_labels, worker_zones[wid])
        pipe.set(f"rec:{wid}", json.dumps(rec), ex=300)

    pipe.set("worker_status_counts", json.dumps(counts), ex=300)
    pipe.set("last_rec_ts", datetime.now().isoformat(timespec="seconds"), ex=300)
    pipe.execute()

    avg_hrs = sum(e["hours_in_high_aqi"] for e in worker_exp.values()) / NUM_WORKERS
    log.info("Safe=%d Warning=%d Critical=%d | avg_exposure=%.3fh | top=%s(%.3f)",
             counts["SAFE"], counts["WARNING"], counts["CRITICAL"],
             avg_hrs, ranked[0][0], ranked[0][1])

def main():
    log.info("UrbanStream Recommender v3 | Redis=%s | interval=%ds", REDIS_HOST, RUN_INTERVAL)
    while True:
        try:
            run_once(get_r())
        except redis.ConnectionError as e:
            log.error("Redis error: %s", e)
        except Exception as e:
            log.error("Error: %s", e, exc_info=True)
        time.sleep(RUN_INTERVAL)

if __name__ == "__main__":
    main()
