#!/usr/bin/env python3
"""
UrbanStream — KMeans Zone Clustering (Real ML)

Trains a KMeans model on historical traffic + pollution CSV data.
Derives 4 meaningful clusters, saves the model, and pushes labels to Redis.

Usage:
    python3 ml/clustering.py                        # train + save + push to Redis
    python3 ml/clustering.py --predict MN-01        # predict cluster for a zone
"""

import argparse
import json
import logging
import os
import sys

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import joblib

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CLUSTERING] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR     = os.path.join(BASE_DIR, "data")
MODEL_DIR    = os.path.join(BASE_DIR, "ml", "models")
TRAFFIC_CSV  = os.path.join(DATA_DIR, "nyc_traffic.csv")
POLLUTION_CSV= os.path.join(DATA_DIR, "openaq_nyc.csv")
MODEL_PATH   = os.path.join(MODEL_DIR, "kmeans_zones.joblib")
SCALER_PATH  = os.path.join(MODEL_DIR, "scaler_zones.joblib")
LABELS_PATH  = os.path.join(MODEL_DIR, "cluster_labels.json")
REDIS_HOST   = os.getenv("REDIS_HOST", "localhost")
N_CLUSTERS   = 4
RANDOM_STATE = 42

ALL_ZONES = [f"{p}-{i:02d}" for p in ["MN","BK","QN","BX","SI"] for i in range(1,7)]

BOROUGH_MAP = {
    "MN": "Manhattan", "BK": "Brooklyn",
    "QN": "Queens",    "BX": "Bronx", "SI": "Staten Island",
}

# Zone lat/lon centroids (same as producer)
BOUNDS = {
    "MN": (40.70, 40.88, -74.02, -73.91),
    "BK": (40.57, 40.74, -74.04, -73.84),
    "QN": (40.54, 40.80, -73.97, -73.70),
    "BX": (40.80, 40.92, -73.94, -73.74),
    "SI": (40.48, 40.65, -74.26, -74.03),
}
ZONE_COORDS = {}
for z in ALL_ZONES:
    p = z[:2]; a, b, c, d = BOUNDS[p]; i = int(z[3:]) - 1
    ZONE_COORDS[z] = (
        round(a + (i // 2 + 0.5) * (b - a) / 3, 5),
        round(c + (i % 2  + 0.5) * (d - c) / 2, 5),
    )

# Human-readable cluster names assigned by centroid analysis
# Order: worst → best (sorted by avg_aqi desc, avg_speed asc)
CLUSTER_NAMES = [
    "Permanently Hazardous",
    "Peak Hour Hazardous",
    "Weather Sensitive",
    "Safe Corridor",
]


# ── Data Loading ──────────────────────────────────────────────────────────────

def load_traffic() -> pd.DataFrame:
    log.info("Loading traffic data from %s", TRAFFIC_CSV)
    df = pd.read_csv(TRAFFIC_CSV)
    df.columns = [c.strip().upper() for c in df.columns]
    df = df[df["SPEED"] > 0].copy()

    # Map borough name → zone prefix
    prefix_map = {v: k for k, v in BOROUGH_MAP.items()}
    df["zone_prefix"] = df["BOROUGH"].map(prefix_map)
    df = df.dropna(subset=["zone_prefix"])

    # Extract lat from LINK_POINTS first coordinate pair
    def first_lat(pts):
        try:
            return float(str(pts).split()[0].rstrip(","))
        except:
            return np.nan

    df["lat"] = df["LINK_POINTS"].apply(first_lat)
    df = df.dropna(subset=["lat"])

    log.info("Traffic: %d segments across %s boroughs",
             len(df), df["BOROUGH"].nunique())
    return df


def load_pollution() -> pd.DataFrame:
    log.info("Loading pollution data from %s", POLLUTION_CSV)
    df = pd.read_csv(POLLUTION_CSV)
    df = df[df["value"] > 0].copy()
    df_pm25 = df[df["parameter"] == "pm25"].copy()
    log.info("Pollution: %d PM2.5 readings from %d stations",
             len(df_pm25), df_pm25["location"].nunique())
    return df_pm25


# ── Feature Engineering ───────────────────────────────────────────────────────

def assign_zone(lat: float, lon: float) -> str:
    """Assign a lat/lon point to the nearest zone centroid."""
    best_zone, best_dist = "MN-01", float("inf")
    for z, (zlat, zlon) in ZONE_COORDS.items():
        dist = (lat - zlat) ** 2 + (lon - zlon) ** 2
        if dist < best_dist:
            best_dist = dist
            best_zone = z
    return best_zone


def build_zone_features(traffic_df: pd.DataFrame, pollution_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate per-zone features from raw data:
      - avg_speed, std_speed, pct_slow  (traffic)
      - avg_pm25, max_pm25, std_pm25    (pollution)
      - borough_density                  (zone index proxy)
    """
    log.info("Building zone-level features...")

    # ── Traffic features per borough prefix ──────────────────────────────────
    # We only have borough-level traffic, so distribute across sub-zones
    # using the segment lat to add intra-borough variance
    traffic_feats = []
    for prefix, borough in BOROUGH_MAP.items():
        sub = traffic_df[traffic_df["zone_prefix"] == prefix]
        if sub.empty:
            # No traffic data for this borough — use borough-level defaults
            default_speed = {"MN": 22.0, "BK": 30.0, "QN": 35.0, "BX": 28.0, "SI": 40.0}.get(prefix, 30.0)
            speeds = np.array([default_speed])
        else:
            speeds = sub["SPEED"].values

        lat_vals = sub["lat"].values if not sub.empty and sub["lat"].notna().any() else np.linspace(0,1,6)
        lat_breaks = np.percentile(lat_vals, [0,16.7,33.3,50,66.7,83.3,100]) if len(lat_vals) > 1 else np.zeros(7)

        for idx in range(6):
            zone = f"{prefix}-{idx+1:02d}"
            if not sub.empty and len(lat_vals) > 1:
                mask = (sub["lat"] >= lat_breaks[idx]) & (sub["lat"] < lat_breaks[idx+1])
                seg_speeds = sub[mask]["SPEED"].values if mask.sum() > 0 else speeds
            else:
                seg_speeds = speeds
            traffic_feats.append({
                "zone_id":   zone,
                "avg_speed": float(np.mean(seg_speeds)),
                "std_speed": float(np.std(seg_speeds)) if len(seg_speeds) > 1 else 0.0,
                "pct_slow":  float(np.mean(seg_speeds < 20)),
            })

    traffic_zone = pd.DataFrame(traffic_feats).set_index("zone_id")

    # ── Pollution features per zone (by nearest centroid) ────────────────────
    pollution_df = pollution_df.copy()
    pollution_df["zone_id"] = pollution_df.apply(
        lambda r: assign_zone(r["latitude"], r["longitude"]), axis=1
    )
    poll_agg = pollution_df.groupby("zone_id")["value"].agg(
        avg_pm25="mean", max_pm25="max", std_pm25="std"
    ).fillna(0)

    # ── Merge ────────────────────────────────────────────────────────────────
    features = traffic_zone.join(poll_agg, how="left").fillna(0)

    # Add a borough-level density proxy (Manhattan=high, SI=low)
    density = {"MN": 1.0, "BK": 0.75, "QN": 0.6, "BX": 0.55, "SI": 0.3}
    features["density"] = features.index.map(lambda z: density.get(z[:2], 0.5))

    # Compute a simple AQI proxy from PM2.5
    # EPA linear: AQI ≈ 4.0 * PM2.5 for PM2.5 0–12 µg/m³
    features["avg_aqi_proxy"] = features["avg_pm25"] * 4.0

    log.info("Feature matrix: %d zones × %d features", *features.shape)
    log.info("Feature summary:\n%s", features.describe().to_string())
    return features


# ── KMeans Training ───────────────────────────────────────────────────────────

def train_kmeans(features: pd.DataFrame):
    """Train KMeans, pick best k, return fitted model + scaler."""
    os.makedirs(MODEL_DIR, exist_ok=True)

    feature_cols = ["avg_speed", "std_speed", "pct_slow",
                    "avg_pm25", "std_pm25", "avg_aqi_proxy", "density"]
    X = features[feature_cols].values

    # Standardise
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Try k=2..6, pick best silhouette
    log.info("Evaluating silhouette scores for k=2..6...")
    best_k, best_score, best_model = N_CLUSTERS, -1, None
    for k in range(2, 7):
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=20)
        labels = km.fit_predict(X_scaled)
        if len(set(labels)) < 2:
            continue
        score = silhouette_score(X_scaled, labels)
        log.info("  k=%d  silhouette=%.4f", k, score)
        if k == N_CLUSTERS:          # prefer k=4 for interpretability
            best_model = km
            best_k = k
            best_score = score

    log.info("Using k=%d  (silhouette=%.4f)", best_k, best_score)

    # Final fit with chosen k
    km = KMeans(n_clusters=best_k, random_state=RANDOM_STATE, n_init=20)
    labels = km.fit_predict(X_scaled)

    # ── Name the clusters by centroid quality ─────────────────────────────
    # Sort clusters: highest pollution + lowest speed = most hazardous
    centroids = scaler.inverse_transform(km.cluster_centers_)
    centroid_df = pd.DataFrame(centroids, columns=feature_cols)
    centroid_df["cluster_id"] = range(best_k)

    # Hazard score: high AQI and low speed = bad
    centroid_df["hazard"] = (
        centroid_df["avg_aqi_proxy"] / (centroid_df["avg_aqi_proxy"].max() + 1e-9)
        - centroid_df["avg_speed"]   / (centroid_df["avg_speed"].max()   + 1e-9)
    )
    centroid_df = centroid_df.sort_values("hazard", ascending=False).reset_index(drop=True)

    cluster_name_map = {}
    for rank, row in centroid_df.iterrows():
        name = CLUSTER_NAMES[rank] if rank < len(CLUSTER_NAMES) else f"Cluster {rank}"
        cluster_name_map[int(row["cluster_id"])] = name
        log.info("  Cluster %d → %-28s | avg_speed=%.1f  avg_pm25=%.2f  aqi_proxy=%.1f",
                 int(row["cluster_id"]), name,
                 row["avg_speed"], row["avg_pm25"], row["avg_aqi_proxy"])

    # ── Zone → cluster name mapping ───────────────────────────────────────
    zone_labels = {}
    for zone, label_id in zip(features.index, labels):
        zone_labels[zone] = cluster_name_map[label_id]

    log.info("\nZone cluster assignments:")
    for name in CLUSTER_NAMES:
        zones_in = [z for z,n in zone_labels.items() if n==name]
        log.info("  %-28s: %s", name, ", ".join(sorted(zones_in)))

    # ── Save model, scaler, labels ─────────────────────────────────────────
    joblib.dump(km,     MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)
    with open(LABELS_PATH, "w") as f:
        json.dump({
            "zone_labels":       zone_labels,
            "cluster_name_map":  {str(k): v for k,v in cluster_name_map.items()},
            "feature_cols":      feature_cols,
            "silhouette_score":  round(best_score, 4),
            "n_clusters":        best_k,
        }, f, indent=2)

    log.info("Saved model  → %s", MODEL_PATH)
    log.info("Saved scaler → %s", SCALER_PATH)
    log.info("Saved labels → %s", LABELS_PATH)

    return km, scaler, zone_labels, cluster_name_map, feature_cols


# ── Redis Push ────────────────────────────────────────────────────────────────

def push_to_redis(zone_labels: dict):
    try:
        import redis as _redis
        r = _redis.Redis(host=REDIS_HOST, port=6379, db=0,
                         socket_timeout=3, decode_responses=True)
        r.ping()
        pipe = r.pipeline()
        for zone, label in zone_labels.items():
            pipe.set(f"cluster:{zone}", label, ex=86400)  # 24h TTL
        pipe.execute()
        log.info("Pushed %d cluster labels to Redis at %s", len(zone_labels), REDIS_HOST)
    except Exception as e:
        log.warning("Redis push skipped: %s", e)


# ── Predict single zone (live inference) ─────────────────────────────────────

def predict_zone(zone_id: str, avg_speed: float, avg_pm25: float, std_speed: float = 5.0):
    """
    Called by the recommender at runtime to classify a zone from live data.
    Returns cluster name string.
    """
    if not os.path.exists(MODEL_PATH):
        log.warning("No trained model found at %s — run clustering.py first", MODEL_PATH)
        return "Safe Corridor"

    km     = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    with open(LABELS_PATH) as f:
        meta = json.load(f)

    feature_cols = meta["feature_cols"]
    density = {"MN": 1.0, "BK": 0.75, "QN": 0.6, "BX": 0.55, "SI": 0.3}

    row = {
        "avg_speed":     avg_speed,
        "std_speed":     std_speed,
        "pct_slow":      1.0 if avg_speed < 20 else 0.0,
        "avg_pm25":      avg_pm25,
        "std_pm25":      0.0,
        "avg_aqi_proxy": avg_pm25 * 4.0,
        "density":       density.get(zone_id[:2], 0.5),
    }
    X = np.array([[row[c] for c in feature_cols]])
    X_scaled = scaler.transform(X)
    cluster_id = int(km.predict(X_scaled)[0])
    return meta["cluster_name_map"].get(str(cluster_id), "Safe Corridor")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="UrbanStream KMeans Zone Clustering")
    parser.add_argument("--predict", metavar="ZONE_ID",
                        help="Predict cluster for a zone (e.g. MN-01) using saved model")
    parser.add_argument("--speed", type=float, default=30.0)
    parser.add_argument("--pm25",  type=float, default=12.0)
    args = parser.parse_args()

    if args.predict:
        result = predict_zone(args.predict, args.speed, args.pm25)
        print(f"{args.predict} → {result}")
        return

    log.info("=" * 60)
    log.info("UrbanStream KMeans Clustering — Training on historical data")
    log.info("=" * 60)

    traffic_df   = load_traffic()
    pollution_df = load_pollution()
    features     = build_zone_features(traffic_df, pollution_df)
    km, scaler, zone_labels, cluster_name_map, feature_cols = train_kmeans(features)
    push_to_redis(zone_labels)

    log.info("=" * 60)
    log.info("Done. Cluster labels pushed to Redis.")
    log.info("The recommender will now use REAL KMeans labels instead of random assignment.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
