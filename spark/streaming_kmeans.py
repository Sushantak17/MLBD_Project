"""
UrbanStream — StreamingKMeans
Pure-Python MiniBatchKMeans-style model.
Extracted into its own module so it can be unit-tested without Spark or Redis.
Imported by stream_processor.py at runtime.
"""

import random
from collections import defaultdict

CLUSTER_NAMES = [
    "Permanently Hazardous",
    "Peak Hour Hazardous",
    "Weather Sensitive",
    "Safe Corridor",
]


class StreamingKMeans:
    """
    Pure-Python streaming k-means with MiniBatchKMeans-style partial_fit.
    Features (4D): [norm_speed, norm_aqi, norm_pm25, norm_density]
    """
    def __init__(self, k: int = 4):
        self.k         = k
        self.centroids = None
        self.counts    = [0] * k
        self.n_batches = 0
        self.inertia   = float("inf")

    _feat_min = [0.0,   0.0,  0.0, 0.0]
    _feat_max = [80.0, 200.0, 60.0, 1.0]

    def _norm(self, x):
        return [
            (x[i] - self._feat_min[i]) / max(1e-9, self._feat_max[i] - self._feat_min[i])
            for i in range(len(x))
        ]

    def _update_range(self, X):
        for i in range(len(self._feat_min)):
            vals = [row[i] for row in X]
            self._feat_min[i] = min(self._feat_min[i], min(vals))
            self._feat_max[i] = max(self._feat_max[i], max(vals))

    @staticmethod
    def _dist(a, b):
        return sum((ai - bi) ** 2 for ai, bi in zip(a, b))

    def _assign(self, X_norm):
        labels = []
        for x in X_norm:
            best, best_d = 0, float("inf")
            for j, c in enumerate(self.centroids):
                d = self._dist(x, c)
                if d < best_d:
                    best_d, best = d, j
            labels.append(best)
        return labels

    def partial_fit(self, X):
        if len(X) < self.k:
            return list(range(len(X)))
        self._update_range(X)
        X_norm = [self._norm(x) for x in X]
        if self.centroids is None:
            self.centroids = []
            mean = [sum(x[i] for x in X_norm) / len(X_norm) for i in range(len(X_norm[0]))]
            first = min(range(len(X_norm)), key=lambda i: self._dist(X_norm[i], mean))
            self.centroids.append(list(X_norm[first]))
            for _ in range(self.k - 1):
                dists = [min(self._dist(x, c) for c in self.centroids) for x in X_norm]
                total = sum(dists)
                if total < 1e-12:
                    idx = random.randrange(len(X_norm))
                else:
                    r = random.random() * total
                    cumul = 0.0
                    idx = len(X_norm) - 1
                    for i, d in enumerate(dists):
                        cumul += d
                        if cumul >= r:
                            idx = i
                            break
                self.centroids.append(list(X_norm[idx]))
            self.counts = [0] * self.k
        labels = self._assign(X_norm)
        batch_counts = defaultdict(int)
        batch_sums   = defaultdict(lambda: [0.0] * len(X_norm[0]))
        for x, lbl in zip(X_norm, labels):
            batch_counts[lbl] += 1
            for i, v in enumerate(x):
                batch_sums[lbl][i] += v
        for j in range(self.k):
            if batch_counts[j] == 0:
                continue
            n_j = batch_counts[j]
            self.counts[j] += n_j
            eta = n_j / self.counts[j]
            new_c = [batch_sums[j][i] / n_j for i in range(len(self.centroids[j]))]
            self.centroids[j] = [
                (1 - eta) * self.centroids[j][i] + eta * new_c[i]
                for i in range(len(self.centroids[j]))
            ]
        self.inertia = sum(
            self._dist(X_norm[i], self.centroids[labels[i]])
            for i in range(len(X_norm))
        )
        self.n_batches += 1
        return labels

    def semantic_labels(self, zones, labels):
        if self.centroids is None:
            return {z: "Safe Corridor" for z in zones}

        def denorm(c):
            return [
                c[i] * (self._feat_max[i] - self._feat_min[i]) + self._feat_min[i]
                for i in range(len(c))
            ]

        hazard = {j: denorm(c)[1] - denorm(c)[0] for j, c in enumerate(self.centroids)}
        sorted_clusters = sorted(hazard.keys(), key=lambda x: -hazard[x])
        name_map = {c: CLUSTER_NAMES[min(i, 3)] for i, c in enumerate(sorted_clusters)}
        return {z: name_map[lbl] for z, lbl in zip(zones, labels)}
