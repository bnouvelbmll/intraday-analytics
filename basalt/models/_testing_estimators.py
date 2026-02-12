from __future__ import annotations


class MeanRegressor:
    def __init__(self):
        self._mean = 0.0

    def fit(self, _X, y):
        vals = [float(v) for v in y]
        self._mean = sum(vals) / float(len(vals))
        return self

    def predict(self, X):
        return [self._mean for _ in X]
