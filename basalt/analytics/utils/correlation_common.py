from __future__ import annotations

from typing import Iterable, Optional, Sequence

import numpy as np


def _as_float_array(values: Sequence[float]) -> np.ndarray:
    arr = np.asarray(values, dtype="float64")
    if arr.ndim != 1:
        raise ValueError("Expected a 1D array of values.")
    return arr[np.isfinite(arr)]


def hurst_exponent(
    values: Sequence[float],
    *,
    max_lag: int = 20,
    lags: Optional[Iterable[int]] = None,
) -> Optional[float]:
    """
    Estimate the Hurst exponent using the variance of lagged differences.

    Notes:
    - Values are filtered to finite numbers before computation.
    - Returns None if there is not enough data for the requested lags.
    - Hurst interpretation (rule of thumb):
      H < 0.5: mean reversion, H ~ 0.5: random walk, H > 0.5: trending.
    """
    series = _as_float_array(values)
    if series.size < 4:
        return None

    if lags is None:
        max_lag = max(2, int(max_lag))
        max_lag = min(max_lag, series.size - 1)
        lags = range(2, max_lag + 1)
    else:
        lags = [lag for lag in lags if 1 < lag < series.size]

    lags = list(lags)
    if not lags:
        return None

    tau = []
    valid_lags = []
    for lag in lags:
        diffs = series[lag:] - series[:-lag]
        scale = np.std(diffs)
        if not np.isfinite(scale) or scale <= 0:
            continue
        tau.append(np.sqrt(scale))
        valid_lags.append(lag)

    if len(valid_lags) < 2:
        return None

    slope, _ = np.polyfit(np.log(valid_lags), np.log(tau), 1)
    return float(slope * 2.0)


def _normalize_matrix_rows(values: np.ndarray, mode: str) -> np.ndarray:
    if mode == "none":
        return values
    if mode == "l2":
        denom = np.sqrt(np.sum(values * values, axis=1, keepdims=True))
    elif mode == "mean":
        denom = np.mean(np.abs(values), axis=1, keepdims=True)
    elif mode == "max":
        denom = np.max(np.abs(values), axis=1, keepdims=True)
    else:
        raise ValueError(
            "normalisation must be one of: 'l2', 'mean', 'max', 'none'."
        )
    denom = np.where(denom == 0, 1.0, denom)
    return values / denom


def _soft_corr_core(values: np.ndarray, *, alpha: float, full_norm_alpha_scale: bool) -> np.ndarray:
    # Correlation-like matrix with magnitude-smoothing.
    v = np.einsum("ik,jk->ijk", values, values)
    v = np.sign(v) * np.abs(v) ** alpha
    v = v.mean(axis=2)
    if full_norm_alpha_scale:
        v = np.sign(v) * np.abs(v) ** (1.0 / alpha)
    diag = np.diag(v).copy()
    denom = np.sqrt(np.einsum("i,j->ij", diag, diag))
    denom = np.where(denom == 0, 1.0, denom)
    return v / denom


def soft_corr_matrix(
    values: np.ndarray,
    *,
    alpha: float = 1.0,
    sign_only: bool = False,
    recenter: bool = False,
    normalisation: str = "l2",
    multiresolution: Optional[Sequence[int]] = None,
    num_periods_rolling: Optional[int] = None,
    cumulative_product: bool = False,
    cumulative_product_renormalise: Optional[str] = None,
    full_norm_alpha_scale: bool = True,
) -> np.ndarray:
    """
    Compute a soft correlation matrix with magnitude-aware smoothing.

    Parameters map to the archived experimental logic but are simplified:
    - `alpha` controls how strongly large moves dominate.
    - `sign_only` compares only direction (sign of changes).
    - `recenter` removes each series mean before correlation.
    - `normalisation` controls per-series scaling (`l2`, `mean`, `max`, `none`).
    - `multiresolution` averages correlations over block-averaged resolutions.
    - `num_periods_rolling` averages correlations over shrinking tail windows.
    - `cumulative_product` uses cumulative product (price-like) rather than returns.
    - `cumulative_product_renormalise` rescales cumulative products by mean or min.
    """
    if values.ndim != 2:
        raise ValueError("Expected a 2D array shaped (n_series, n_samples).")
    if values.shape[1] < 2:
        return np.eye(values.shape[0])

    data = np.asarray(values, dtype="float64")
    data = data[:, np.isfinite(data).all(axis=0)]
    if data.shape[1] < 2:
        return np.eye(values.shape[0])

    if cumulative_product:
        data = data.copy()
        data[:, 0] = 1.0
        data = np.cumprod(data, axis=1)
        if cumulative_product_renormalise:
            if cumulative_product_renormalise == "mean":
                scale = data.mean(axis=1, keepdims=True)
            elif cumulative_product_renormalise == "min":
                scale = data.min(axis=1, keepdims=True)
            else:
                raise ValueError(
                    "cumulative_product_renormalise must be 'mean' or 'min'."
                )
            scale = np.where(scale == 0, 1.0, scale)
            data = data / scale

    if sign_only:
        data = np.sign(data)
    if recenter:
        data = data - data.mean(axis=1, keepdims=True)
    data = _normalize_matrix_rows(data, normalisation)

    def _corr_for_block(block: np.ndarray) -> np.ndarray:
        return _soft_corr_core(
            block, alpha=alpha, full_norm_alpha_scale=full_norm_alpha_scale
        )

    def _apply_multiresolution(block: np.ndarray) -> np.ndarray:
        if not multiresolution:
            return _corr_for_block(block)
        total = None
        count = 0
        for window in multiresolution:
            window = int(window)
            if window <= 0:
                continue
            length = (block.shape[1] // window) * window
            if length <= 0:
                continue
            reshaped = (
                block[:, -length:]
                .reshape(block.shape[0], length // window, window)
                .mean(axis=2)
            )
            corr = _corr_for_block(reshaped)
            total = corr if total is None else total + corr
            count += 1
        if count == 0:
            return _corr_for_block(block)
        return total / count

    if num_periods_rolling:
        total = None
        count = 0
        for i in range(int(num_periods_rolling)):
            length = int(data.shape[1] // (2**i))
            if length <= 1:
                continue
            corr = _apply_multiresolution(data[:, -length:])
            total = corr if total is None else total + corr
            count += 1
        if count == 0:
            return _apply_multiresolution(data)
        return total / count

    return _apply_multiresolution(data)
