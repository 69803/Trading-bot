"""Technical indicator calculations using pure NumPy (no TA-Lib, no pandas-ta).

Indicators available
--------------------
calculate_ema(prices, period)                       → List[float]
calculate_rsi(prices, period)                       → List[float]
calculate_macd(prices, fast, slow, signal)          → (macd, signal, histogram)
calculate_atr(highs, lows, closes, period)          → List[float]
calculate_adx(highs, lows, closes, period)          → List[float]
calculate_volume_ratio(volumes, period)             → float
calculate_signals(closes, ...)                      → dict  [legacy, kept for compat]
"""

from typing import List, Tuple

import numpy as np


def calculate_ema(prices: List[float], period: int) -> List[float]:
    """Calculate Exponential Moving Average.

    Args:
        prices: Ordered list of closing prices (oldest first).
        period: EMA look-back period.

    Returns:
        List of EMA values the same length as *prices*.  The first
        ``period - 1`` values are ``NaN`` (insufficient history).
    """
    if period <= 0:
        raise ValueError(f"period must be > 0, got {period}")

    arr = np.array(prices, dtype=float)
    result = np.full_like(arr, np.nan)

    if len(arr) < period:
        return result.tolist()

    k = 2.0 / (period + 1)
    # Seed with the simple average of the first *period* values
    result[period - 1] = arr[:period].mean()
    for i in range(period, len(arr)):
        result[i] = arr[i] * k + result[i - 1] * (1.0 - k)

    return result.tolist()


def calculate_rsi(prices: List[float], period: int = 14) -> List[float]:
    """Calculate RSI using Wilder's smoothing method.

    Args:
        prices: Ordered list of closing prices (oldest first).
        period: Look-back period (default 14).

    Returns:
        List of RSI values the same length as *prices*.  The first
        ``period`` values are ``NaN``.
    """
    if period <= 0:
        raise ValueError(f"period must be > 0, got {period}")

    arr = np.array(prices, dtype=float)
    n = len(arr)
    result = np.full(n, np.nan)

    if n <= period:
        return result.tolist()

    deltas = np.diff(arr)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    # Seed: simple average of first `period` gains/losses
    avg_gain = gains[:period].mean()
    avg_loss = losses[:period].mean()

    if avg_loss == 0.0:
        result[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[period] = 100.0 - 100.0 / (1.0 + rs)

    # Wilder smoothing for subsequent values
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
        if avg_loss == 0.0:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i] = 100.0 - 100.0 / (1.0 + rs)

    return result.tolist()


def calculate_macd(
    prices: List[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> Tuple[List[float], List[float], List[float]]:
    """Calculate MACD line, signal line and histogram.

    Args:
        prices:        Ordered closing prices (oldest first).
        fast_period:   Fast EMA period (default 12).
        slow_period:   Slow EMA period (default 26).
        signal_period: EMA period applied to the MACD line (default 9).

    Returns:
        Tuple of three same-length lists:
        (macd_line, signal_line, histogram)
        All values are NaN until enough history exists.
    """
    if fast_period <= 0 or slow_period <= 0 or signal_period <= 0:
        raise ValueError("All periods must be > 0")

    n = len(prices)
    nan_list: List[float] = [float("nan")] * n

    if n < slow_period:
        return nan_list[:], nan_list[:], nan_list[:]

    ema_fast = calculate_ema(prices, fast_period)
    ema_slow = calculate_ema(prices, slow_period)

    # MACD line = EMA(fast) – EMA(slow); NaN until both EMAs are valid.
    macd_line: List[float] = [float("nan")] * n
    for i in range(n):
        f, s = ema_fast[i], ema_slow[i]
        if not (np.isnan(f) or np.isnan(s)):
            macd_line[i] = f - s

    # Signal line = EMA(signal_period) of the MACD line.
    # We compute it on the slice that starts at the first valid MACD value.
    signal_line: List[float] = [float("nan")] * n
    histogram: List[float] = [float("nan")] * n

    first_valid = next((i for i, v in enumerate(macd_line) if not np.isnan(v)), None)
    if first_valid is not None:
        macd_slice = macd_line[first_valid:]
        signal_slice = calculate_ema(macd_slice, signal_period)
        for j, (m, sig) in enumerate(zip(macd_slice, signal_slice)):
            idx = first_valid + j
            signal_line[idx] = sig
            if not (np.isnan(m) or np.isnan(sig)):
                histogram[idx] = m - sig

    return macd_line, signal_line, histogram


def calculate_atr(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 14,
) -> List[float]:
    """Calculate Average True Range using Wilder's smoothing.

    True Range = max(H-L, |H-prev_C|, |L-prev_C|)

    Args:
        highs:   High prices (oldest first).
        lows:    Low prices  (oldest first).
        closes:  Close prices (oldest first).
        period:  Smoothing period (default 14).

    Returns:
        ATR values same length as inputs; first ``period-1`` are NaN.
    """
    if period <= 0:
        raise ValueError(f"period must be > 0, got {period}")

    n = len(closes)
    if n == 0:
        return []

    tr: List[float] = [float("nan")] * n
    # First candle: no previous close, use H-L only.
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        hl  = highs[i]  - lows[i]
        hpc = abs(highs[i]  - closes[i - 1])
        lpc = abs(lows[i]   - closes[i - 1])
        tr[i] = max(hl, hpc, lpc)

    # Wilder's smoothing: seed = simple mean of first `period` TRs.
    result: List[float] = [float("nan")] * n
    if n < period:
        return result

    result[period - 1] = float(np.mean(tr[:period]))
    for i in range(period, n):
        result[i] = (result[i - 1] * (period - 1) + tr[i]) / period

    return result


def calculate_adx(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 14,
) -> List[float]:
    """Calculate ADX (Average Directional Index) — measures trend *strength*, not direction.

    ADX < 20  : sideways / weak trend (avoid new entries)
    ADX 20-30 : moderate trend
    ADX > 30  : strong trend (high-confidence entries)

    Uses Wilder's smoothing (same method as ATR and RSI).
    Requires at least ``period * 2 + 1`` candles; earlier values are NaN.

    Args:
        highs:   High prices (oldest first).
        lows:    Low prices  (oldest first).
        closes:  Close prices (oldest first).
        period:  Smoothing period (default 14).

    Returns:
        ADX values same length as inputs; NaN until enough history exists.
    """
    if period <= 0:
        raise ValueError(f"period must be > 0, got {period}")

    n = len(closes)
    result = [float("nan")] * n

    if n < period * 2 + 1:
        return result

    highs_a  = np.array(highs,  dtype=float)
    lows_a   = np.array(lows,   dtype=float)
    closes_a = np.array(closes, dtype=float)

    # ── True Range ────────────────────────────────────────────────────────────
    tr = np.zeros(n)
    tr[0] = highs_a[0] - lows_a[0]
    for i in range(1, n):
        tr[i] = max(
            highs_a[i] - lows_a[i],
            abs(highs_a[i]  - closes_a[i - 1]),
            abs(lows_a[i]   - closes_a[i - 1]),
        )

    # ── Directional Movements ─────────────────────────────────────────────────
    plus_dm  = np.zeros(n)
    minus_dm = np.zeros(n)
    for i in range(1, n):
        up   = highs_a[i] - highs_a[i - 1]
        down = lows_a[i - 1] - lows_a[i]
        plus_dm[i]  = up   if (up   > down and up   > 0) else 0.0
        minus_dm[i] = down if (down > up   and down > 0) else 0.0

    # ── Wilder's smoothing ────────────────────────────────────────────────────
    def _wilder(arr: np.ndarray) -> np.ndarray:
        out = np.full(n, np.nan)
        out[period] = arr[1: period + 1].sum()      # seed = sum of first `period` bars
        for i in range(period + 1, n):
            out[i] = out[i - 1] - out[i - 1] / period + arr[i]
        return out

    sm_tr    = _wilder(tr)
    sm_plus  = _wilder(plus_dm)
    sm_minus = _wilder(minus_dm)

    # ── DX → ADX ──────────────────────────────────────────────────────────────
    dx = np.full(n, np.nan)
    for i in range(period, n):
        if sm_tr[i] > 0:
            di_p = 100.0 * sm_plus[i]  / sm_tr[i]
            di_m = 100.0 * sm_minus[i] / sm_tr[i]
            dsum = di_p + di_m
            if dsum > 0:
                dx[i] = 100.0 * abs(di_p - di_m) / dsum

    # ADX = Wilder smooth of DX, seeded at index `period * 2`
    seed_start = period
    seed_end   = period * 2
    if seed_end >= n:
        return result

    seed_vals = [dx[i] for i in range(seed_start, seed_end + 1) if not np.isnan(dx[i])]
    if len(seed_vals) < period:
        return result

    adx_val = sum(seed_vals[:period]) / period
    result[seed_end] = adx_val
    for i in range(seed_end + 1, n):
        if not np.isnan(dx[i]):
            adx_val = (adx_val * (period - 1) + dx[i]) / period
            result[i] = adx_val

    return result


def calculate_volume_ratio(volumes: List[float], period: int = 20) -> float:
    """Ratio of the latest candle's volume to the rolling average.

    Ratio > 1  → above-average volume (confirms moves).
    Ratio < 1  → below-average volume (signals may be weak).

    Args:
        volumes: Volume values (oldest first); must have at least 2 entries.
        period:  Number of past candles used for the average (default 20).

    Returns:
        Float ratio; returns 1.0 if data is insufficient or avg is zero.
    """
    if len(volumes) < 2:
        return 1.0
    # Use the previous `period` candles (exclude current) for the average.
    lookback = volumes[-period - 1: -1] if len(volumes) > period else volumes[:-1]
    avg = float(np.mean(lookback)) if lookback else 0.0
    current = volumes[-1]
    if avg == 0.0:
        return 1.0
    return float(current / avg)


def calculate_bollinger_bands(
    prices: List[float],
    period: int = 20,
    num_std: float = 2.0,
) -> Tuple[List[float], List[float], List[float]]:
    """Calculate Bollinger Bands (upper, middle/SMA, lower).

    Args:
        prices:   Closing prices (oldest first).
        period:   Look-back period for SMA and std (default 20).
        num_std:  Number of standard deviations (default 2.0).

    Returns:
        Tuple (upper, middle, lower) — each a list the same length as prices.
        Values before index period-1 are NaN.
    """
    arr    = np.array(prices, dtype=float)
    n      = len(arr)
    upper  = np.full(n, np.nan)
    middle = np.full(n, np.nan)
    lower  = np.full(n, np.nan)

    for i in range(period - 1, n):
        window     = arr[i - period + 1 : i + 1]
        sma        = window.mean()
        std        = window.std(ddof=0)          # population std, like most platforms
        middle[i]  = sma
        upper[i]   = sma + num_std * std
        lower[i]   = sma - num_std * std

    return upper.tolist(), middle.tolist(), lower.tolist()


def calculate_stochastic(
    highs:  List[float],
    lows:   List[float],
    closes: List[float],
    k_period: int = 5,
    d_period: int = 3,
    smooth_k: int = 3,
) -> Tuple[List[float], List[float]]:
    """Calculate Stochastic Oscillator (%K smoothed, %D).

    Args:
        highs:    High prices (oldest first).
        lows:     Low prices (oldest first).
        closes:   Close prices (oldest first).
        k_period: Raw %K look-back (default 5).
        d_period: %D smoothing period (default 3).
        smooth_k: %K smoothing period (default 3).

    Returns:
        Tuple (k_smooth, d) — each a list the same length as closes.
        Values before enough history are NaN.
    """
    n      = len(closes)
    raw_k  = np.full(n, np.nan)

    for i in range(k_period - 1, n):
        h_max = max(highs[i - k_period + 1 : i + 1])
        l_min = min(lows[i  - k_period + 1 : i + 1])
        denom = h_max - l_min
        raw_k[i] = 100.0 * (closes[i] - l_min) / denom if denom != 0 else 50.0

    # Smooth %K with simple moving average of smooth_k
    k_smooth = np.full(n, np.nan)
    for i in range(k_period - 1 + smooth_k - 1, n):
        window = raw_k[i - smooth_k + 1 : i + 1]
        if not np.any(np.isnan(window)):
            k_smooth[i] = window.mean()

    # %D = SMA(d_period) of smoothed %K
    d = np.full(n, np.nan)
    for i in range(len(k_smooth)):
        if i < d_period - 1:
            continue
        window = k_smooth[i - d_period + 1 : i + 1]
        if not np.any(np.isnan(window)):
            d[i] = window.mean()

    return k_smooth.tolist(), d.tolist()


def calculate_signals(
    closes: List[float],
    ema_fast_period: int = 50,
    ema_slow_period: int = 200,
    rsi_period: int = 14,
    rsi_overbought: float = 70.0,
    rsi_oversold: float = 30.0,
) -> dict:
    """Generate the latest EMA/RSI-based trading signal.

    Signal logic
    ------------
    BUY  : ema_fast > ema_slow  AND  50 < rsi < rsi_overbought
           AND  latest_close > ema_fast
    SELL : ema_fast < ema_slow  AND  rsi_oversold < rsi < 50
           AND  latest_close < ema_fast
    HOLD : all other cases

    Confidence is a linear function of how far RSI is from the 50
    mid-point relative to the trigger zone width, clipped to [0, 1].

    Args:
        closes: Ordered list of closing prices (oldest first).
        ema_fast_period: Fast EMA period.
        ema_slow_period: Slow EMA period.
        rsi_period: RSI period.
        rsi_overbought: Upper RSI boundary for signals.
        rsi_oversold: Lower RSI boundary for signals.

    Returns:
        Dict with keys: ``ema_fast``, ``ema_slow``, ``rsi``,
        ``signal`` ("buy" | "sell" | "hold"), ``confidence`` (float 0-1).

    Raises:
        ValueError: If there are not enough data points to compute any
                    indicator value.
    """
    if not closes:
        raise ValueError("closes must not be empty")

    ema_fast_vals = calculate_ema(closes, ema_fast_period)
    ema_slow_vals = calculate_ema(closes, ema_slow_period)
    rsi_vals = calculate_rsi(closes, rsi_period)

    ema_fast = ema_fast_vals[-1]
    ema_slow = ema_slow_vals[-1]
    rsi = rsi_vals[-1]
    price = closes[-1]

    if any(np.isnan(v) for v in (ema_fast, ema_slow, rsi)):
        return {
            "ema_fast": float("nan"),
            "ema_slow": float("nan"),
            "rsi": float("nan"),
            "signal": "hold",
            "confidence": 0.0,
        }

    signal = "hold"
    confidence = 0.0

    bullish_trend = ema_fast > ema_slow
    bearish_trend = ema_fast < ema_slow
    rsi_mid = 50.0

    if bullish_trend and rsi_mid < rsi < rsi_overbought and price > ema_fast:
        signal = "buy"
        # Confidence: how far RSI is into the buy zone (50 → rsi_overbought)
        zone_width = rsi_overbought - rsi_mid
        confidence = float(np.clip((rsi - rsi_mid) / zone_width, 0.0, 1.0))

    elif bearish_trend and rsi_oversold < rsi < rsi_mid and price < ema_fast:
        signal = "sell"
        # Confidence: how far RSI is into the sell zone (rsi_oversold → 50)
        zone_width = rsi_mid - rsi_oversold
        confidence = float(np.clip((rsi_mid - rsi) / zone_width, 0.0, 1.0))

    return {
        "ema_fast": float(ema_fast),
        "ema_slow": float(ema_slow),
        "rsi": float(rsi),
        "signal": signal,
        "confidence": confidence,
    }
