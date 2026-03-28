"""
TP/SL Experiment Runner
=======================
Runs 10 backtest experiments (5 TP/SL combos × 2 signal_exit modes) against
the local backtest API and prints a comparison table with all required metrics.

Usage
-----
    # Make sure the backend is running first, then:
    python backend/scripts/run_tp_sl_experiments.py

    # Custom API base URL or token:
    python backend/scripts/run_tp_sl_experiments.py --base-url http://localhost:8000 --token <JWT>

    # Use a saved token from a file:
    python backend/scripts/run_tp_sl_experiments.py --token-file .token

Configuration
-------------
Edit BACKTEST_PARAMS below to change symbol, date range, capital, etc.
The TP/SL combinations and signal_exit modes are defined in EXPERIMENTS.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is not installed. Run: pip install requests")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Base parameters shared by all experiments
# ---------------------------------------------------------------------------
BACKTEST_PARAMS: Dict[str, Any] = {
    "symbol"          : "EURUSD",
    "timeframe"       : "1h",
    "start_date"      : "2024-01-01",
    "end_date"        : "2024-12-31",
    "initial_capital" : 10000.0,
    "ema_fast"        : 9,
    "ema_slow"        : 21,
    "rsi_period"      : 14,
    "rsi_overbought"  : 70.0,
    "rsi_oversold"    : 30.0,
    "commission_pct"  : 0.001,
    "position_size_pct": 0.05,
    "use_sentiment"   : True,
    "force_pct_tp_sl" : True,   # always True — we want pure pct TP/SL, no ATR override
}

# ---------------------------------------------------------------------------
# TP/SL combinations to test
# ---------------------------------------------------------------------------
TP_SL_COMBOS: List[Dict[str, float]] = [
    {"take_profit_pct": 0.03, "stop_loss_pct": 0.03},  # 1:1  R:R
    {"take_profit_pct": 0.04, "stop_loss_pct": 0.03},  # 1.33:1
    {"take_profit_pct": 0.05, "stop_loss_pct": 0.03},  # 1.67:1
    {"take_profit_pct": 0.06, "stop_loss_pct": 0.03},  # 2:1
    {"take_profit_pct": 0.05, "stop_loss_pct": 0.02},  # 2.5:1
]

# ---------------------------------------------------------------------------
# Polling config
# ---------------------------------------------------------------------------
POLL_INTERVAL_SEC  = 4
POLL_TIMEOUT_SEC   = 600   # 10 minutes max per backtest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _label(tp: float, sl: float, signal_exit: bool) -> str:
    se = "signal_exit=ON " if signal_exit else "signal_exit=OFF"
    return f"TP={tp:.0%} / SL={sl:.0%}  [{se}]"


def _run_backtest(
    session: requests.Session,
    base_url: str,
    params: Dict[str, Any],
) -> Optional[str]:
    """POST /api/v1/backtest/run and return run_id, or None on error."""
    url = f"{base_url}/api/v1/backtest/run"
    try:
        resp = session.post(url, json=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return str(data["id"])
    except requests.RequestException as exc:
        print(f"  ERROR launching backtest: {exc}")
        if hasattr(exc, "response") and exc.response is not None:
            print(f"  Server response: {exc.response.text[:400]}")
        return None


def _poll_backtest(
    session: requests.Session,
    base_url: str,
    run_id: str,
) -> Optional[Dict[str, Any]]:
    """Poll /{run_id}/status until completed or failed. Return results dict."""
    status_url  = f"{base_url}/api/v1/backtest/{run_id}/status"
    results_url = f"{base_url}/api/v1/backtest/{run_id}/results"
    deadline    = time.time() + POLL_TIMEOUT_SEC

    while time.time() < deadline:
        try:
            resp = session.get(status_url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status", "unknown")
            pct    = data.get("progress_pct", 0)
            print(f"  ... {status} ({pct}%)", end="\r", flush=True)

            if status == "completed":
                print()
                rresp = session.get(results_url, timeout=15)
                rresp.raise_for_status()
                return rresp.json().get("results", {})

            if status == "failed":
                print(f"\n  FAILED: {data.get('error_message', 'unknown error')}")
                return None

        except requests.RequestException as exc:
            print(f"\n  Polling error: {exc}")
            return None

        time.sleep(POLL_INTERVAL_SEC)

    print(f"\n  TIMEOUT after {POLL_TIMEOUT_SEC}s")
    return None


def _fmt_pct(v: Optional[float], decimals: int = 1) -> str:
    if v is None:
        return "N/A"
    return f"{v:.{decimals}f}%"


def _fmt_dollar(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    sign = "+" if v >= 0 else ""
    return f"{sign}${v:.4f}"


def _exit_breakdown(exit_reasons: Dict[str, int]) -> str:
    tp = exit_reasons.get("take_profit", 0)
    sl = exit_reasons.get("stop_loss", 0)
    se = exit_reasons.get("signal_exit", 0)
    eo = exit_reasons.get("end_of_data", 0)
    return f"TP:{tp} SL:{sl} SE:{se} EOD:{eo}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Run TP/SL backtest experiments")
    parser.add_argument("--base-url",   default="http://localhost:8000", help="API base URL")
    parser.add_argument("--token",      default=None,  help="Bearer JWT token")
    parser.add_argument("--token-file", default=None,  help="File containing a Bearer JWT token")
    args = parser.parse_args()

    # Resolve token
    token: Optional[str] = args.token
    if token is None and args.token_file:
        try:
            with open(args.token_file) as f:
                token = f.read().strip()
        except OSError as exc:
            print(f"ERROR reading token file: {exc}")
            sys.exit(1)

    if token is None:
        print("ERROR: --token or --token-file is required.")
        print("  Get a token by logging in via POST /api/v1/auth/login")
        print("  or pass: --token <your_jwt_token>")
        sys.exit(1)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    base_url = args.base_url.rstrip("/")

    # Build experiment list: 5 combos × 2 signal_exit modes = 10 runs
    experiments = []
    for combo in TP_SL_COMBOS:
        for use_se in (True, False):
            experiments.append({
                **BACKTEST_PARAMS,
                **combo,
                "use_signal_exit": use_se,
            })

    print("=" * 70)
    print("  TP/SL EXPERIMENT RUNNER")
    print(f"  {len(experiments)} runs  |  symbol={BACKTEST_PARAMS['symbol']}")
    print(f"  {BACKTEST_PARAMS['start_date']} -> {BACKTEST_PARAMS['end_date']}")
    print(f"  capital=${BACKTEST_PARAMS['initial_capital']:,.0f}  |  force_pct_tp_sl=True")
    print("=" * 70)

    results_table: List[Dict[str, Any]] = []

    for i, exp in enumerate(experiments, 1):
        tp = exp["take_profit_pct"]
        sl = exp["stop_loss_pct"]
        se = exp["use_signal_exit"]
        label = _label(tp, sl, se)

        print(f"\n[{i:02d}/{len(experiments)}] {label}")
        run_id = _run_backtest(session, base_url, exp)
        if run_id is None:
            results_table.append({"label": label, "results": None})
            continue

        print(f"  run_id={run_id}")
        results = _poll_backtest(session, base_url, run_id)
        results_table.append({"label": label, "tp": tp, "sl": sl, "signal_exit": se, "results": results})

    # ---------------------------------------------------------------------------
    # Print comparison table
    # ---------------------------------------------------------------------------
    print("\n")
    print("=" * 100)
    print("  RESULTS TABLE — 10 EXPERIMENTS")
    print("=" * 100)

    col_w = 38
    hdr = (
        f"{'Experiment':<{col_w}} "
        f"{'Trades':>6} "
        f"{'WinRate':>7} "
        f"{'PF':>5} "
        f"{'NetPnL':>10} "
        f"{'AvgWin':>8} "
        f"{'AvgLoss':>9} "
        f"{'MaxDD':>6} "
        f"{'Expect':>8}"
    )
    print(hdr)
    print("-" * 100)

    for row in results_table:
        r = row.get("results")
        if r is None:
            print(f"{row['label']:<{col_w}}  {'ERROR — no results':}")
            continue

        trades   = r.get("total_trades", 0)
        win_rate = r.get("win_rate", 0.0)
        pf       = r.get("profit_factor", 0.0)
        net_pnl  = r.get("net_pnl", 0.0)
        avg_win  = r.get("avg_win", 0.0)
        avg_loss = r.get("avg_loss", 0.0)
        max_dd   = r.get("max_drawdown", 0.0)
        expect   = r.get("expectancy_per_trade", 0.0)
        exits    = r.get("exit_reasons", {})

        line = (
            f"{row['label']:<{col_w}} "
            f"{trades:>6} "
            f"{win_rate:>6.1f}% "
            f"{pf:>5.2f} "
            f"{_fmt_dollar(net_pnl):>10} "
            f"{_fmt_dollar(avg_win):>8} "
            f"{_fmt_dollar(avg_loss):>9} "
            f"{max_dd:>5.1f}% "
            f"{_fmt_dollar(expect):>8}"
        )
        print(line)

    print("-" * 100)

    # ---------------------------------------------------------------------------
    # Exit reason breakdown
    # ---------------------------------------------------------------------------
    print("\n")
    print("=" * 80)
    print("  EXIT REASON BREAKDOWN")
    print("=" * 80)
    print(f"{'Experiment':<{col_w}}  {'TP':>5}  {'SL':>5}  {'SignalExit':>10}  {'EOD':>5}")
    print("-" * 80)
    for row in results_table:
        r = row.get("results")
        if r is None:
            continue
        exits = r.get("exit_reasons", {})
        tp_c  = exits.get("take_profit", 0)
        sl_c  = exits.get("stop_loss", 0)
        se_c  = exits.get("signal_exit", 0)
        eod_c = exits.get("end_of_data", 0)
        print(f"{row['label']:<{col_w}}  {tp_c:>5}  {sl_c:>5}  {se_c:>10}  {eod_c:>5}")
    print("-" * 80)

    # ---------------------------------------------------------------------------
    # Best combo analysis
    # ---------------------------------------------------------------------------
    print("\n")
    print("=" * 70)
    print("  BEST COMBINATION ANALYSIS")
    print("=" * 70)

    valid = [r for r in results_table if r.get("results")]
    if not valid:
        print("  No valid results to analyse.")
        return

    # Rank by expectancy_per_trade (primary), profit_factor (tiebreak)
    def _score(row: Dict) -> tuple:
        r = row["results"]
        return (
            r.get("expectancy_per_trade", -999),
            r.get("profit_factor", 0.0),
            r.get("net_pnl", 0.0),
        )

    ranked = sorted(valid, key=_score, reverse=True)
    best   = ranked[0]
    r      = best["results"]

    print(f"\n  WINNER: {best['label']}")
    print(f"    Expectancy/trade : {_fmt_dollar(r.get('expectancy_per_trade'))}")
    print(f"    Profit Factor    : {r.get('profit_factor', 0):.3f}")
    print(f"    Net PnL          : {_fmt_dollar(r.get('net_pnl'))}")
    print(f"    Win Rate         : {r.get('win_rate', 0):.1f}%")
    print(f"    Avg Win          : {_fmt_dollar(r.get('avg_win'))}")
    print(f"    Avg Loss         : {_fmt_dollar(r.get('avg_loss'))}")
    print(f"    Max Drawdown     : {r.get('max_drawdown', 0):.1f}%")
    print(f"    Exit breakdown   : {_exit_breakdown(r.get('exit_reasons', {}))}")

    # Signal-exit impact: compare same combo with SE on vs off
    print("\n  SIGNAL_EXIT IMPACT ANALYSIS:")
    print(f"  {'Combo':<28}  {'Expect ON':>10}  {'Expect OFF':>11}  {'Delta':>8}  {'Verdict'}")
    print("  " + "-" * 75)
    for combo in TP_SL_COMBOS:
        tp = combo["take_profit_pct"]
        sl = combo["stop_loss_pct"]
        row_on  = next((r for r in valid if r.get("tp") == tp and r.get("sl") == sl and r.get("signal_exit") is True),  None)
        row_off = next((r for r in valid if r.get("tp") == tp and r.get("sl") == sl and r.get("signal_exit") is False), None)
        if row_on and row_off:
            e_on  = row_on["results"].get("expectancy_per_trade", 0)
            e_off = row_off["results"].get("expectancy_per_trade", 0)
            delta = e_off - e_on
            verdict = "OFF better" if delta > 0.0001 else ("ON better" if delta < -0.0001 else "neutral")
            label  = f"TP={tp:.0%}/SL={sl:.0%}"
            print(f"  {label:<28}  {_fmt_dollar(e_on):>10}  {_fmt_dollar(e_off):>11}  {_fmt_dollar(delta):>8}  {verdict}")

    # Overall signal_exit verdict
    gains_from_off = sum(
        1 for combo in TP_SL_COMBOS
        for row_on in [next((r for r in valid if r.get("tp") == combo["take_profit_pct"] and r.get("sl") == combo["stop_loss_pct"] and r.get("signal_exit") is True), None)]
        for row_off in [next((r for r in valid if r.get("tp") == combo["take_profit_pct"] and r.get("sl") == combo["stop_loss_pct"] and r.get("signal_exit") is False), None)]
        if row_on and row_off
        and row_off["results"].get("expectancy_per_trade", 0) > row_on["results"].get("expectancy_per_trade", 0)
    )
    print(f"\n  signal_exit=OFF beats ON in {gains_from_off}/5 combos")
    if gains_from_off >= 4:
        print("  VERDICT: signal_exit is DESTROYING profitability — disable it.")
    elif gains_from_off >= 3:
        print("  VERDICT: signal_exit is HURTING profitability — consider disabling.")
    elif gains_from_off <= 1:
        print("  VERDICT: signal_exit is HELPING or neutral — keep it.")
    else:
        print("  VERDICT: signal_exit has mixed impact — context-dependent.")

    print("\n  (Ranking by expectancy_per_trade desc, profit_factor as tiebreak)")
    print("  Top 3:")
    for rank, row in enumerate(ranked[:3], 1):
        r = row["results"]
        print(f"    #{rank} {row['label']}")
        print(f"       expectancy={_fmt_dollar(r.get('expectancy_per_trade'))}  PF={r.get('profit_factor',0):.3f}  win_rate={r.get('win_rate',0):.1f}%")

    print("\n" + "=" * 70)
    print("  Done.")
    print("=" * 70)

    # Also dump full JSON to stdout for piping / saving
    print("\n\n--- RAW JSON (pipe to file if needed) ---")
    print(json.dumps(
        [{"label": r["label"], "results": r.get("results")} for r in results_table],
        indent=2,
    ))


if __name__ == "__main__":
    main()
