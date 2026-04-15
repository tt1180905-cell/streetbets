"""
StreetBets - Dhan API Integration
Uses official dhanhq SDK: dhanhq(client_id, access_token)

SDK response structure (confirmed from live data):
  expiry_list → {"status": "success", "data": {"data": [...], "status": "success"}}
  option_chain → {"status": "success", "data": {"data": {"last_price": ..., "oc": {...}}, "status": "success"}}
  i.e. data is double-nested: resp["data"]["data"]
"""

import os
import logging
from datetime import date, timedelta
from typing import Optional

from dhanhq import dhanhq

logger = logging.getLogger(__name__)

DHAN_CLIENT_ID    = os.environ.get("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")

UNDERLYING_CONFIG = {
    "NIFTY": {
        "security_id": 13,
        "segment":     "IDX_I",
        "lot_size":    65,
        "expiry_day":  3,   # Thursday
    },
    "SENSEX": {
        "security_id": 51,
        "segment":     "IDX_I",
        "lot_size":    20,
        "expiry_day":  4,   # Friday
    },
}


def _client() -> dhanhq:
    return dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)


def _unwrap_data(resp: dict, underlying: str, call: str) -> any:
    """
    SDK wraps responses as resp["data"]["data"].
    Unwraps to the inner data payload.
    """
    if resp.get("status") != "success":
        raise RuntimeError(f"{call} failed for {underlying}: {str(resp)[:300]}")
    outer = resp.get("data", {})
    # Double-nested: {"data": {"data": <actual>, "status": "success"}}
    if isinstance(outer, dict) and "data" in outer:
        return outer["data"]
    # Single-nested: {"data": <actual>}
    return outer


def get_expiry_list(underlying: str) -> list:
    cfg  = UNDERLYING_CONFIG[underlying]
    dhan = _client()
    resp = dhan.expiry_list(
        under_security_id=cfg["security_id"],
        under_exchange_segment=cfg["segment"],
    )
    data = _unwrap_data(resp, underlying, "expiry_list")
    if not isinstance(data, list):
        raise RuntimeError(f"Expiry list unexpected format for {underlying}: {data}")
    return sorted(data)


def get_nearest_expiry(underlying: str) -> Optional[str]:
    expiries = get_expiry_list(underlying)
    today    = date.today().isoformat()
    future   = [e for e in expiries if e >= today]
    return future[0] if future else None


def get_option_chain(underlying: str, expiry: str) -> dict:
    """
    Returns the unwrapped chain dict:
    {"last_price": float, "oc": {strike: {"ce": {...}, "pe": {...}}}}
    """
    cfg  = UNDERLYING_CONFIG[underlying]
    dhan = _client()
    resp = dhan.option_chain(
        under_security_id=cfg["security_id"],
        under_exchange_segment=cfg["segment"],
        expiry=expiry,
    )
    data = _unwrap_data(resp, underlying, "option_chain")
    if "last_price" not in data or "oc" not in data:
        raise RuntimeError(f"Option chain unexpected format for {underlying}: {str(data)[:200]}")
    return data


def parse_chain(underlying: str, expiry: str, raw: dict, spot_range_pct: float = 0.05) -> dict:
    """
    raw = unwrapped chain dict from get_option_chain()
    i.e. raw["last_price"] and raw["oc"] directly accessible.
    """
    lot_size = UNDERLYING_CONFIG[underlying]["lot_size"]
    spot     = raw["last_price"]
    oc       = raw["oc"]
    lower    = spot * (1 - spot_range_pct)
    upper    = spot * (1 + spot_range_pct)

    contracts = []
    for strike_str, strike_data in oc.items():
        strike = float(strike_str)
        if not (lower <= strike <= upper):
            continue
        for opt_type in ("ce", "pe"):
            if opt_type not in strike_data:
                continue
            d   = strike_data[opt_type]
            ltp = d.get("last_price", 0.0) or 0.0
            if ltp <= 0:
                continue
            iv_raw = d.get("implied_volatility", 0.0) or 0.0
            iv     = iv_raw / 100.0 if iv_raw > 1.0 else iv_raw
            greeks = d.get("greeks", {})
            contracts.append({
                "security_id":     d.get("security_id"),
                "strike":          strike,
                "option_type":     opt_type.upper(),
                "ltp":             round(ltp, 2),
                "iv":              round(iv, 6),
                "bid":             d.get("top_bid_price", 0.0),
                "ask":             d.get("top_ask_price", 0.0),
                "oi":              d.get("oi", 0),
                "volume":          d.get("volume", 0),
                "delta":           greeks.get("delta"),
                "theta":           greeks.get("theta"),
                "gamma":           greeks.get("gamma"),
                "vega":            greeks.get("vega"),
                "lot_size":        lot_size,
                "invested_amount": round(ltp * lot_size, 2),
            })

    contracts.sort(key=lambda x: (x["strike"], x["option_type"]))
    logger.info(f"[Dhan] {underlying} @ {spot:.1f} | {len(contracts)} contracts within ±{spot_range_pct*100:.0f}%")
    return {"spot": spot, "expiry": expiry, "contracts": contracts}


def get_eod_ltp(underlying: str, expiry: str, security_ids: list) -> dict:
    try:
        raw  = get_option_chain(underlying, expiry)
        spot = raw["last_price"]
        oc   = raw["oc"]
        ltps = {}
        for strike_data in oc.values():
            for opt_type in ("ce", "pe"):
                if opt_type in strike_data:
                    d   = strike_data[opt_type]
                    sid = d.get("security_id")
                    if sid in security_ids:
                        ltps[sid] = d.get("last_price", 0.0)
        return {"spot": spot, "ltps": ltps}
    except Exception as e:
        logger.error(f"[Dhan] EOD fetch failed for {underlying}: {e}")
        return {"spot": None, "ltps": {}}


def is_trading_day(dt: date = None) -> bool:
    if dt is None:
        dt = date.today()
    return dt.weekday() < 5


def get_trading_days_between(start: date, end: date) -> list:
    days, current = [], start
    while current <= end:
        if is_trading_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for und in ["NIFTY", "SENSEX"]:
        expiry = get_nearest_expiry(und)
        print(f"{und} nearest expiry: {expiry}")
