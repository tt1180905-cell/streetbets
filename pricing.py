"""
StreetBets - Pricing Engine
Black-Scholes-Merton for non-expiry predictions.
Intrinsic value for expiry-day predictions.
T is always computed in minutes for precision.
"""

import math
from dataclasses import dataclass
from typing import Literal


RISK_FREE_RATE = 0.065          # ~6.5% annualised (Gsec)
TRADING_MINUTES_PER_DAY = 375   # 9:15 to 3:30
TRADING_DAYS_PER_YEAR = 252
MINUTES_PER_YEAR = TRADING_DAYS_PER_YEAR * TRADING_MINUTES_PER_DAY  # 94,500


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


@dataclass
class PricingResult:
    method: Literal["BSM", "INTRINSIC"]
    projected_ltp: float
    intrinsic_value: float
    time_value: float
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    iv_used: float | None = None
    t_minutes: float | None = None


def bsm_price(
    option_type: Literal["CE", "PE"],
    spot: float,
    strike: float,
    iv: float,                # annualised decimal e.g. 0.15
    t_minutes: float,         # minutes to EOD of prediction day
    risk_free: float = RISK_FREE_RATE,
) -> PricingResult:
    """
    Price an option using BSM.
    t_minutes = minutes from snapshot to EOD of the prediction day.
    """
    if t_minutes <= 0 or iv <= 0:
        return intrinsic_price(option_type, spot, strike)

    T = t_minutes / MINUTES_PER_YEAR          # fraction of year
    sqrt_T = math.sqrt(T)

    try:
        d1 = (math.log(spot / strike) + (risk_free + 0.5 * iv ** 2) * T) / (iv * sqrt_T)
        d2 = d1 - iv * sqrt_T
    except (ValueError, ZeroDivisionError):
        return intrinsic_price(option_type, spot, strike)

    if option_type == "CE":
        price = spot * _norm_cdf(d1) - strike * math.exp(-risk_free * T) * _norm_cdf(d2)
        delta = _norm_cdf(d1)
    else:
        price = strike * math.exp(-risk_free * T) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)
        delta = _norm_cdf(d1) - 1.0

    price = max(price, 0.0)

    gamma = _norm_pdf(d1) / (spot * iv * sqrt_T)
    vega  = spot * _norm_pdf(d1) * sqrt_T / 100.0   # per 1% IV change
    theta = (
        -(spot * _norm_pdf(d1) * iv) / (2.0 * sqrt_T)
        - risk_free * strike * math.exp(-risk_free * T) * (
            _norm_cdf(d2) if option_type == "CE" else _norm_cdf(-d2)
        )
    ) / TRADING_DAYS_PER_YEAR                         # per trading day

    intrinsic = max(spot - strike, 0.0) if option_type == "CE" else max(strike - spot, 0.0)

    return PricingResult(
        method="BSM",
        projected_ltp=round(price, 2),
        intrinsic_value=round(intrinsic, 2),
        time_value=round(max(price - intrinsic, 0.0), 2),
        delta=round(delta, 5),
        gamma=round(gamma, 6),
        theta=round(theta, 4),
        vega=round(vega, 4),
        iv_used=iv,
        t_minutes=t_minutes,
    )


def intrinsic_price(
    option_type: Literal["CE", "PE"],
    spot: float,
    strike: float,
) -> PricingResult:
    """
    Expiry settlement price = intrinsic value only.
    Used when prediction_day == expiry_day.
    """
    if option_type == "CE":
        intrinsic = max(spot - strike, 0.0)
    else:
        intrinsic = max(strike - spot, 0.0)

    return PricingResult(
        method="INTRINSIC",
        projected_ltp=round(intrinsic, 2),
        intrinsic_value=round(intrinsic, 2),
        time_value=0.0,
    )


def compute_pnl(
    projected_ltp: float,
    entry_ltp: float,
    lot_size: int,
) -> tuple[float, float]:
    """Returns (pnl_long, pnl_short) in rupees for one lot."""
    pnl_long  = round((projected_ltp - entry_ltp) * lot_size, 2)
    pnl_short = round((entry_ltp - projected_ltp) * lot_size, 2)
    return pnl_long, pnl_short


def select_iv_for_predicted_spot(
    predicted_spot: float,
    option_type: Literal["CE", "PE"],
    contracts: list[dict],
) -> float | None:
    """
    From the chain, find the IV of the strike closest to predicted_spot.
    contracts: list of dicts with keys 'strike', 'option_type', 'iv'
    Falls back to nearest available IV if exact type not found.
    """
    relevant = [
        c for c in contracts
        if c["option_type"] == option_type and c.get("iv") and c["iv"] > 0
    ]
    if not relevant:
        # fallback: any option type
        relevant = [c for c in contracts if c.get("iv") and c["iv"] > 0]
    if not relevant:
        return None

    closest = min(relevant, key=lambda c: abs(c["strike"] - predicted_spot))
    return closest["iv"]


def minutes_to_eod(
    snapshot_dt,          # datetime object (IST)
    prediction_date,      # date object
    eod_hour: int = 15,
    eod_minute: int = 30,
) -> float:
    """
    Compute trading minutes from snapshot_dt to EOD (3:30 PM) of prediction_date.
    Only counts minutes within trading hours (9:15–15:30 IST).
    For simplicity in POC: uses calendar minutes minus non-trading time.
    """
    from datetime import datetime, date, timedelta
    import pytz

    IST = pytz.timezone("Asia/Kolkata")

    if isinstance(snapshot_dt, datetime) and snapshot_dt.tzinfo is None:
        snapshot_dt = IST.localize(snapshot_dt)

    pred_eod = IST.localize(datetime(
        prediction_date.year, prediction_date.month, prediction_date.day,
        eod_hour, eod_minute, 0
    ))

    if snapshot_dt >= pred_eod:
        return 0.0

    # Count trading minutes between snapshot and pred EOD
    # Trading session: 9:15 to 15:30 = 375 minutes per day
    total_minutes = 0.0
    current = snapshot_dt

    while current.date() <= prediction_date:
        day_open  = IST.localize(datetime(current.year, current.month, current.day, 9, 15, 0))
        day_close = IST.localize(datetime(current.year, current.month, current.day, 15, 30, 0))

        if current.date() == prediction_date:
            day_close = pred_eod

        start = max(current, day_open)
        end   = min(day_close, pred_eod)

        if start < end:
            total_minutes += (end - start).total_seconds() / 60.0

        # Move to next day 9:15
        next_day = current.date() + timedelta(days=1)
        current  = IST.localize(datetime(next_day.year, next_day.month, next_day.day, 9, 15, 0))

        if current.date() > prediction_date:
            break

    return max(total_minutes, 0.0)


if __name__ == "__main__":
    # Quick sanity check
    result = bsm_price("CE", spot=24000, strike=24000, iv=0.12, t_minutes=375)
    print(f"ATM Call (1 day): ₹{result.projected_ltp} | Delta: {result.delta}")

    result2 = bsm_price("PE", spot=24000, strike=23500, iv=0.14, t_minutes=750)
    print(f"OTM Put (2 days): ₹{result2.projected_ltp} | Delta: {result2.delta}")

    result3 = intrinsic_price("CE", spot=24200, strike=24000)
    print(f"Expiry CE intrinsic: ₹{result3.projected_ltp}")

    pnl_l, pnl_s = compute_pnl(result.projected_ltp, 120.0, 65)
    print(f"Nifty long PnL: ₹{pnl_l} | short PnL: ₹{pnl_s}")
