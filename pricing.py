"""
StreetBets - Pricing Engine
Black-Scholes-Merton for non-expiry predictions.
Intrinsic value for expiry-day predictions.

T CONVENTION (critical):
  Dhan's IV is calibrated using T = calendar_days / 365.
  We must use the same: T = calendar_seconds_to_expiry / (365 * 24 * 3600)

  For a prediction (spot=X, day=D):
    - The option still has life until expiry date, not until end of day D
    - T = calendar seconds from 9:15 AM of prediction day D to expiry close
    - This correctly prices what the option would be worth at day D open
      given that predicted spot, with full remaining life to expiry

  Why 9:15 of prediction day (not snapshot time)?
    All predictions are EOD snapshots — "what if spot is X at end of day D?"
    But we price the option at the START of day D (9:15 AM) to get a
    consistent T regardless of which snapshot generated the prediction.
    The entry LTP is from the snapshot, projected LTP uses start-of-day T.
"""

import math
from dataclasses import dataclass
from typing import Literal
from datetime import datetime, date, timedelta


RISK_FREE_RATE   = 0.065   # 6.5% p.a. (Indian Gsec)
SECS_PER_YEAR    = 365 * 24 * 3600   # 31,536,000 — calendar convention


def _norm_cdf(x: float) -> float:
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
    t_years: float | None = None


def bsm_price(
    option_type: Literal["CE", "PE"],
    spot: float,
    strike: float,
    iv: float,          # annualised decimal — calibrated to calendar days
    t_years: float,     # T = calendar_seconds_to_expiry / SECS_PER_YEAR
    risk_free: float = RISK_FREE_RATE,
) -> PricingResult:
    if t_years <= 0 or iv <= 0:
        return intrinsic_price(option_type, spot, strike)

    sqrt_T = math.sqrt(t_years)

    try:
        d1 = (math.log(spot / strike) + (risk_free + 0.5 * iv**2) * t_years) / (iv * sqrt_T)
        d2 = d1 - iv * sqrt_T
    except (ValueError, ZeroDivisionError):
        return intrinsic_price(option_type, spot, strike)

    if option_type == "CE":
        price = spot * _norm_cdf(d1) - strike * math.exp(-risk_free * t_years) * _norm_cdf(d2)
        delta = _norm_cdf(d1)
    else:
        price = strike * math.exp(-risk_free * t_years) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)
        delta = _norm_cdf(d1) - 1.0

    price = max(price, 0.0)

    gamma = _norm_pdf(d1) / (spot * iv * sqrt_T)
    vega  = spot * _norm_pdf(d1) * sqrt_T / 100.0
    theta = (
        -(spot * _norm_pdf(d1) * iv) / (2.0 * sqrt_T)
        - risk_free * strike * math.exp(-risk_free * t_years) * (
            _norm_cdf(d2) if option_type == "CE" else _norm_cdf(-d2)
        )
    ) / 365.0  # per calendar day

    intrinsic = (
        max(spot - strike, 0.0) if option_type == "CE"
        else max(strike - spot, 0.0)
    )

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
        t_years=t_years,
    )


def intrinsic_price(
    option_type: Literal["CE", "PE"],
    spot: float,
    strike: float,
) -> PricingResult:
    """Expiry settlement = intrinsic value only (T=0)."""
    intrinsic = (
        max(spot - strike, 0.0) if option_type == "CE"
        else max(strike - spot, 0.0)
    )
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


def t_years_for_prediction(
    prediction_date: date,
    expiry_date: date,
    expiry_close_hour: int = 15,
    expiry_close_minute: int = 30,
    market_open_hour: int = 9,
    market_open_minute: int = 15,
) -> float:
    """
    Compute T (in years, calendar convention) for a prediction on prediction_date.

    T = calendar seconds from 9:15 AM of prediction_date to expiry close / SECS_PER_YEAR

    Using start of prediction day (9:15 AM) as the pricing point so that
    all snapshots on the same day produce the same T for the same prediction_date.
    This gives a consistent "what is the option worth at start of day D?" price.

    Returns 0.0 if prediction_date >= expiry_date (use intrinsic instead).
    """
    if prediction_date >= expiry_date:
        return 0.0

    # Pricing point: 9:15 AM of prediction_date
    pricing_point = datetime(
        prediction_date.year, prediction_date.month, prediction_date.day,
        market_open_hour, market_open_minute, 0
    )
    # Expiry close: 3:30 PM of expiry_date
    expiry_close = datetime(
        expiry_date.year, expiry_date.month, expiry_date.day,
        expiry_close_hour, expiry_close_minute, 0
    )

    secs = (expiry_close - pricing_point).total_seconds()
    return max(secs / SECS_PER_YEAR, 0.0)


def select_iv_for_predicted_spot(
    predicted_spot: float,
    option_type: Literal["CE", "PE"],
    contracts: list[dict],
) -> float | None:
    """
    Find IV of the strike closest to predicted_spot for the given option_type.
    Falls back to nearest strike of either type if no match for specific type.
    """
    relevant = [
        c for c in contracts
        if c["option_type"] == option_type and c.get("iv") and c["iv"] > 0
    ]
    if not relevant:
        relevant = [c for c in contracts if c.get("iv") and c["iv"] > 0]
    if not relevant:
        return None
    return min(relevant, key=lambda c: abs(c["strike"] - predicted_spot))["iv"]


if __name__ == "__main__":
    from datetime import date

    # Sanity check: April 15 prediction, April 21 expiry
    pred_date   = date(2026, 4, 15)
    expiry_date = date(2026, 4, 21)

    T = t_years_for_prediction(pred_date, expiry_date)
    print(f"T (Apr15 → Apr21 expiry): {T:.6f} yr = {T*365:.3f} calendar days")

    # Should give ~₹254 for 23900 CE with IV=19.28%
    result = bsm_price("CE", spot=23900, strike=23900, iv=0.1928, t_years=T)
    print(f"23900 CE: ₹{result.projected_ltp} | delta={result.delta} (actual LTP was ₹259)")

    # Expiry day prediction
    T_expiry = t_years_for_prediction(expiry_date, expiry_date)
    print(f"\nT on expiry day: {T_expiry} → uses intrinsic")
    result2 = bsm_price("CE", spot=24200, strike=23900, iv=0.18, t_years=T_expiry)
    print(f"Expiry CE intrinsic (spot>strike): ₹{result2.projected_ltp}")
