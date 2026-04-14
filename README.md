# StreetBets — Options PnL Accuracy Lab

BSM prediction accuracy validator for Nifty and Sensex options.
Captures 5 snapshots/day, generates ±5% prediction matrices, reconciles at EOD.

## Structure

```
streetbets/
├── main.py         # Scheduler (APScheduler) — runs 5 snapshot jobs + EOD reconcile
├── api.py          # FastAPI dashboard backend
├── snapshot.py     # Snapshot job: fetch chain → store contracts → generate predictions
├── reconcile.py    # EOD job: fetch closing prices → compare projected vs actual PnL
├── pricing.py      # BSM + intrinsic value engine
├── dhan.py         # Dhan API integration (option chain, expiry list)
├── db.py           # SQLite schema + helpers
├── static/
│   └── index.html  # Dashboard (4 pages: Reconciliation, Accuracy, Insights, Setup)
├── requirements.txt
├── Procfile
├── railway.toml
└── .env.example
```

## Local Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Fill in DHAN_ACCESS_TOKEN and DHAN_CLIENT_ID

# Init DB and test pricing
python db.py
python pricing.py

# Start scheduler (runs at market times)
python main.py

# Start dashboard (separate terminal)
uvicorn api:app --reload --port 8000
# Open http://localhost:8000
```

## Railway Deployment

1. Push repo to GitHub
2. Create new Railway project → Deploy from GitHub repo
3. Add environment variables:
   - `DHAN_ACCESS_TOKEN`
   - `DHAN_CLIENT_ID`
   - `DB_PATH` = `/data/streetbets.db`
   - `LOG_PATH` = `/data/streetbets.log`
4. Railway auto-detects `railway.toml` and mounts `/data` as persistent volume
5. Add a second service for the worker:
   - Start command: `python main.py`
   - Same env vars + same volume mount

Both services share the `/data` volume — the worker writes SQLite, the web service reads it.

## Snapshot Schedule (IST)

| Time  | Label  | Purpose                    |
|-------|--------|----------------------------|
| 9:17  | 9:17   | Market open (post-auction) |
| 10:30 | 10:30  | Post-open volatility       |
| 12:15 | 12:15  | Midday                     |
| 13:30 | 1:30   | Post-lunch                 |
| 15:00 | 3:00   | Pre-close                  |
| 15:35 | —      | EOD reconciliation         |

## Pricing Logic

- **Non-expiry day**: Black-Scholes-Merton with strike-specific IV from chain
- **Expiry day predictions**: Intrinsic value only (max(S-K, 0) for calls, max(K-S, 0) for puts)
- **T**: Trading minutes from snapshot to EOD of prediction day
- **IV**: Nearest strike's IV to the predicted spot price
- **Risk-free rate**: 6.5% p.a. (Indian Gsec)

## Lot Sizes

- Nifty: 65
- Sensex: 20

## Accuracy Threshold

Default 10% of invested amount (LTP × lot size). Configurable on dashboard.

## Data Volume Estimate

~72K prediction rows/day across both underlyings (5 snapshots × ~100 contracts × 51 price points × avg 2.8 trading days DTE).
One full expiry cycle (~5 days): ~360K rows. SQLite handles this comfortably.
