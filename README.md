# Weather Trader

Detecting mispriced temperature brackets on Kalshi prediction markets.

## Stack
Python, pandas, scikit-learn, Open-Meteo API, NWS API, Jupyter

## Setup
1. Clone the repo
2. Create virtual environment: `py -3.11 -m venv venv`
3. Activate: `venv\Scripts\activate`
4. Install dependencies: `pip install -r requirements.txt`

## Structure
- `config/` — city config (coordinates, timezones, settlement stations)
- `src/` — pipeline scripts (ingest, features, model, signals, backtest)
- `notebooks/` — analysis and visualization
- `data/` — raw and processed data (gitignored)

## Progress
- [x] Project structure
- [x] City config (20 cities, Kalshi settlement stations)
- [x] Data ingestion (Open-Meteo historical + NWS forecast)
- [ ] Feature engineering