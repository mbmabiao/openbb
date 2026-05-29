# & "C:`Program Files (x86)`cloudflared`cloudflared.exe" tunnel --url http://localhost:8501

# python src\build_zone_snapshots.py `
# --symbol CL=F `
# --start-date 2024-04-28 `
# --end-date 2026-04-28 `
# --lookback-years 2 `
# --reset

python src\build_zone_snapshots.py `
--symbol GC=F `
--lookback-years 2 `
--reset

python src\build_zone_snapshots.py `
--symbol GLD `
--lookback-years 2 `
--reset

# python src\build_zone_snapshots.py `
# --symbol AAPL `
# --start-date 2024-04-28 `
# --end-date 2026-04-28 `
# --lookback-years 5 `
# --reset

python src\build_zone_snapshots.py `
--symbol NVDA `
--lookback-years 3 `
--reset

python src\build_zone_snapshots.py `
--symbol 3033.HK `
--lookback-years 3 `
--reset

python src\build_zone_snapshots.py `
--symbol JPM `
--start-date 2024-04-28 `
--lookback-years 5 `
--reset

python src\build_zone_snapshots.py `
--symbol BABA `
--lookback-years 2 `
--reset

python src\build_zone_snapshots.py `
--symbol TLT `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 5 `
--reset

python src\build_zone_snapshots.py `
--symbol GLD `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 5 `
--reset

python src\build_zone_snapshots.py `
--symbol AMZN `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 5 `
--reset

python src\build_zone_snapshots.py `
--symbol NIO `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 5 `
--reset

python src\build_zone_snapshots.py `
--symbol MA `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 5 `
--reset

python src\build_zone_snapshots.py `
--symbol GS `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 5 `
--reset


python src\build_zone_snapshots.py `
--symbol DIDIY `
--lookback-years 2 `
--reset

python src\build_zone_snapshots.py `
--symbol GLD `
--lookback-years 2 `
--reset

# --database-url sqlite:///outputs/zone_lifecycle.sqlite
# --provider yfinance 

#regime monitor
# streamlit run regime_monitor/run_visual.py

# streamlit run src/app.py

python .\buffett_quant_screen.py `
--symbols-csv .\sp500_constituents.csv

python .\buffett_quant_screen.py `
--tickers BABA
