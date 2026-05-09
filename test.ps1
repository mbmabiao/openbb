# & "C:`Program Files (x86)`cloudflared`cloudflared.exe" tunnel --url http://localhost:8501

python src\build_zone_snapshots.py `
--symbol CL=F `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 2 `
--reset

python src\build_zone_snapshots.py `
--symbol GC=F `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 2 `
--reset

python src\build_zone_snapshots.py `
--symbol AAPL `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 2 `
--reset



python src\build_zone_snapshots.py `
--symbol AAPL `
--start-date 2024-04-28 `
--lookback-years 1 `
--reset


python src\build_zone_snapshots.py `
--symbol AAPL `
--start-date 2024-04-28 `
--lookback-years 2 `
--no-force

python src\build_zone_snapshots.py `
--symbol CQQQ `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 2 `
--reset

# python src\build_zone_snapshots.py `
# --symbol AMD `
# --start-date 2024-04-28 `
# --end-date 2026-04-28 `
# --lookback-years 2 `
# --reset

# python src\build_zone_snapshots.py `
# --symbol DIDIY `
# --start-date 2024-04-28 `
# --end-date 2026-04-28 `
# --lookback-years 2 `
# --reset

python src\build_zone_snapshots.py `
--symbol SPY `
--start-date 2024-04-28 `
--end-date 2026-04-28 `
--lookback-years 2 `
--reset


# --database-url sqlite:///outputs/zone_lifecycle.sqlite
# --provider yfinance 