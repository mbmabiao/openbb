$jobs = @(
    # # @{ Symbol = "MMM";  LookbackYears = 5 }
    # # @{ Symbol = "AOS";   LookbackYears = 5 }
    @{ Symbol = "NVDA";  LookbackYears = 5 }
    # # @{ Symbol = "KO";    LookbackYears = 5 }
    # # @{ Symbol = "JPM";   LookbackYears = 5 }
    # # @{ Symbol = "BABA";  LookbackYears = 5 }
    # # @{ Symbol = "ABT";   LookbackYears = 5 }
    # # @{ Symbol = "AMZN";  LookbackYears = 5 }
    # # @{ Symbol = "ADBE";   LookbackYears = 5 }
    # # @{ Symbol = "GOOGL";    LookbackYears = 5 }
    # # @{ Symbol = "DIDIY"; LookbackYears = 5 }
    # @{ Symbol = "TSLA";  LookbackYears = 5 }
    # @{ Symbol = "MRK";   LookbackYears = 5 }
    # @{ Symbol = "GILD";  LookbackYears = 5 }
    # @{ Symbol = "DECK";  LookbackYears = 5 }
    # @{ Symbol = "NFLX";  LookbackYears = 5 }
    # @{ Symbol = "LDOS";  LookbackYears = 5 }
    # @{ Symbol = "PG";    LookbackYears = 5 }
    # @{ Symbol = "ALLE";  LookbackYears = 5 }
    # @{ Symbol = "PNR";   LookbackYears = 5 }
)

foreach ($job in $jobs) {
    Write-Host "Running build_zone_snapshots.py for $($job.Symbol)..." -ForegroundColor Cyan

    python src\build_zone_snapshots.py `
        --symbol $job.Symbol `
        --lookback-years $job.LookbackYears `
        --reset
}