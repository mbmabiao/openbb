$jobs = @(
    @{ Symbol = "SPY";   LookbackYears = 5 }
    @{ Symbol = "QQQ";   LookbackYears = 5 }
    @{ Symbol = "IWM";   LookbackYears = 5 }
    @{ Symbol = "DIA";   LookbackYears = 5 }
    @{ Symbol = "XLK";   LookbackYears = 5 }
    @{ Symbol = "XLF";   LookbackYears = 5 }
    @{ Symbol = "XLE";   LookbackYears = 5 }
    @{ Symbol = "XLV";   LookbackYears = 5 }
    @{ Symbol = "XLI";   LookbackYears = 5 }
    @{ Symbol = "XLC";   LookbackYears = 5 }
    @{ Symbol = "XLY";   LookbackYears = 5 }
    @{ Symbol = "XLP";   LookbackYears = 5 }
    @{ Symbol = "XLU";   LookbackYears = 5 }
    @{ Symbol = "XLRE";  LookbackYears = 5 }
    @{ Symbol = "XLB";   LookbackYears = 5 }
    @{ Symbol = "BOTZ";  LookbackYears = 5 }
    # @{ Symbol = "AIQ";   LookbackYears = 5 }
    # @{ Symbol = "CHAT";  LookbackYears = 5 }
    # @{ Symbol = "SOXX";  LookbackYears = 5 }
    # @{ Symbol = "SMH";   LookbackYears = 5 }
    # @{ Symbol = "CIBR";  LookbackYears = 5 }
    # @{ Symbol = "HACK";  LookbackYears = 5 }
    # @{ Symbol = "WCLD";  LookbackYears = 5 }
    # @{ Symbol = "SKYY";  LookbackYears = 5 }
    # @{ Symbol = "CLOU";  LookbackYears = 5 }
    # @{ Symbol = "ROBO";  LookbackYears = 5 }
    # @{ Symbol = "ICLN";  LookbackYears = 5 }
    # @{ Symbol = "TAN";   LookbackYears = 5 }
    # @{ Symbol = "QCLN";  LookbackYears = 5 }
    # @{ Symbol = "URA";   LookbackYears = 5 }
    # @{ Symbol = "URNM";  LookbackYears = 5 }
    # @{ Symbol = "NLR";   LookbackYears = 5 }
    # @{ Symbol = "XBI";   LookbackYears = 5 }
    # @{ Symbol = "IBB";   LookbackYears = 5 }
    # @{ Symbol = "ITA";   LookbackYears = 5 }
    # @{ Symbol = "XAR";   LookbackYears = 5 }
    # @{ Symbol = "PPA";   LookbackYears = 5 }
    # @{ Symbol = "BITQ";  LookbackYears = 5 }
    # @{ Symbol = "BLOK";  LookbackYears = 5 }
    # @{ Symbol = "MAGS";  LookbackYears = 5 }
    # @{ Symbol = "IGV";   LookbackYears = 5 }
    # @{ Symbol = "IFRA";  LookbackYears = 5 }
    # @{ Symbol = "PAVE";  LookbackYears = 5 }
    # @{ Symbol = "XOP";   LookbackYears = 5 }
    # @{ Symbol = "AMLP";  LookbackYears = 5 }
    # @{ Symbol = "MLPA";  LookbackYears = 5 }
    # @{ Symbol = "CRAK";  LookbackYears = 5 }
    # @{ Symbol = "OIH";   LookbackYears = 5 }
    # @{ Symbol = "XES";   LookbackYears = 5 }
    # @{ Symbol = "VNQ";   LookbackYears = 5 }
    # @{ Symbol = "IYR";   LookbackYears = 5 }
    # @{ Symbol = "ITB";   LookbackYears = 5 }
    # @{ Symbol = "XHB";   LookbackYears = 5 }
    # @{ Symbol = "QUAL";  LookbackYears = 5 }
    # @{ Symbol = "MTUM";  LookbackYears = 5 }
    # @{ Symbol = "VLUE";  LookbackYears = 5 }
    # @{ Symbol = "VTV";   LookbackYears = 5 }
    # @{ Symbol = "USMV";  LookbackYears = 5 }
    # @{ Symbol = "SPLV";  LookbackYears = 5 }
    # @{ Symbol = "SIZE";  LookbackYears = 5 }
    # @{ Symbol = "VIG";   LookbackYears = 5 }
    # @{ Symbol = "SCHD";  LookbackYears = 5 }
    # @{ Symbol = "SDY";   LookbackYears = 5 }
    # @{ Symbol = "VUG";   LookbackYears = 5 }
    # @{ Symbol = "IWF";   LookbackYears = 5 }
    # @{ Symbol = "VB";    LookbackYears = 5 }
    # @{ Symbol = "SLY";   LookbackYears = 5 }
    # @{ Symbol = "IBUY";  LookbackYears = 5 }
    # @{ Symbol = "VCR";   LookbackYears = 5 }
    # @{ Symbol = "VIS";   LookbackYears = 5 }
    # @{ Symbol = "GDX";   LookbackYears = 5 }
    # @{ Symbol = "COPX";  LookbackYears = 5 }
    # @{ Symbol = "VPU";   LookbackYears = 5 }
    # @{ Symbol = "KRE";   LookbackYears = 5 }
    # @{ Symbol = "KBE";   LookbackYears = 5 }
    # @{ Symbol = "IGF";   LookbackYears = 5 }
    # @{ Symbol = "FDN";   LookbackYears = 5 }
    # @{ Symbol = "KIE";   LookbackYears = 5 }
    # @{ Symbol = "VDC";   LookbackYears = 5 }
    # @{ Symbol = "DRIV";  LookbackYears = 5 }
    # @{ Symbol = "IDRV";  LookbackYears = 5 }
    # @{ Symbol = "LIT";   LookbackYears = 5 }
    # @{ Symbol = "BATT";  LookbackYears = 5 }
    # @{ Symbol = "PPH";   LookbackYears = 5 }
    # @{ Symbol = "IHI";   LookbackYears = 5 }
    # @{ Symbol = "IHF";   LookbackYears = 5 }
)

foreach ($job in $jobs) {
    Write-Host "Running build_zone_snapshots.py for $($job.Symbol)..." -ForegroundColor Cyan

    python src\build_zone_snapshots.py `
        --symbol $job.Symbol `
        --lookback-years $job.LookbackYears `
        --reset
}