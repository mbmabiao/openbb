import argparse
import warnings
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")


# =========================================================
# USER CONFIG AREA
# =========================================================
# 以后主要改这里即可。
# English: Main editable parameters are kept here.

DEFAULT_YEARS = 5

# Tickers / Output
DEFAULT_OUTPUT_FILE = "buffett_quant_screen_debt_safety.csv"

# Model applicability
EXCLUDE_FINANCIAL_SECTOR = True
EXCLUDE_REAL_ESTATE_SECTOR = True

EXCLUDED_SECTORS = [
    "Financial Services",
    "Real Estate",
]

EXCLUDED_INDUSTRY_KEYWORDS = [
    "bank",
    "banks",
    "insurance",
    "asset management",
    "credit services",
    "reit",
    "real estate",
]

EXCLUDED_TICKERS = [
    "ALL",
    "HIG",
    "TRV",
    "CINF",
    "PGR",
    "CB",
    "AIZ",
    "WRB",
    "AFL",
]

# Quality thresholds
MIN_MEDIAN_ROIC = 0.12
MIN_MEDIAN_ROE = 0.15

# Debt safety thresholds
# 这次不再把 Debt / Equity 作为核心债务指标。
# 核心逻辑：
# 1. Interest Coverage: 先看利息压力
# 2. Net Debt / Owner Earnings: 再看真实现金收益还债能力
# 3. Net Debt / EBIT: 作为 owner earnings 缺失或异常时的备用指标
MIN_INTEREST_COVERAGE = 5.0
MAX_NET_DEBT_TO_OWNER_EARNINGS = 3.0
MAX_NET_DEBT_TO_EBIT = 3.0

# Owner earnings thresholds
MIN_OWNER_EARNINGS_TO_NET_INCOME = 0.70
MIN_OWNER_EARNINGS_YIELD = 0.04

# Capital allocation threshold
MIN_ONE_DOLLAR_TEST_RATIO = 1.0

# Tax assumption
DEFAULT_TAX_RATE = 0.21

# Maintenance Capex estimation:
# conservative: Maintenance Capex = Total Capex
# da_proxy: Maintenance Capex = Depreciation & Amortisation
MAINTENANCE_CAPEX_METHOD = "conservative"

# Scoring weights
QUALITY_ROIC_WEIGHT = 0.70
QUALITY_ROE_WEIGHT = 0.30

DEBT_INTEREST_COVERAGE_WEIGHT = 0.50
DEBT_NET_DEBT_OWNER_EARNINGS_WEIGHT = 0.35
DEBT_NET_DEBT_EBIT_WEIGHT = 0.15

CASH_OWNER_EARNINGS_TO_NET_INCOME_WEIGHT = 0.60
CASH_OWNER_EARNINGS_CONSISTENCY_WEIGHT = 0.40

FINAL_QUALITY_WEIGHT = 0.30
FINAL_DEBT_SAFETY_WEIGHT = 0.20
FINAL_CASH_WEIGHT = 0.25
FINAL_CAPITAL_ALLOCATION_WEIGHT = 0.15
FINAL_VALUATION_WEIGHT = 0.10

# Debug switch
# True: print raw yfinance interest rows and aligned df interest fields
# False: normal screening mode
DEBUG_INTEREST = False


# =========================================================
# Buffett-style Quant Framework
# =========================================================
# 目标：
# 1. ROIC: 判断企业本身投入资本的赚钱能力，作为主质量指标
# 2. ROE: 判断股东权益回报，作为辅助验证指标
# 3. Debt Safety: 用 Interest Coverage + Net Debt / Owner Earnings 判断债务安全
# 4. Owner Earnings: 判断真实可分配现金流
# 5. $1 Test: 判断管理层留存收益是否创造市值
# 6. Valuation: 用 Owner Earnings Yield 判断估值吸引力
#
# 重要变化：
# - 不再把 Debt / Equity 作为核心债务指标
# - Debt / Equity 容易被账面权益、回购、商誉减值扭曲
# - 新版优先看：
#   1) Interest Coverage: 经营利润覆盖利息的能力
#   2) Net Debt / Owner Earnings: 真实现金收益还债需要几年
#   3) Net Debt / EBIT: 备用偿债能力指标
#
# 注意：
# - yfinance 财务字段不同公司可能缺失
# - Owner Earnings 的 Maintenance Capex 无法精确取得
# - 本脚本默认用 Total Capex 作为保守估计
# - ROIC 中的 Invested Capital 使用：
#   Invested Capital = Total Debt + Stockholders Equity - Cash
# =========================================================


@dataclass
class BuffettConfig:
    years: int = DEFAULT_YEARS

    # Quality
    min_median_roic: float = MIN_MEDIAN_ROIC
    min_median_roe: float = MIN_MEDIAN_ROE

    # Debt safety
    min_interest_coverage: float = MIN_INTEREST_COVERAGE
    max_net_debt_to_owner_earnings: float = MAX_NET_DEBT_TO_OWNER_EARNINGS
    max_net_debt_to_ebit: float = MAX_NET_DEBT_TO_EBIT

    # Owner earnings
    min_owner_earnings_to_net_income: float = MIN_OWNER_EARNINGS_TO_NET_INCOME
    min_owner_earnings_yield: float = MIN_OWNER_EARNINGS_YIELD

    # $1 Test
    min_one_dollar_test_ratio: float = MIN_ONE_DOLLAR_TEST_RATIO

    # Tax / capex
    default_tax_rate: float = DEFAULT_TAX_RATE
    maintenance_capex_method: str = MAINTENANCE_CAPEX_METHOD


# =========================================================
# Helper functions
# =========================================================

def assess_model_applicability(sector: str, industry: str, ticker: str = "") -> tuple:
    """
    判断当前 Buffett-style owner earnings 模型是否适用于该公司。

    普通企业适用。
    金融股和 REIT/房地产不适用，因为：
    - debt / interest expense 对金融股意义不同
    - cash 对金融股不是普通可支配现金
    - REIT 应该看 FFO/AFFO，而不是 owner earnings
    """
    ticker = (ticker or "").strip().upper()
    sector = sector or ""
    industry = industry or ""
    excluded_sectors = set(EXCLUDED_SECTORS)

    if ticker in EXCLUDED_TICKERS:
        return 0, "excluded_ticker_permanent_list"

    if not EXCLUDE_FINANCIAL_SECTOR:
        excluded_sectors.discard("Financial Services")

    if not EXCLUDE_REAL_ESTATE_SECTOR:
        excluded_sectors.discard("Real Estate")

    if sector == "Financial Services" and sector in excluded_sectors:
        return 0, "excluded_financial_sector"

    if sector == "Real Estate" and sector in excluded_sectors:
        return 0, "excluded_real_estate_reit_sector"

    if sector in excluded_sectors:
        return 0, f"excluded_sector_{sector.lower().replace(' ', '_')}"

    industry_lower = industry.lower()

    for keyword in EXCLUDED_INDUSTRY_KEYWORDS:
        if keyword in industry_lower:
            note_keyword = keyword.replace(" ", "_")
            return 0, f"excluded_industry_{note_keyword}"

    return 100, "applicable_operating_company"


def safe_get(df: pd.DataFrame, possible_names: List[str]) -> Optional[pd.Series]:
    """
    从 yfinance 财务表中读取字段。
    不同公司/不同市场字段名可能不完全一致，所以用候选字段列表。
    """
    if df is None or df.empty:
        return None

    for name in possible_names:
        if name in df.index:
            return df.loc[name]

    return None


def clean_series(s: Optional[pd.Series]) -> Optional[pd.Series]:
    if s is None:
        return None

    s = s.copy()
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()
    s = pd.to_numeric(s, errors="coerce")
    return s.dropna()


def latest_n(s: Optional[pd.Series], n: int) -> Optional[pd.Series]:
    s = clean_series(s)
    if s is None or s.empty:
        return None
    return s.tail(n)


def safe_divide(a, b):
    if b is None or pd.isna(b) or b == 0:
        return np.nan
    return a / b


def safe_interest_coverage(ebit_value, interest_expense_abs):
    """
    Interest Coverage = EBIT / abs(Interest Expense)

    Important:
    - Missing interest expense should NOT become infinity.
    - Zero or invalid interest expense should become NaN and be explained by note.
    """
    if pd.isna(ebit_value):
        return np.nan

    if pd.isna(interest_expense_abs):
        return np.nan

    if interest_expense_abs <= 0:
        return np.nan

    return ebit_value / interest_expense_abs


def pick_effective_interest_coverage(latest_value, median_value):
    """
    Use latest interest coverage first.
    If latest is missing, use median historical interest coverage.
    """
    if not pd.isna(latest_value):
        return latest_value

    if not pd.isna(median_value):
        return median_value

    return np.nan


def debt_ratio(numerator, denominator):
    """
    债务偿还年数类指标。
    如果 net debt <= 0，说明净现金，直接给 0。
    如果 denominator <= 0，说明没有偿债利润/现金流，返回 NaN。
    """
    if numerator is None or pd.isna(numerator):
        return np.nan

    if numerator <= 0:
        return 0.0

    if denominator is None or pd.isna(denominator) or denominator <= 0:
        return np.nan

    return numerator / denominator


def bounded_tax_rate(tax_rate: float, default_tax_rate: float = DEFAULT_TAX_RATE) -> float:
    """
    避免异常税率污染 ROIC。
    """
    if pd.isna(tax_rate):
        return default_tax_rate

    if tax_rate < 0:
        return default_tax_rate

    if tax_rate > 0.50:
        return default_tax_rate

    return tax_rate


def score_threshold(value, good_level, higher_is_better=True):
    """
    简单阈值打分。
    higher_is_better=True: value >= good_level 得 100
    higher_is_better=False: value <= good_level 得 100
    """
    if pd.isna(value):
        return np.nan

    if value == np.inf:
        return 100 if higher_is_better else np.nan

    if higher_is_better:
        if value >= good_level:
            return 100
        if value <= 0:
            return 0
        return max(0, min(100, value / good_level * 100))

    if value <= good_level:
        return 100
    if value <= 0:
        return 100

    return max(0, min(100, good_level / value * 100))


def weighted_score(items: List[tuple]) -> float:
    """
    items: [(score, weight), ...]
    自动跳过 NaN score，并重新归一化权重。
    """
    valid_items = [(s, w) for s, w in items if not pd.isna(s)]

    if not valid_items:
        return np.nan

    total_weight = sum(w for _, w in valid_items)

    if total_weight == 0:
        return np.nan

    return sum(s * w for s, w in valid_items) / total_weight


def get_close_near_date(ticker: str, date: pd.Timestamp) -> Optional[float]:
    """
    获取某个财报日期附近的股价。
    用于估算历史 market cap。
    """
    start = date - pd.Timedelta(days=10)
    end = date + pd.Timedelta(days=10)

    hist = yf.download(
        ticker,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        progress=False,
        auto_adjust=True
    )

    if hist is None or hist.empty:
        return None

    if isinstance(hist.columns, pd.MultiIndex):
        close = hist["Close"].iloc[:, 0]
    else:
        close = hist["Close"]

    close = close.dropna()

    if close.empty:
        return None

    return float(close.iloc[-1])


# =========================================================
# Core analysis
# =========================================================

def analyse_ticker(ticker: str, config: BuffettConfig) -> Dict:
    tk = yf.Ticker(ticker)
    info = tk.info

    company_name = info.get("longName", "")
    sector = info.get("sector", "")
    industry = info.get("industry", "")

    model_applicability_score, model_applicability_note = assess_model_applicability(
        sector,
        industry,
        ticker
    )

    result = {
        "ticker": ticker,
        "status": "ok",
        "company_name": company_name,
        "sector": sector,
        "industry": industry,
        "model_applicability_score": model_applicability_score,
        "model_applicability_note": model_applicability_note,
    }

    if model_applicability_score == 0:
        result["status"] = "skipped_model_not_applicable"
        return result

    income = tk.financials
    balance = tk.balance_sheet
    cashflow = tk.cashflow

    # -----------------------------------------------------
    # 1. 读取核心财务字段
    # -----------------------------------------------------
    net_income = latest_n(
        safe_get(income, [
            "Net Income",
            "Net Income Common Stockholders",
            "Net Income Continuous Operations"
        ]),
        config.years
    )

    revenue = latest_n(
        safe_get(income, [
            "Total Revenue",
            "Operating Revenue"
        ]),
        config.years
    )

    ebit = latest_n(
        safe_get(income, [
            "EBIT",
            "Operating Income",
            "Normalized EBIT"
        ]),
        config.years
    )

    interest_expense = latest_n(
        safe_get(income, [
            "Interest Expense",
            "Interest Expense Non Operating",
            "Interest Expense Operating",
            "Total Interest Expense"
        ]),
        config.years
    )

    pretax_income = latest_n(
        safe_get(income, [
            "Pretax Income",
            "Income Before Tax",
            "Earnings Before Tax"
        ]),
        config.years
    )

    tax_provision = latest_n(
        safe_get(income, [
            "Tax Provision",
            "Income Tax Expense"
        ]),
        config.years
    )

    equity = latest_n(
        safe_get(balance, [
            "Stockholders Equity",
            "Total Stockholder Equity",
            "Common Stock Equity"
        ]),
        config.years
    )

    total_debt = latest_n(
        safe_get(balance, [
            "Total Debt",
            "Long Term Debt",
            "Current Debt"
        ]),
        config.years
    )

    net_debt_reported = latest_n(
        safe_get(balance, [
            "Net Debt"
        ]),
        config.years
    )

    cash = latest_n(
        safe_get(balance, [
            "Cash And Cash Equivalents",
            "Cash Cash Equivalents And Short Term Investments",
            "Cash Financial",
            "Cash Equivalents"
        ]),
        config.years
    )

    total_assets = latest_n(
        safe_get(balance, [
            "Total Assets"
        ]),
        config.years
    )

    current_liabilities = latest_n(
        safe_get(balance, [
            "Current Liabilities",
            "Total Current Liabilities"
        ]),
        config.years
    )

    depreciation = latest_n(
        safe_get(cashflow, [
            "Depreciation And Amortization",
            "Depreciation",
            "Depreciation Amortization Depletion"
        ]),
        config.years
    )

    capex = latest_n(
        safe_get(cashflow, [
            "Capital Expenditure",
            "Capital Expenditures"
        ]),
        config.years
    )

    dividends_paid = latest_n(
        safe_get(cashflow, [
            "Cash Dividends Paid",
            "Common Stock Dividend Paid"
        ]),
        config.years
    )

    shares_outstanding = latest_n(
        safe_get(balance, [
            "Ordinary Shares Number",
            "Share Issued",
            "Common Stock Shares Outstanding"
        ]),
        config.years
    )

    if net_income is None or equity is None:
        result["status"] = "missing_core_financials"
        return result

    # -----------------------------------------------------
    # 2. 对齐日期
    # -----------------------------------------------------
    df = pd.DataFrame({
        "net_income": net_income,
        "equity": equity
    })

    optional_series = {
        "revenue": revenue,
        "ebit": ebit,
        "interest_expense": interest_expense,
        "pretax_income": pretax_income,
        "tax_provision": tax_provision,
        "total_debt": total_debt,
        "net_debt_reported": net_debt_reported,
        "cash": cash,
        "total_assets": total_assets,
        "current_liabilities": current_liabilities,
        "depreciation": depreciation,
        "capex": capex,
        "dividends_paid": dividends_paid,
        "shares_outstanding": shares_outstanding,
    }

    for col, series in optional_series.items():
        if series is not None:
            df[col] = series

    df = df.sort_index().dropna(subset=["net_income", "equity"])

    if len(df) < 2:
        result["status"] = "not_enough_years"
        return result

    # -----------------------------------------------------
    # 3. ROE：辅助指标
    # -----------------------------------------------------
    df["avg_equity"] = df["equity"].rolling(2).mean()
    df["roe"] = df["net_income"] / df["avg_equity"]

    median_roe = df["roe"].median()
    latest_roe = df["roe"].iloc[-1]
    roe_volatility = df["roe"].std()

    # -----------------------------------------------------
    # 4. ROIC：主质量指标
    # -----------------------------------------------------
    if "ebit" not in df.columns:
        df["ebit"] = df["net_income"]
        roic_method_note = "fallback_net_income_as_ebit"
    else:
        roic_method_note = "ebit_based"

    if "pretax_income" in df.columns and "tax_provision" in df.columns:
        df["tax_rate"] = df.apply(
            lambda row: safe_divide(row["tax_provision"], row["pretax_income"]),
            axis=1
        )
        df["tax_rate"] = df["tax_rate"].apply(
            lambda x: bounded_tax_rate(x, config.default_tax_rate)
        )
    else:
        df["tax_rate"] = config.default_tax_rate

    df["nopat"] = df["ebit"] * (1 - df["tax_rate"])

    if "total_debt" in df.columns:
        debt_for_ic = df["total_debt"].fillna(0)
    else:
        debt_for_ic = 0

    if "cash" in df.columns:
        cash_for_ic = df["cash"].fillna(0)
    else:
        cash_for_ic = 0

    df["invested_capital"] = debt_for_ic + df["equity"] - cash_for_ic

    if "total_assets" in df.columns and "current_liabilities" in df.columns:
        fallback_ic = df["total_assets"] - df["current_liabilities"] - cash_for_ic
        df["invested_capital"] = df["invested_capital"].where(
            df["invested_capital"] > 0,
            fallback_ic
        )

    df.loc[df["invested_capital"] <= 0, "invested_capital"] = np.nan

    df["avg_invested_capital"] = df["invested_capital"].rolling(2).mean()
    df["roic"] = df["nopat"] / df["avg_invested_capital"]

    median_roic = df["roic"].median()
    latest_roic = df["roic"].iloc[-1]
    roic_volatility = df["roic"].std()

    # -----------------------------------------------------
    # 5. Owner Earnings
    # -----------------------------------------------------
    if "depreciation" not in df.columns:
        df["depreciation"] = 0

    if "capex" not in df.columns:
        df["capex"] = 0
        capex_note = "capex_missing_assumed_zero"
    else:
        capex_note = "capex_available"

    df["capex_abs"] = df["capex"].abs()

    if config.maintenance_capex_method == "da_proxy":
        df["maintenance_capex"] = df["depreciation"].abs()
    else:
        df["maintenance_capex"] = df["capex_abs"]

    df["owner_earnings"] = (
        df["net_income"]
        + df["depreciation"].abs()
        - df["maintenance_capex"]
    )

    latest_owner_earnings = df["owner_earnings"].iloc[-1]
    median_owner_earnings = df["owner_earnings"].median()

    latest_net_income = df["net_income"].iloc[-1]
    owner_earnings_to_net_income = safe_divide(latest_owner_earnings, latest_net_income)

    positive_owner_earnings_years = int((df["owner_earnings"] > 0).sum())
    owner_earnings_positive_ratio = positive_owner_earnings_years / len(df)

    # -----------------------------------------------------
    # 6. Debt Safety：新版核心债务逻辑
    # -----------------------------------------------------
    # Interest Coverage = EBIT / abs(Interest Expense)
    # Net Debt = Total Debt - Cash，优先使用公司披露的 Net Debt
    # Net Debt / Owner Earnings = 净债务 / 真实现金收益
    # Net Debt / EBIT = 净债务 / EBIT，作为备用
    # -----------------------------------------------------
    if DEBUG_INTEREST:
        print("\n========== Income Statement Index ==========")
        print(income.index.tolist() if income is not None else "income is None")

        print("\n========== Interest-related rows in raw income ==========")
        if income is not None and not income.empty:
            found_interest_row = False
            for idx in income.index:
                if "interest" in idx.lower():
                    found_interest_row = True
                    print("\nROW:", idx)
                    print(income.loc[idx])
            if not found_interest_row:
                print("No interest-related rows found in raw income statement.")

        print("\n========== Columns currently in aligned df ==========")
        print(df.columns.tolist())

        print("\n========== EBIT / Interest fields BEFORE coverage ==========")
        debug_cols = [c for c in ["ebit", "interest_expense"] if c in df.columns]
        if debug_cols:
            print(df[debug_cols])
        else:
            print("No EBIT or interest_expense columns found in df.")

    if "interest_expense" in df.columns:
        df["interest_expense_abs"] = df["interest_expense"].abs()
    else:
        df["interest_expense_abs"] = np.nan

    df["interest_coverage"] = df.apply(
        lambda row: safe_interest_coverage(row["ebit"], row["interest_expense_abs"]),
        axis=1
    )

    if "net_debt_reported" in df.columns:
        df["net_debt"] = df["net_debt_reported"]
        net_debt_method_note = "reported_net_debt"
    elif "total_debt" in df.columns:
        df["net_debt"] = df["total_debt"] - cash_for_ic
        net_debt_method_note = "total_debt_minus_cash"
    else:
        df["net_debt"] = np.nan
        net_debt_method_note = "net_debt_missing"

    latest_net_debt = df["net_debt"].iloc[-1]
    latest_interest_expense_abs = df["interest_expense_abs"].iloc[-1]
    latest_interest_coverage = df["interest_coverage"].iloc[-1]
    median_interest_coverage = df["interest_coverage"].median()
    effective_interest_coverage = pick_effective_interest_coverage(
        latest_interest_coverage,
        median_interest_coverage
    )

    if "interest_expense" not in df.columns:
        interest_coverage_note = "interest_expense_column_missing"
    elif pd.isna(df["interest_expense"].iloc[-1]):
        interest_coverage_note = "latest_interest_expense_missing_using_median_if_available"
    elif pd.isna(latest_interest_coverage):
        interest_coverage_note = "latest_interest_coverage_invalid_using_median_if_available"
    else:
        interest_coverage_note = "interest_expense_available"

    if DEBUG_INTEREST:
        print("\n========== EBIT / Interest fields AFTER coverage ==========")
        cols = ["ebit", "interest_expense", "interest_expense_abs", "interest_coverage"]
        print(df[cols])

        print("\n========== Interest Coverage Summary ==========")
        print("latest_interest_expense_abs:", latest_interest_expense_abs)
        print("latest_interest_coverage:", latest_interest_coverage)
        print("median_interest_coverage:", median_interest_coverage)
        print("effective_interest_coverage:", effective_interest_coverage)
        print("interest_coverage_note:", interest_coverage_note)

    latest_net_debt_to_owner_earnings = debt_ratio(
        latest_net_debt,
        latest_owner_earnings
    )

    latest_net_debt_to_ebit = debt_ratio(
        latest_net_debt,
        df["ebit"].iloc[-1]
    )

    # 保留 Debt / Equity 作为展示字段，不参与核心打分与 pass/fail。
    if "total_debt" in df.columns:
        latest_debt_to_equity = safe_divide(df["total_debt"].iloc[-1], df["equity"].iloc[-1])
    else:
        latest_debt_to_equity = np.nan

    # -----------------------------------------------------
    # 7. 当前市值与 Owner Earnings Yield
    # -----------------------------------------------------
    market_cap = info.get("marketCap", np.nan)
    current_price = info.get("currentPrice", np.nan)

    if pd.isna(market_cap) or market_cap is None:
        shares_current = info.get("sharesOutstanding", np.nan)
        if not pd.isna(current_price) and not pd.isna(shares_current):
            market_cap = current_price * shares_current

    owner_earnings_yield = safe_divide(latest_owner_earnings, market_cap)
    price_to_owner_earnings = safe_divide(market_cap, latest_owner_earnings)

    # -----------------------------------------------------
    # 8. $1 Test
    # -----------------------------------------------------
    cumulative_net_income = df["net_income"].sum()

    if "dividends_paid" in df.columns:
        cumulative_dividends = df["dividends_paid"].abs().sum()
    else:
        cumulative_dividends = 0

    retained_earnings_proxy = cumulative_net_income - cumulative_dividends

    first_date = df.index[0]
    first_price = get_close_near_date(ticker, first_date)

    if "shares_outstanding" in df.columns:
        first_shares = df["shares_outstanding"].iloc[0]
    else:
        first_shares = info.get("sharesOutstanding", np.nan)

    if first_price is not None and not pd.isna(first_shares):
        beginning_market_cap = first_price * first_shares
    else:
        beginning_market_cap = np.nan

    market_value_increase = market_cap - beginning_market_cap
    one_dollar_test_ratio = safe_divide(market_value_increase, retained_earnings_proxy)

    # -----------------------------------------------------
    # 9. Scoring
    # -----------------------------------------------------
    roic_score = score_threshold(
        median_roic,
        config.min_median_roic,
        higher_is_better=True
    )

    roe_score = score_threshold(
        median_roe,
        config.min_median_roe,
        higher_is_better=True
    )

    interest_coverage_score = score_threshold(
        effective_interest_coverage,
        config.min_interest_coverage,
        higher_is_better=True
    )

    net_debt_owner_earnings_score = score_threshold(
        latest_net_debt_to_owner_earnings,
        config.max_net_debt_to_owner_earnings,
        higher_is_better=False
    )

    net_debt_ebit_score = score_threshold(
        latest_net_debt_to_ebit,
        config.max_net_debt_to_ebit,
        higher_is_better=False
    )

    owner_earnings_score = score_threshold(
        owner_earnings_to_net_income,
        config.min_owner_earnings_to_net_income,
        higher_is_better=True
    )

    owner_earnings_consistency_score = owner_earnings_positive_ratio * 100

    one_dollar_score = score_threshold(
        one_dollar_test_ratio,
        config.min_one_dollar_test_ratio,
        higher_is_better=True
    )

    valuation_score = score_threshold(
        owner_earnings_yield,
        config.min_owner_earnings_yield,
        higher_is_better=True
    )

    quality_score = weighted_score([
        (roic_score, QUALITY_ROIC_WEIGHT),
        (roe_score, QUALITY_ROE_WEIGHT),
    ])

    debt_safety_score = weighted_score([
        (interest_coverage_score, DEBT_INTEREST_COVERAGE_WEIGHT),
        (net_debt_owner_earnings_score, DEBT_NET_DEBT_OWNER_EARNINGS_WEIGHT),
        (net_debt_ebit_score, DEBT_NET_DEBT_EBIT_WEIGHT),
    ])

    cash_score = weighted_score([
        (owner_earnings_score, CASH_OWNER_EARNINGS_TO_NET_INCOME_WEIGHT),
        (owner_earnings_consistency_score, CASH_OWNER_EARNINGS_CONSISTENCY_WEIGHT),
    ])

    capital_allocation_score = one_dollar_score

    final_score = weighted_score([
        (quality_score, FINAL_QUALITY_WEIGHT),
        (debt_safety_score, FINAL_DEBT_SAFETY_WEIGHT),
        (cash_score, FINAL_CASH_WEIGHT),
        (capital_allocation_score, FINAL_CAPITAL_ALLOCATION_WEIGHT),
        (valuation_score, FINAL_VALUATION_WEIGHT),
    ])

    # -----------------------------------------------------
    # 10. Pass / Fail flags
    # -----------------------------------------------------
    pass_roic = bool(not pd.isna(median_roic) and median_roic >= config.min_median_roic)

    # ROE 是辅助验证，不作为 overall_pass 的必要条件
    pass_roe_supportive = bool(not pd.isna(median_roe) and median_roe >= config.min_median_roe)

    pass_interest_coverage = bool(
        not pd.isna(effective_interest_coverage)
        and effective_interest_coverage >= config.min_interest_coverage
    )

    pass_net_debt_to_owner_earnings = bool(
        not pd.isna(latest_net_debt_to_owner_earnings)
        and latest_net_debt_to_owner_earnings <= config.max_net_debt_to_owner_earnings
    )

    pass_net_debt_to_ebit = bool(
        not pd.isna(latest_net_debt_to_ebit)
        and latest_net_debt_to_ebit <= config.max_net_debt_to_ebit
    )

    # 核心债务通过逻辑：
    # - Interest Coverage 必须过
    # - Net Debt / Owner Earnings 优先过
    # - 如果 Owner Earnings 指标缺失，再允许用 Net Debt / EBIT 兜底
    if not pd.isna(latest_net_debt_to_owner_earnings):
        pass_debt_safety = pass_interest_coverage and pass_net_debt_to_owner_earnings
    else:
        pass_debt_safety = pass_interest_coverage and pass_net_debt_to_ebit

    pass_owner_earnings = bool(
        not pd.isna(owner_earnings_to_net_income)
        and owner_earnings_to_net_income >= config.min_owner_earnings_to_net_income
    )

    pass_one_dollar_test = bool(
        not pd.isna(one_dollar_test_ratio)
        and one_dollar_test_ratio >= config.min_one_dollar_test_ratio
    )

    pass_valuation = bool(
        not pd.isna(owner_earnings_yield)
        and owner_earnings_yield >= config.min_owner_earnings_yield
    )

    overall_pass = all([
        pass_roic,
        pass_debt_safety,
        pass_owner_earnings,
        pass_one_dollar_test,
        pass_valuation
    ])

    # -----------------------------------------------------
    # 11. Result
    # -----------------------------------------------------
    result.update({
        "years_used": len(df),

        # ROIC
        "latest_roic": latest_roic,
        "median_roic": median_roic,
        "roic_volatility": roic_volatility,
        "latest_nopat": df["nopat"].iloc[-1],
        "latest_invested_capital": df["invested_capital"].iloc[-1],
        "roic_method_note": roic_method_note,

        # ROE
        "latest_roe": latest_roe,
        "median_roe": median_roe,
        "roe_volatility": roe_volatility,

        # Debt safety
        "latest_total_debt": df["total_debt"].iloc[-1] if "total_debt" in df.columns else np.nan,
        "latest_cash": df["cash"].iloc[-1] if "cash" in df.columns else np.nan,
        "latest_net_debt": latest_net_debt,
        "latest_interest_expense_abs": latest_interest_expense_abs,
        "latest_interest_coverage": latest_interest_coverage,
        "median_interest_coverage": median_interest_coverage,
        "effective_interest_coverage": effective_interest_coverage,
        "interest_coverage_note": interest_coverage_note,
        "latest_net_debt_to_owner_earnings": latest_net_debt_to_owner_earnings,
        "latest_net_debt_to_ebit": latest_net_debt_to_ebit,
        "latest_debt_to_equity_reference": latest_debt_to_equity,
        "net_debt_method_note": net_debt_method_note,

        # Owner earnings
        "latest_net_income": latest_net_income,
        "latest_owner_earnings": latest_owner_earnings,
        "median_owner_earnings": median_owner_earnings,
        "owner_earnings_to_net_income": owner_earnings_to_net_income,
        "owner_earnings_positive_ratio": owner_earnings_positive_ratio,
        "capex_note": capex_note,

        # Valuation
        "market_cap": market_cap,
        "owner_earnings_yield": owner_earnings_yield,
        "price_to_owner_earnings": price_to_owner_earnings,

        # $1 Test
        "retained_earnings_proxy": retained_earnings_proxy,
        "beginning_market_cap": beginning_market_cap,
        "market_value_increase": market_value_increase,
        "one_dollar_test_ratio": one_dollar_test_ratio,

        # Scores
        "roic_score": roic_score,
        "roe_score": roe_score,
        "quality_score": quality_score,

        "interest_coverage_score": interest_coverage_score,
        "net_debt_owner_earnings_score": net_debt_owner_earnings_score,
        "net_debt_ebit_score": net_debt_ebit_score,
        "debt_safety_score": debt_safety_score,

        "owner_earnings_score": owner_earnings_score,
        "cash_score": cash_score,
        "capital_allocation_score": capital_allocation_score,
        "valuation_score": valuation_score,
        "final_score": final_score,

        # Pass / Fail
        "pass_roic": pass_roic,
        "pass_roe_supportive": pass_roe_supportive,
        "pass_interest_coverage": pass_interest_coverage,
        "pass_net_debt_to_owner_earnings": pass_net_debt_to_owner_earnings,
        "pass_net_debt_to_ebit": pass_net_debt_to_ebit,
        "pass_debt_safety": pass_debt_safety,
        "pass_owner_earnings": pass_owner_earnings,
        "pass_one_dollar_test": pass_one_dollar_test,
        "pass_valuation": pass_valuation,
        "overall_pass": overall_pass
    })

    return result


# =========================================================
# Main
# =========================================================

def run_screen(tickers: List[str], config: BuffettConfig) -> pd.DataFrame:
    results = []

    for ticker in tickers:
        ticker = ticker.strip().upper()
        if not ticker:
            continue

        print(f"Analysing {ticker} ...")

        try:
            r = analyse_ticker(ticker, config)
        except Exception as e:
            r = {
                "ticker": ticker,
                "status": f"error: {str(e)}"
            }

        results.append(r)

    df = pd.DataFrame(results)

    if "final_score" in df.columns:
        df = df.sort_values("final_score", ascending=False)

    return df


def normalize_tickers(tickers: List[str]) -> List[str]:
    seen = set()
    normalized = []

    for ticker in tickers:
        ticker = str(ticker).strip().upper()
        if not ticker or ticker == "NAN":
            continue
        if ticker in seen:
            continue

        seen.add(ticker)
        normalized.append(ticker)

    return normalized


def load_tickers_from_csv(path: str) -> List[str]:
    symbols_df = pd.read_csv(path)
    symbol_cols = [
        col for col in symbols_df.columns
        if str(col).strip().lower() == "symbol"
    ]

    if not symbol_cols:
        available_cols = ", ".join(str(col) for col in symbols_df.columns)
        raise ValueError(
            f"Could not find a symbol column in {path}. "
            f"Available columns: {available_cols}"
        )

    return normalize_tickers(symbols_df[symbol_cols[0]].tolist())


def format_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    输出更易读的版本。
    """
    percentage_cols = [
        "latest_roic",
        "median_roic",
        "roic_volatility",
        "latest_roe",
        "median_roe",
        "roe_volatility",
        "owner_earnings_to_net_income",
        "owner_earnings_positive_ratio",
        "owner_earnings_yield",
    ]

    ratio_cols = [
        "latest_interest_coverage",
        "median_interest_coverage",
        "effective_interest_coverage",
        "latest_net_debt_to_owner_earnings",
        "latest_net_debt_to_ebit",
        "latest_debt_to_equity_reference",
        "price_to_owner_earnings",
        "one_dollar_test_ratio",
    ]

    score_cols = [
        "roic_score",
        "roe_score",
        "quality_score",
        "interest_coverage_score",
        "net_debt_owner_earnings_score",
        "net_debt_ebit_score",
        "debt_safety_score",
        "owner_earnings_score",
        "cash_score",
        "capital_allocation_score",
        "valuation_score",
        "model_applicability_score",
        "final_score",
    ]

    money_cols = [
        "latest_nopat",
        "latest_invested_capital",
        "latest_total_debt",
        "latest_cash",
        "latest_net_debt",
        "latest_interest_expense_abs",
        "latest_net_income",
        "latest_owner_earnings",
        "median_owner_earnings",
        "market_cap",
        "retained_earnings_proxy",
        "beginning_market_cap",
        "market_value_increase",
    ]

    out = df.copy()

    for col in percentage_cols:
        if col in out.columns:
            out[col] = out[col].apply(
                lambda x: np.nan if pd.isna(x) else round(x * 100, 2)
            )

    for col in ratio_cols:
        if col in out.columns:
            out[col] = out[col].apply(
                lambda x: np.nan if pd.isna(x) else ("inf" if x == np.inf else round(x, 2))
            )

    for col in score_cols:
        if col in out.columns:
            out[col] = out[col].apply(
                lambda x: np.nan if pd.isna(x) else round(x, 2)
            )

    for col in money_cols:
        if col in out.columns:
            out[col] = out[col].apply(
                lambda x: np.nan if pd.isna(x) else round(x, 0)
            )

    out = out.rename(columns={
        "latest_roic": "latest_roic_pct",
        "median_roic": "median_roic_pct",
        "latest_roe": "latest_roe_pct",
        "median_roe": "median_roe_pct",
        "roic_volatility": "roic_volatility_pct",
        "roe_volatility": "roe_volatility_pct",
        "owner_earnings_to_net_income": "owner_earnings_to_net_income_pct",
        "owner_earnings_positive_ratio": "owner_earnings_positive_ratio_pct",
        "owner_earnings_yield": "owner_earnings_yield_pct",
    })

    return out


def build_config_from_args(args) -> BuffettConfig:
    return BuffettConfig(
        years=args.years,
        min_median_roic=args.min_roic,
        min_median_roe=args.min_roe,
        min_interest_coverage=args.min_interest_coverage,
        max_net_debt_to_owner_earnings=args.max_net_debt_to_owner_earnings,
        max_net_debt_to_ebit=args.max_net_debt_to_ebit,
        min_owner_earnings_yield=args.min_owner_earnings_yield,
        min_owner_earnings_to_net_income=args.min_owner_earnings_to_net_income,
        min_one_dollar_test_ratio=args.min_one_dollar_test_ratio,
        default_tax_rate=args.default_tax_rate,
        maintenance_capex_method=args.maintenance_capex_method
    )


def main():
    parser = argparse.ArgumentParser(
        description="Buffett-style Quant Screen: ROIC + Debt Safety + Owner Earnings + $1 Test"
    )

    parser.add_argument(
        "--tickers",
        type=str,
        default="",
        help="Comma separated tickers, e.g. AAPL,MSFT,KO,COST,BRK-B"
    )

    parser.add_argument(
        "--symbols-csv",
        type=str,
        default="",
        help="CSV file containing a symbol column, e.g. sp500_constituents.csv"
    )

    parser.add_argument(
        "--years",
        type=int,
        default=DEFAULT_YEARS,
        help=f"Number of annual financial years to analyse, default {DEFAULT_YEARS}"
    )

    parser.add_argument(
        "--min-roic",
        type=float,
        default=MIN_MEDIAN_ROIC,
        help=f"Minimum median ROIC, default {MIN_MEDIAN_ROIC}"
    )

    parser.add_argument(
        "--min-roe",
        type=float,
        default=MIN_MEDIAN_ROE,
        help=f"Minimum median ROE as supportive check, default {MIN_MEDIAN_ROE}"
    )

    parser.add_argument(
        "--min-interest-coverage",
        type=float,
        default=MIN_INTEREST_COVERAGE,
        help=f"Minimum interest coverage, default {MIN_INTEREST_COVERAGE}"
    )

    parser.add_argument(
        "--max-net-debt-to-owner-earnings",
        type=float,
        default=MAX_NET_DEBT_TO_OWNER_EARNINGS,
        help=f"Maximum net debt / owner earnings, default {MAX_NET_DEBT_TO_OWNER_EARNINGS}"
    )

    parser.add_argument(
        "--max-net-debt-to-ebit",
        type=float,
        default=MAX_NET_DEBT_TO_EBIT,
        help=f"Maximum net debt / EBIT, default {MAX_NET_DEBT_TO_EBIT}"
    )

    parser.add_argument(
        "--min-owner-earnings-yield",
        type=float,
        default=MIN_OWNER_EARNINGS_YIELD,
        help=f"Minimum owner earnings yield, default {MIN_OWNER_EARNINGS_YIELD}"
    )

    parser.add_argument(
        "--min-owner-earnings-to-net-income",
        type=float,
        default=MIN_OWNER_EARNINGS_TO_NET_INCOME,
        help=f"Minimum owner earnings to net income ratio, default {MIN_OWNER_EARNINGS_TO_NET_INCOME}"
    )

    parser.add_argument(
        "--min-one-dollar-test-ratio",
        type=float,
        default=MIN_ONE_DOLLAR_TEST_RATIO,
        help=f"Minimum $1 test ratio, default {MIN_ONE_DOLLAR_TEST_RATIO}"
    )

    parser.add_argument(
        "--default-tax-rate",
        type=float,
        default=DEFAULT_TAX_RATE,
        help=f"Default tax rate used when tax data is missing or abnormal, default {DEFAULT_TAX_RATE}"
    )

    parser.add_argument(
        "--maintenance-capex-method",
        type=str,
        default=MAINTENANCE_CAPEX_METHOD,
        choices=["conservative", "da_proxy"],
        help="Maintenance capex estimation method"
    )

    parser.add_argument(
        "--debug-interest",
        action="store_true",
        help="Print debug information for interest expense and interest coverage"
    )

    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT_FILE,
        help=f"Output CSV file, default {DEFAULT_OUTPUT_FILE}"
    )

    args = parser.parse_args()

    global DEBUG_INTEREST
    DEBUG_INTEREST = args.debug_interest

    config = build_config_from_args(args)
    tickers = normalize_tickers(args.tickers.split(","))

    if args.symbols_csv:
        csv_tickers = load_tickers_from_csv(args.symbols_csv)
        tickers = normalize_tickers(tickers + csv_tickers)

    if not tickers:
        parser.error("Provide --tickers or --symbols-csv.")

    print(f"Loaded {len(tickers)} tickers.")

    result_df = run_screen(tickers, config)
    output_df = format_output(result_df)

    print("\n========== Buffett-style Debt Safety Screen Result ==========")

    display_cols = [
        "ticker",
        "company_name",
        "status",
        "sector",
        "industry",
        "model_applicability_score",
        "model_applicability_note",
        "final_score",
        "overall_pass",

        "median_roic_pct",
        "latest_roic_pct",
        "median_roe_pct",
        "latest_roe_pct",

        "latest_interest_coverage",
        "median_interest_coverage",
        "effective_interest_coverage",
        "interest_coverage_note",
        "latest_net_debt_to_owner_earnings",
        "latest_net_debt_to_ebit",
        "latest_debt_to_equity_reference",

        "owner_earnings_to_net_income_pct",
        "owner_earnings_yield_pct",
        "one_dollar_test_ratio",

        "quality_score",
        "debt_safety_score",
        "cash_score",
        "capital_allocation_score",
        "valuation_score",

        "pass_roic",
        "pass_roe_supportive",
        "pass_interest_coverage",
        "pass_net_debt_to_owner_earnings",
        "pass_net_debt_to_ebit",
        "pass_debt_safety",
        "pass_owner_earnings",
        "pass_one_dollar_test",
        "pass_valuation",

        "roic_method_note",
        "net_debt_method_note",
        "capex_note",
    ]

    display_cols = [c for c in display_cols if c in output_df.columns]

    print(output_df[display_cols].to_string(index=False))

    output_df.to_csv(args.output, index=False, encoding="utf-8-sig")

    print(f"\nSaved to: {args.output}")


if __name__ == "__main__":
    main()
