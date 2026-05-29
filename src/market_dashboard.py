from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf


REFRESH_SECONDS = 10
MA_CACHE_SECONDS = 30 * 60
DATA_SOURCE_NAME = "yfinance"
DISPLAY_TZ = ZoneInfo("Pacific/Auckland")
MARKET_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class EtfDefinition:
    ticker: str
    name: str
    category: str
    meaning: str
    use_case: str


@dataclass(frozen=True)
class EtfSection:
    title: str
    description: str
    items: tuple[EtfDefinition, ...]
    note: str | None = None


@dataclass(frozen=True)
class ModuleDiagnosis:
    module: str
    logic: str
    breakout_context: str


@dataclass(frozen=True)
class ModuleState:
    section_key: str
    leaders: tuple[str, ...]
    laggards: tuple[str, ...]
    breadth20: float
    breadth50: float
    valid_count: int
    item_count: int


def _etf(ticker: str, name: str, category: str, meaning: str, use_case: str) -> EtfDefinition:
    return EtfDefinition(ticker=ticker, name=name, category=category, meaning=meaning, use_case=use_case)


MARKET_SECTIONS: tuple[EtfSection, ...] = (
    EtfSection(
        title="Market Benchmark / 市场基准",
        description="市场基准用于判断整体市场风险偏好。后续所有行业、主题、风格和供应链表现，都可以与这些基准进行对照。",
        items=(
            _etf("SPY", "SPDR S&P 500 ETF Trust", "Market Benchmark", "S&P 500 大盘基准", "观察美股整体市场方向"),
            _etf("QQQ", "Invesco QQQ Trust", "Market Benchmark", "Nasdaq 100 科技成长基准", "观察科技成长股风险偏好"),
            _etf("IWM", "iShares Russell 2000 ETF", "Market Benchmark", "Russell 2000 小盘股基准", "观察小盘股和高 beta 风险偏好"),
            _etf("DIA", "SPDR Dow Jones Industrial Average ETF", "Market Benchmark", "Dow Jones 工业指数基准", "观察传统大型蓝筹股表现"),
        ),
    ),
    EtfSection(
        title="Sector Classification / 宏观行业分类",
        description="宏观行业分类用于观察美股市场中最标准的一级行业轮动。该区域主要使用 Select Sector SPDR ETFs，代表 S&P 500 的 11 个主要行业板块。",
        items=(
            _etf("XLK", "Technology Select Sector SPDR Fund", "Technology / 科技", "代表科技硬件、软件、半导体等大科技板块", "判断科技板块是否强于大盘"),
            _etf("XLF", "Financial Select Sector SPDR Fund", "Financials / 金融", "代表银行、保险、券商、资管等金融板块", "判断金融板块对利率和经济周期的反应"),
            _etf("XLE", "Energy Select Sector SPDR Fund", "Energy / 能源", "代表油气开采、能源设备、综合能源公司", "观察油价和能源周期"),
            _etf("XLV", "Health Care Select Sector SPDR Fund", "Health Care / 医疗保健", "代表药企、医疗设备、医疗服务公司", "观察防御型医疗板块表现"),
            _etf("XLI", "Industrial Select Sector SPDR Fund", "Industrials / 工业", "代表工业制造、航空航天、物流、工程服务", "观察经济周期和工业活动"),
            _etf("XLC", "Communication Services Select Sector SPDR Fund", "Communication Services / 通信服务", "代表互联网平台、媒体、电信和广告平台", "观察广告、媒体和平台型公司表现"),
            _etf("XLY", "Consumer Discretionary Select Sector SPDR Fund", "Consumer Discretionary / 可选消费", "代表电商、汽车、零售、旅游和休闲消费", "观察消费风险偏好"),
            _etf("XLP", "Consumer Staples Select Sector SPDR Fund", "Consumer Staples / 必需消费", "代表食品、饮料、日用品等防御型消费", "观察防御型消费是否走强"),
            _etf("XLU", "Utilities Select Sector SPDR Fund", "Utilities / 公用事业", "代表电力、水务、燃气等利率敏感型防御板块", "观察防御需求和利率敏感资产"),
            _etf("XLRE", "Real Estate Select Sector SPDR Fund", "Real Estate / 房地产", "代表 REITs 和房地产相关公司", "观察房地产和利率敏感资产表现"),
            _etf("XLB", "Materials Select Sector SPDR Fund", "Materials / 原材料", "代表化工、金属、建材、包装材料", "观察原材料和通胀周期"),
        ),
    ),
    EtfSection(
        title="Thematic Classification / 主题行业分类",
        description="主题行业分类围绕市场叙事、长期趋势或资本开支方向分类，适合观察 alpha 来源、热门主线扩散和主题强势状态。",
        items=(
            _etf("BOTZ", "Global X Robotics & AI ETF", "Artificial Intelligence / Robotics", "机器人、自动化和人工智能主题", "观察 AI 与机器人主题强弱"),
            _etf("AIQ", "Global X Artificial Intelligence & Technology ETF", "Artificial Intelligence", "人工智能和技术创新相关公司", "观察 AI 主题扩散程度"),
            _etf("CHAT", "Roundhill Generative AI & Technology ETF", "Generative AI", "生成式 AI、AI 软件和相关应用", "观察生成式 AI 应用方向"),
            _etf("SOXX", "iShares Semiconductor ETF", "Semiconductors", "美国上市半导体公司", "观察芯片、GPU 和 AI 算力主线"),
            _etf("SMH", "VanEck Semiconductor ETF", "Semiconductors", "全球核心半导体公司", "观察半导体龙头和 AI 硬件链"),
            _etf("CIBR", "First Trust NASDAQ Cybersecurity ETF", "Cybersecurity", "网络安全软件和服务", "观察企业安全支出和安全软件强弱"),
            _etf("HACK", "ETFMG Prime Cyber Security ETF", "Cybersecurity", "网络安全主题 ETF", "观察网络安全板块表现"),
            _etf("WCLD", "WisdomTree Cloud Computing Fund", "Cloud Computing", "云计算和 SaaS 公司", "观察云软件、订阅软件和成长软件"),
            _etf("SKYY", "First Trust Cloud Computing ETF", "Cloud Computing", "云基础设施和云服务公司", "观察云计算产业链"),
            _etf("CLOU", "Global X Cloud Computing ETF", "Cloud Computing", "云软件和云基础设施", "观察云主题强弱"),
            _etf("ROBO", "ROBO Global Robotics and Automation ETF", "Robotics", "机器人、自动化和智能制造", "观察工业自动化方向"),
            _etf("ICLN", "iShares Global Clean Energy ETF", "Clean Energy", "清洁能源公司", "观察新能源主题"),
            _etf("TAN", "Invesco Solar ETF", "Solar", "太阳能产业链", "观察太阳能主题强弱"),
            _etf("QCLN", "First Trust NASDAQ Clean Edge Green Energy ETF", "Clean Energy / EV", "清洁能源、电动车和新能源技术", "观察新能源和电动车综合主题"),
            _etf("URA", "Global X Uranium ETF", "Uranium / Nuclear", "铀矿和核能相关公司", "观察核能和数据中心电力主题"),
            _etf("URNM", "Sprott Uranium Miners ETF", "Uranium", "铀矿生产商和核燃料链", "观察铀矿价格和核能主题"),
            _etf("NLR", "VanEck Uranium and Nuclear ETF", "Nuclear Energy", "核能、电力和核设施相关公司", "观察核能产业链"),
            _etf("XBI", "SPDR S&P Biotech ETF", "Biotech", "生物科技和创新药公司", "观察医药风险偏好"),
            _etf("IBB", "iShares Biotechnology ETF", "Biotech", "大型生物科技和创新药公司", "观察生物科技龙头表现"),
            _etf("ITA", "iShares U.S. Aerospace & Defense ETF", "Defence / Aerospace", "航空航天和国防承包商", "观察国防支出和军工板块"),
            _etf("XAR", "SPDR S&P Aerospace & Defense ETF", "Aerospace & Defence", "航空航天和国防企业", "观察军工主题扩散"),
            _etf("PPA", "Invesco Aerospace & Defense ETF", "Aerospace & Defence", "国防、航空航天和安全相关公司", "观察政府国防订单相关主题"),
            _etf("BITQ", "Bitwise Crypto Industry Innovators ETF", "Crypto-linked Equities", "加密货币相关上市公司", "观察加密股票风险偏好"),
            _etf("BLOK", "Amplify Transformational Data Sharing ETF", "Blockchain", "区块链和加密基础设施公司", "观察区块链主题"),
        ),
    ),
    EtfSection(
        title="Value Chain Classification / 产业链分类",
        description="产业链分类用于观察一个市场主线内部，上游、中游、下游是否共振，适合用于 breakout validation。",
        items=(
            _etf("SOXX", "iShares Semiconductor ETF", "AI 上游 / 半导体", "芯片设计、制造、设备、材料", "验证 AI 硬件链是否强势"),
            _etf("SMH", "VanEck Semiconductor ETF", "AI 上游 / 半导体龙头", "全球核心半导体龙头", "验证 AI 算力主线是否强势"),
            _etf("MAGS", "Roundhill Magnificent Seven ETF", "AI 中游 / AI 平台与巨头", "Magnificent Seven 等大型科技平台", "观察 AI 资本开支和大科技平台表现"),
            _etf("QQQ", "Invesco QQQ Trust", "AI 云平台与成长科技", "Nasdaq 100 科技成长股", "观察 AI 主题是否扩散到大盘科技"),
            _etf("WCLD", "WisdomTree Cloud Computing Fund", "AI 下游 / AI 软件应用", "云软件和 SaaS 公司", "观察 AI 应用层是否跟随"),
            _etf("IGV", "iShares Expanded Tech-Software Sector ETF", "AI 下游 / 软件", "软件和企业应用公司", "观察软件应用层强弱"),
            _etf("XLU", "Utilities Select Sector SPDR Fund", "AI 电力与能源", "公用事业、电力供应", "观察数据中心用电主题"),
            _etf("URA", "Global X Uranium ETF", "AI 核能与铀矿", "核能和铀矿链", "观察 AI 电力需求对核能主题的影响"),
            _etf("IFRA", "iShares U.S. Infrastructure ETF", "AI 基础设施", "美国基础设施公司", "观察数据中心和电网建设需求"),
            _etf("PAVE", "Global X U.S. Infrastructure Development ETF", "AI 基础设施", "美国基建和工程建设", "观察基建资本开支强弱"),
            _etf("XOP", "SPDR S&P Oil & Gas Exploration & Production ETF", "能源上游 / 油气开采", "油气勘探和生产", "观察油价敏感上游公司"),
            _etf("XLE", "Energy Select Sector SPDR Fund", "能源综合能源", "大型能源公司", "观察能源板块整体表现"),
            _etf("AMLP", "Alerian MLP ETF", "能源中游 / 管道运输", "能源管道和 MLP 基础设施", "观察能源运输和现金流型资产"),
            _etf("MLPA", "Global X MLP ETF", "能源中游 / 能源基础设施", "能源管道、储运和基础设施", "观察中游能源资产"),
            _etf("CRAK", "VanEck Oil Refiners ETF", "能源下游 / 炼油", "炼油和成品油公司", "观察炼油利润和下游能源"),
            _etf("OIH", "VanEck Oil Services ETF", "能源油服设备", "油田服务和钻井设备", "观察能源资本开支"),
            _etf("XES", "SPDR S&P Oil & Gas Equipment & Services ETF", "能源油服设备", "能源设备和服务", "观察油服板块强弱"),
            _etf("XLRE", "Real Estate Select Sector SPDR Fund", "房地产资产", "REITs 和房地产相关公司", "观察房地产资产表现"),
            _etf("VNQ", "Vanguard Real Estate ETF", "房地产资产", "广泛 REITs 暴露", "观察租金和地产现金流"),
            _etf("IYR", "iShares U.S. Real Estate ETF", "房地产资产", "美国房地产和 REITs", "观察地产板块强弱"),
            _etf("ITB", "iShares U.S. Home Construction ETF", "房屋建造", "Homebuilders", "观察住宅建设周期"),
            _etf("XHB", "SPDR S&P Homebuilders ETF", "房屋建造与家装", "房屋建筑商、建材和家装相关公司", "观察房地产下游需求"),
            _etf("XLB", "Materials Select Sector SPDR Fund", "建材与材料", "材料、化工、建材", "观察房地产和基建材料需求"),
        ),
    ),
    EtfSection(
        title="Style Factor Classification / 风格因子分类",
        description="风格因子分类用于观察当前市场偏好，例如成长、价值、质量、动量、低波动、小盘和股息。",
        items=(
            _etf("QUAL", "iShares MSCI USA Quality Factor ETF", "Quality / 质量", "高 ROE、盈利稳定、低负债公司", "观察质量因子是否占优"),
            _etf("MTUM", "iShares MSCI USA Momentum Factor ETF", "Momentum / 动量", "近期价格动量较强的股票", "观察动量因子是否强势"),
            _etf("VLUE", "iShares MSCI USA Value Factor ETF", "Value / 价值", "低估值、价值特征股票", "观察价值股是否走强"),
            _etf("VTV", "Vanguard Value ETF", "Value / 大盘价值", "美国大盘价值股", "观察价值风格表现"),
            _etf("USMV", "iShares MSCI USA Min Vol Factor ETF", "Minimum Volatility / 低波动", "低波动股票", "观察防御型低波动资产"),
            _etf("SPLV", "Invesco S&P 500 Low Volatility ETF", "Low Volatility / 低波动", "S&P 500 中低波动股票", "观察低波动防御风格"),
            _etf("SIZE", "iShares MSCI USA Size Factor ETF", "Size Factor / 小市值倾向", "相对更小市值的大中盘股票", "观察 size factor"),
            _etf("IWM", "iShares Russell 2000 ETF", "Small Cap / 小盘", "Russell 2000 小盘股", "观察小盘风险偏好"),
            _etf("VIG", "Vanguard Dividend Appreciation ETF", "Dividend Growth / 股息增长", "持续提高股息的公司", "观察股息增长风格"),
            _etf("SCHD", "Schwab U.S. Dividend Equity ETF", "Dividend / 高质量股息", "高股息和质量股息公司", "观察防御型股息资产"),
            _etf("SDY", "SPDR S&P Dividend ETF", "Dividend Aristocrats / 股息贵族", "长期稳定增加股息的公司", "观察稳定股息风格"),
            _etf("VUG", "Vanguard Growth ETF", "Growth / 成长", "美国大盘成长股", "观察成长风格"),
            _etf("IWF", "iShares Russell 1000 Growth ETF", "Growth / 大盘成长", "Russell 1000 成长股", "观察大盘成长风格"),
            _etf("VB", "Vanguard Small-Cap ETF", "Small Cap / 小盘", "美国小盘股", "观察小盘资产表现"),
            _etf("SLY", "SPDR S&P 600 Small Cap ETF", "Small Cap / S&P 小盘", "S&P SmallCap 600", "观察小盘质量更高的股票池"),
        ),
    ),
    EtfSection(
        title="Business Model Classification / 商业模式分类",
        description="商业模式分类用 ETF 作为 proxy benchmark，观察 SaaS、平台型互联网、电商、轻资产品牌、重资产制造和资源生产商等模式强弱。",
        note="本区域 ETF 是 proxy ETF，不是严格一一对应分类。",
        items=(
            _etf("WCLD", "WisdomTree Cloud Computing Fund", "SaaS / 订阅软件", "云软件、订阅收入、企业软件服务", "观察 SaaS 和订阅软件模式"),
            _etf("IGV", "iShares Expanded Tech-Software Sector ETF", "Software / 企业软件", "软件服务和企业应用", "观察软件商业模式强弱"),
            _etf("MAGS", "Roundhill Magnificent Seven ETF", "Platform / 平台型互联网", "大型平台公司和科技巨头", "观察网络效应和大平台资产"),
            _etf("QQQ", "Invesco QQQ Trust", "Platform / Growth Technology", "科技成长和平台型公司", "观察大科技平台风险偏好"),
            _etf("XLC", "Communication Services Select Sector SPDR Fund", "Platform / Media / Communication", "互联网平台、媒体和通信服务", "观察广告平台和媒体平台"),
            _etf("IBUY", "Amplify Online Retail ETF", "Online Retail / 电商", "线上零售和电商消费", "观察电商消费强弱"),
            _etf("XLY", "Consumer Discretionary Select Sector SPDR Fund", "Consumer Discretionary / 可选消费", "汽车、电商、零售、旅游", "观察消费弹性和风险偏好"),
            _etf("XLP", "Consumer Staples Select Sector SPDR Fund", "Asset-light Brand / 防御消费品牌", "食品、饮料、日用品品牌", "观察防御型消费品牌"),
            _etf("VCR", "Vanguard Consumer Discretionary ETF", "Consumer Discretionary / 可选消费品牌", "可选消费品牌和零售", "观察消费品牌强弱"),
            _etf("XLI", "Industrial Select Sector SPDR Fund", "Capital-intensive Manufacturing / 重资产制造", "工业制造、工程、运输", "观察制造业和工业资本开支"),
            _etf("VIS", "Vanguard Industrials ETF", "Industrial Manufacturing / 工业制造", "美国工业公司", "观察工业制造商业模式"),
            _etf("XLB", "Materials Select Sector SPDR Fund", "Materials / 原材料生产", "化工、金属、建材", "观察原材料和重资产周期"),
            _etf("XLE", "Energy Select Sector SPDR Fund", "Commodity Producer / 能源生产商", "油气生产和综合能源", "观察商品价格收入模式"),
            _etf("GDX", "VanEck Gold Miners ETF", "Gold Miners / 黄金矿商", "黄金矿业公司", "观察黄金价格相关企业"),
            _etf("COPX", "Global X Copper Miners ETF", "Copper Miners / 铜矿商", "铜矿和铜资源公司", "观察铜价和电气化需求"),
            _etf("XLU", "Utilities Select Sector SPDR Fund", "Regulated Utility / 受监管公用事业", "电力、水务、燃气", "观察稳定现金流和利率敏感资产"),
            _etf("VPU", "Vanguard Utilities ETF", "Utilities / 公用事业", "美国公用事业公司", "观察防御型公用事业资产"),
            _etf("XLF", "Financial Select Sector SPDR Fund", "Financial Intermediary / 金融中介", "银行、保险、券商、资管", "观察金融中介商业模式"),
            _etf("KRE", "SPDR S&P Regional Banking ETF", "Regional Banks / 区域银行", "区域银行", "观察利率、信贷和区域银行风险"),
            _etf("KBE", "SPDR S&P Bank ETF", "Banks / 银行", "银行业", "观察银行商业模式"),
            _etf("XLRE", "Real Estate Select Sector SPDR Fund", "Rental Income / 租金收入模型", "REITs 和房地产资产", "观察租金现金流资产"),
            _etf("VNQ", "Vanguard Real Estate ETF", "REITs / 房地产租金", "广泛 REITs", "观察地产现金流和利率敏感资产"),
            _etf("IFRA", "iShares U.S. Infrastructure ETF", "Infrastructure Operator / 基础设施运营", "基础设施运营和建设", "观察基建和稳定资产"),
            _etf("IGF", "iShares Global Infrastructure ETF", "Global Infrastructure / 全球基础设施", "全球基础设施资产", "观察基础设施运营模式"),
        ),
    ),
    EtfSection(
        title="Revenue Exposure Classification / 收入来源分类",
        description="收入来源分类从公司赚钱方式角度观察市场，适合宏观冲击分析，例如利率、美元、油价、广告周期、云支出和消费周期。",
        note="本区域 ETF 是 proxy ETF，不是严格收入拆分。",
        items=(
            _etf("XLC", "Communication Services Select Sector SPDR Fund", "Advertising Revenue / 广告收入", "互联网广告平台、媒体和通信服务", "观察广告周期"),
            _etf("FDN", "First Trust Dow Jones Internet Index Fund", "Internet Revenue / 互联网收入", "互联网平台和线上业务", "观察互联网收入模式"),
            _etf("MAGS", "Roundhill Magnificent Seven ETF", "Platform / Advertising / Cloud / Hardware", "大型科技平台综合收入", "观察大科技收入暴露"),
            _etf("WCLD", "WisdomTree Cloud Computing Fund", "Subscription Revenue / 订阅收入", "SaaS 和云软件订阅收入", "观察订阅软件支出"),
            _etf("IGV", "iShares Expanded Tech-Software Sector ETF", "Software Revenue / 软件收入", "企业软件和应用软件", "观察软件收入模式"),
            _etf("XLK", "Technology Select Sector SPDR Fund", "Hardware / Software / Tech Revenue", "科技硬件、软件和半导体收入", "观察科技收入综合表现"),
            _etf("SOXX", "iShares Semiconductor ETF", "Semiconductor Revenue / 半导体收入", "芯片、GPU、半导体设备", "观察芯片收入周期"),
            _etf("SMH", "VanEck Semiconductor ETF", "Semiconductor Revenue / 半导体收入", "全球半导体龙头", "观察 AI 算力和芯片需求"),
            _etf("QQQ", "Invesco QQQ Trust", "Cloud / Platform / Growth Revenue", "科技成长收入", "观察云、平台和成长收入"),
            _etf("XLF", "Financial Select Sector SPDR Fund", "Net Interest Income / 利息收入", "银行和金融机构", "观察利率和信贷周期"),
            _etf("KRE", "SPDR S&P Regional Banking ETF", "Regional Bank Interest Income / 区域银行利息收入", "区域银行", "观察区域银行和利率风险"),
            _etf("KBE", "SPDR S&P Bank ETF", "Banking Revenue / 银行收入", "银行业收入", "观察银行利润周期"),
            _etf("KIE", "SPDR S&P Insurance ETF", "Insurance Premium / 保费收入", "保险公司", "观察保险收入和承保周期"),
            _etf("XLE", "Energy Select Sector SPDR Fund", "Commodity-linked Revenue / 能源商品收入", "油气价格相关收入", "观察能源商品价格"),
            _etf("XLB", "Materials Select Sector SPDR Fund", "Materials Revenue / 原材料收入", "材料、化工、金属", "观察通胀和材料需求"),
            _etf("GDX", "VanEck Gold Miners ETF", "Gold-linked Revenue / 黄金相关收入", "黄金矿业收入", "观察黄金价格敏感资产"),
            _etf("COPX", "Global X Copper Miners ETF", "Copper-linked Revenue / 铜相关收入", "铜矿收入", "观察铜价和电气化需求"),
            _etf("URA", "Global X Uranium ETF", "Uranium-linked Revenue / 铀矿相关收入", "铀矿和核能相关收入", "观察核能和铀价"),
            _etf("XLRE", "Real Estate Select Sector SPDR Fund", "Rental Income / 租金收入", "REITs 和房地产租金", "观察租金现金流"),
            _etf("VNQ", "Vanguard Real Estate ETF", "Rental Income / REITs 收入", "广泛 REITs", "观察房地产收入模式"),
            _etf("IYR", "iShares U.S. Real Estate ETF", "Real Estate Income / 房地产收入", "美国房地产和 REITs", "观察地产现金流"),
            _etf("XLP", "Consumer Staples Select Sector SPDR Fund", "Defensive Consumer Revenue / 防御消费收入", "食品、饮料、日用品", "观察防御型消费"),
            _etf("VDC", "Vanguard Consumer Staples ETF", "Consumer Staples Revenue / 必需消费收入", "必需消费品", "观察稳定消费收入"),
            _etf("XLY", "Consumer Discretionary Select Sector SPDR Fund", "Discretionary Consumer Revenue / 可选消费收入", "汽车、电商、旅游、休闲消费", "观察消费周期"),
            _etf("VCR", "Vanguard Consumer Discretionary ETF", "Discretionary Consumer Revenue / 可选消费收入", "可选消费公司", "观察消费弹性"),
            _etf("ITA", "iShares U.S. Aerospace & Defense ETF", "Government Contract Revenue / 政府合同收入", "国防和航空航天承包商", "观察政府订单"),
            _etf("XAR", "SPDR S&P Aerospace & Defense ETF", "Defence Contract Revenue / 国防合同收入", "航空航天和国防企业", "观察军工订单"),
            _etf("PPA", "Invesco Aerospace & Defense ETF", "Security / Defence Revenue / 安全与国防收入", "国防、安全、航空航天", "观察国防支出"),
        ),
    ),
    EtfSection(
        title="Supply Chain Relationship Classification / 供应链关系分类",
        description="供应链关系分类用于观察一个主题内部的上下游关系，判断市场主线是单点上涨还是已经形成供应链扩散。",
        note="本区域 ETF 是供应链 proxy，不是严格供应链成分股映射。",
        items=(
            _etf("SOXX", "iShares Semiconductor ETF", "AI / Fabless Semiconductor / 芯片设计", "Nvidia、AMD、Broadcom 等芯片设计和半导体公司", "观察 AI 算力上游"),
            _etf("SMH", "VanEck Semiconductor ETF", "AI / Semiconductor Leaders / 半导体龙头", "全球半导体龙头", "观察 AI 硬件核心链"),
            _etf("WCLD", "WisdomTree Cloud Computing Fund", "AI / AI Applications / 软件应用", "云软件、SaaS 和 AI 应用", "观察 AI 应用层扩散"),
            _etf("IGV", "iShares Expanded Tech-Software Sector ETF", "AI / Enterprise Software / 企业软件", "企业软件和应用软件", "观察 AI 应用软件表现"),
            _etf("MAGS", "Roundhill Magnificent Seven ETF", "AI / Hyperscalers / 云平台客户", "大型科技平台和 AI 资本开支方", "观察 AI capex 主体"),
            _etf("QQQ", "Invesco QQQ Trust", "AI / Growth Technology / 科技成长", "Nasdaq 100 科技成长股", "观察科技成长整体环境"),
            _etf("XLU", "Utilities Select Sector SPDR Fund", "AI / Power Supply / 电力供应", "电力和公用事业", "观察数据中心用电需求"),
            _etf("URA", "Global X Uranium ETF", "AI / Nuclear / Uranium / 核能铀矿", "核能和铀矿链", "观察 AI 电力需求相关主题"),
            _etf("NLR", "VanEck Uranium and Nuclear ETF", "AI / Nuclear Energy / 核能", "核电和核能相关公司", "观察核能链"),
            _etf("IFRA", "iShares U.S. Infrastructure ETF", "AI / Infrastructure / 基础设施", "美国基础设施建设", "观察数据中心和电网建设"),
            _etf("PAVE", "Global X U.S. Infrastructure Development ETF", "AI / Infrastructure & Construction / 基建工程", "基建、工程和材料", "观察电网和数据中心建设需求"),
            _etf("XLB", "Materials Select Sector SPDR Fund", "AI / Materials / 材料", "化工、建材和金属", "观察基础设施材料需求"),
            _etf("DRIV", "Global X Autonomous & Electric Vehicles ETF", "EV / EV Makers / 电动车整车", "电动车和自动驾驶相关公司", "观察电动车主题"),
            _etf("IDRV", "iShares Self-Driving EV and Tech ETF", "EV / EV Ecosystem / 电动车生态", "电动车、自动驾驶和相关供应链", "观察电动车产业链"),
            _etf("QCLN", "First Trust NASDAQ Clean Edge Green Energy ETF", "EV / Clean Energy / EV", "清洁能源和电动车技术", "观察新能源车和清洁能源共振"),
            _etf("LIT", "Global X Lithium & Battery Tech ETF", "EV / Battery / Lithium / 电池与锂", "锂电池和锂资源", "观察电池链"),
            _etf("BATT", "Amplify Lithium & Battery Technology ETF", "EV / Battery / 电池", "电池材料、储能和电池公司", "观察电池产业链"),
            _etf("COPX", "Global X Copper Miners ETF", "EV / Copper / 铜", "铜矿和铜资源", "观察电动车、电网和数据中心用铜需求"),
            _etf("SOXX", "iShares Semiconductor ETF", "EV / Power Chips / 功率芯片", "半导体和车规芯片", "观察车规芯片和功率半导体"),
            _etf("SMH", "VanEck Semiconductor ETF", "EV / Power Chips / 半导体", "全球半导体龙头", "观察电动车芯片链"),
            _etf("PAVE", "Global X U.S. Infrastructure Development ETF", "EV / Charging Infrastructure / 充电基础设施", "基建和工程建设", "观察充电网络和电网建设"),
            _etf("XLV", "Health Care Select Sector SPDR Fund", "Healthcare / Big Pharma / 大药企", "大型药企、医疗设备和医疗服务", "观察医疗保健整体表现"),
            _etf("PPH", "VanEck Pharmaceutical ETF", "Healthcare / Big Pharma / 大药企", "大型制药公司", "观察成熟药企表现"),
            _etf("XBI", "SPDR S&P Biotech ETF", "Healthcare / Biotech / 生物科技", "创新药和临床阶段药企", "观察医药风险偏好"),
            _etf("IBB", "iShares Biotechnology ETF", "Healthcare / Biotech Leaders / 生物科技龙头", "大型生物科技公司", "观察生物科技龙头表现"),
            _etf("IHI", "iShares U.S. Medical Devices ETF", "Healthcare / Medical Devices / 医疗设备", "医疗设备和耗材", "观察医疗设备需求"),
            _etf("IHF", "iShares U.S. Healthcare Providers ETF", "Healthcare / Healthcare Providers / 医疗服务", "医院、保险和医疗服务", "观察医疗服务链"),
        ),
    ),
)


def render_market_dashboard() -> None:
    st.markdown(_market_css(), unsafe_allow_html=True)
    st.markdown(
        """
        <div class="market-hero">
          <div>
            <div class="eyebrow">Multi-Dimension ETF Classification Dashboard</div>
            <h1>多维 ETF 分类聚合看板</h1>
            <p>按行业、主题、产业链、风格因子、商业模式、收入来源和供应链关系观察美股 ETF 截面状态</p>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(
        "本页面展示的是当前截面，不展示 OHLCV，只展示当前价格、相对前收盘涨跌幅，以及当前价格距离 20 日 / 50 日均线的百分比。"
    )

    auto_refresh = st.toggle(
        "自动刷新",
        value=True,
        help="开启后每 10 秒异步刷新卡片内的动态行情字段；关闭后页面只在手动刷新或重新运行时更新。",
    )
    if auto_refresh:
        _render_market_data_fragment()
    else:
        if st.button("手动刷新行情", type="primary"):
            st.cache_data.clear()
        _render_market_data_snapshot()


@st.fragment(run_every=REFRESH_SECONDS)
def _render_market_data_fragment() -> None:
    _render_market_data_snapshot()


def _render_market_data_snapshot() -> None:
    tickers = _unique_tickers()
    now = datetime.now(DISPLAY_TZ)
    market_data = _fetch_market_snapshot(tickers)
    module_states = {section.title: _build_module_state(section, market_data) for section in MARKET_SECTIONS}
    module_diagnoses: dict[str, ModuleDiagnosis] = {}

    meta_cols = st.columns(4)
    meta_cols[0].metric("最后刷新时间", now.strftime("%Y-%m-%d %H:%M:%S %Z"))
    meta_cols[1].metric("自动刷新间隔", f"{REFRESH_SECONDS} 秒")
    meta_cols[2].metric("数据源", DATA_SOURCE_NAME)
    meta_cols[3].metric("ETF 数量", str(len(tickers)))

    for section in MARKET_SECTIONS:
        st.markdown(f"## {section.title}")
        st.caption(section.description)
        if section.note:
            st.info(section.note)
        diagnosis = _diagnose_section(section, module_states, module_diagnoses, market_data)
        module_diagnoses[section.title] = diagnosis
        _render_module_diagnosis(module_states[section.title], diagnosis)
        _render_card_grid(section, market_data, now)


def _render_card_grid(section: EtfSection, market_data: dict[str, dict], refreshed_at: datetime) -> None:
    items = section.items
    for row_start in range(0, len(items), 4):
        columns = st.columns(4)
        for column, item in zip(columns, items[row_start : row_start + 4], strict=False):
            with column:
                _render_etf_card(section, item, market_data.get(item.ticker), refreshed_at)


def _render_etf_card(section: EtfSection, item: EtfDefinition, snapshot: dict | None, refreshed_at: datetime) -> None:
    tags_html = _render_tags(_get_tags(section.title, item.ticker))
    if not snapshot or not snapshot.get("ok"):
        card = f"""
        <div class="etf-card unavailable">
          <div class="card-top">
            <div><div class="ticker">{item.ticker}</div><div class="name">{item.name}</div></div>
          </div>
          <div class="category">{item.category}</div>
          {tags_html}
          <div class="status-text">Data unavailable</div>
          <div class="static-line"><b>Meaning / 含义</b><br>{item.meaning}</div>
          <div class="static-line"><b>Use Case / 用途</b><br>{item.use_case}</div>
          <div class="updated">Last Updated: {refreshed_at.strftime("%H:%M:%S %Z")}</div>
        </div>
        """
        st.markdown(card, unsafe_allow_html=True)
        return

    change_class = _value_class(snapshot["change_pct"])
    ma20_class = _value_class(snapshot["ma20_distance_pct"])
    ma50_class = _value_class(snapshot["ma50_distance_pct"])
    trend_class = _trend_class(snapshot["trend"])
    card = f"""
    <div class="etf-card">
      <div class="card-top">
        <div><div class="ticker">{item.ticker}</div><div class="name">{item.name}</div></div>
      </div>
      <div class="category">{item.category}</div>
      {tags_html}
      <div class="metrics">
        <div><span>Current Price / 当前价格</span><strong>{_format_price(snapshot["current_price"])}</strong></div>
        <div><span>Change vs Previous Close / 相对前收盘涨跌幅</span><strong class="{change_class}">{_format_pct(snapshot["change_pct"])}</strong></div>
        <div><span>20D MA Distance</span><strong class="{ma20_class}">{_format_ma_distance(20, snapshot["ma20_distance_pct"])}</strong></div>
        <div><span>50D MA Distance</span><strong class="{ma50_class}">{_format_ma_distance(50, snapshot["ma50_distance_pct"])}</strong></div>
      </div>
      <div class="trend {trend_class}">{snapshot["trend"]}</div>
      <div class="static-line"><b>Meaning / 含义</b><br>{item.meaning}</div>
      <div class="static-line"><b>Use Case / 用途</b><br>{item.use_case}</div>
      <div class="updated">Data Status: OK · Last Updated: {refreshed_at.strftime("%H:%M:%S %Z")}</div>
    </div>
    """
    st.markdown(card, unsafe_allow_html=True)


def _render_module_diagnosis(state: ModuleState, diagnosis: ModuleDiagnosis) -> None:
    leaders = ", ".join(state.leaders) if state.leaders else "N/A"
    laggards = ", ".join(state.laggards) if state.laggards else "N/A"
    st.markdown(
        f"""
        <div class="diagnosis-panel">
          <div class="diagnosis-meta">
            <span>Leaders: <b>{leaders}</b></span>
            <span>Laggards: <b>{laggards}</b></span>
            <span>20MA Breadth: <b>{state.breadth20:.0f}%</b></span>
            <span>50MA Breadth: <b>{state.breadth50:.0f}%</b></span>
          </div>
          <div class="diagnosis-grid">
            <div><span>Module Diagnosis / 模块诊断</span><strong>{diagnosis.module}</strong></div>
            <div><span>Diagnosis Logic / 诊断逻辑</span><p>{diagnosis.logic}</p></div>
            <div><span>Breakout Context / 突破环境</span><p>{diagnosis.breakout_context}</p></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _build_module_state(section: EtfSection, market_data: dict[str, dict]) -> ModuleState:
    rows: list[tuple[str, float, float]] = []
    for item in section.items:
        snapshot = market_data.get(item.ticker)
        if not snapshot or not snapshot.get("ok"):
            continue
        rows.append((item.ticker, float(snapshot["ma20_distance_pct"]), float(snapshot["ma50_distance_pct"])))

    ranked = sorted(rows, key=lambda row: row[1], reverse=True)
    leaders = tuple(ticker for ticker, _, _ in ranked[:3])
    laggards = tuple(ticker for ticker, _, _ in ranked[-3:])
    valid_count = len(rows)
    breadth20 = sum(1 for _, ma20, _ in rows if ma20 > 0) / valid_count * 100 if valid_count else 0.0
    breadth50 = sum(1 for _, _, ma50 in rows if ma50 > 0) / valid_count * 100 if valid_count else 0.0
    return ModuleState(
        section_key=_section_key(section.title),
        leaders=leaders,
        laggards=laggards,
        breadth20=breadth20,
        breadth50=breadth50,
        valid_count=valid_count,
        item_count=len(section.items),
    )


def _diagnose_section(
    section: EtfSection,
    states: dict[str, ModuleState],
    diagnoses: dict[str, ModuleDiagnosis],
    market_data: dict[str, dict],
) -> ModuleDiagnosis:
    state = states[section.title]
    key = state.section_key
    if key == "Market Benchmark":
        return _diagnose_market(state, market_data)
    if key == "Sector Classification":
        return _diagnose_sector(state)
    if key == "Thematic Classification":
        return _diagnose_theme(state, market_data)
    if key == "Value Chain Classification":
        return _diagnose_value_chain(state, market_data)
    if key == "Style Factor Classification":
        market_state = _find_state(states, "Market Benchmark")
        return _diagnose_style(state, market_data, market_state)
    if key == "Business Model Classification":
        return _diagnose_business_model(state)
    if key == "Revenue Exposure Classification":
        business_diagnosis = _find_diagnosis(diagnoses, "Business Model Classification")
        return _diagnose_revenue(state, business_diagnosis)
    if key == "Supply Chain Relationship Classification":
        return _diagnose_supply_chain(state, market_data)
    return _diag("不明确 / Mixed", "当前模块没有可用的专属诊断规则。", "Neutral / 中性。当前截面结构不能明显加分或减分。")


def _diagnose_market(state: ModuleState, market_data: dict[str, dict]) -> ModuleDiagnosis:
    if all(_above_both(ticker, market_data) for ticker in ("SPY", "QQQ", "IWM")):
        return _diag(
            "广泛风险偏好 / Broad Risk-on。",
            "SPY、QQQ、IWM 同时强，说明大盘、科技成长和小盘风险偏好同步扩散，市场环境较健康。",
            "Supportive / 支持。多数行业、主题和小盘相关突破拥有较好的市场背景。",
        )
    if _is_leader(state, "QQQ") and (_is_laggard(state, "IWM") or _below_20_or_50("IWM", market_data)):
        return _diag(
            "大盘科技主导 / Mega-cap Tech Led。",
            "QQQ 领先但 IWM 未跟随，说明资金集中在大型科技和成长股，市场宽度不足。",
            "Selective / 选择性支持。大科技、AI、半导体相关突破更有背景；小盘和非科技突破需要更多确认。",
        )
    if all(_above_both(ticker, market_data) for ticker in ("SPY", "QQQ")) and _below_20_or_50("IWM", market_data):
        return _diag(
            "大盘强但小盘未扩散 / Large-cap Strength Without Small-cap Confirmation。",
            "大盘环境尚可，但风险偏好没有充分扩散到小盘。",
            "Selective / 选择性支持。大盘股突破背景较好，小盘突破环境一般。",
        )
    if _is_leader(state, "DIA") and (_is_laggard(state, "QQQ") or _is_laggard(state, "IWM")):
        return _diag(
            "传统蓝筹轮动 / Blue-chip Rotation。",
            "资金偏向传统蓝筹或老经济资产，而不是高 beta 成长和小盘。",
            "Selective / 选择性支持。传统蓝筹、价值、防御类突破更有背景；高成长突破需要谨慎。",
        )
    if sum(1 for ticker in ("SPY", "QQQ", "IWM") if _below_both(ticker, market_data)) >= 2:
        return _diag(
            "风险偏弱 / Risk-off。",
            "主要市场基准多数处于短中期均线下方，整体风险环境偏弱。",
            "Cautious / 谨慎。大多数突破需要降低信任度，除非有非常强的行业或主题确认。",
        )
    return _diag(
        "不明确 / Mixed。",
        "大盘、科技成长、小盘和蓝筹之间没有形成清晰方向。",
        "Neutral / 中性。需要结合 Sector、Theme 和 Style 再判断突破背景。",
    )


def _diagnose_sector(state: ModuleState) -> ModuleDiagnosis:
    if _leader_tag_count(state, ["进攻型"]) >= 2 and _laggard_tag_count(state, ["防御型"]) >= 1:
        return _diag(
            "进攻型行业轮动 / Risk-on Sector Rotation。",
            "资金流向科技、可选消费、通信服务、工业、金融等进攻型行业，防御板块相对落后。",
            "Supportive / 支持。发生在进攻型行业内的个股突破，行业背景较好。",
        )
    if _leader_tag_count(state, ["防御型"]) >= 2:
        return _diag(
            "防御型轮动 / Defensive Rotation。",
            "资金偏向必需消费、公用事业、医疗、房地产等稳定现金流板块。",
            "Selective / 选择性支持。防御、股息、现金流类突破更有行业背景；进攻型成长股突破需要谨慎。",
        )
    if _leader_tag_count(state, ["周期型", "能源", "原材料", "商品", "通胀"]) >= 2:
        return _diag(
            "周期 / 通胀交易 / Cyclical or Inflation Trade。",
            "资金进入能源、材料、工业、金融等周期行业，市场可能在交易商品、通胀或经济修复。",
            "Supportive / 支持。能源、材料、工业、金融相关突破更有行业确认。",
        )
    if _is_leader(state, "XLK") and state.breadth20 < 60:
        return _diag(
            "科技单点领导 / Narrow Tech Leadership。",
            "科技板块领先，但其他行业没有广泛跟随，行业宽度不足。",
            "Selective / 选择性支持。科技突破可以优先观察；非科技突破需要更多确认。",
        )
    if state.breadth20 >= 70 and _leader_distinct_tag_count(state) >= 3:
        return _diag(
            "行业广泛扩散 / Broad Sector Expansion。",
            "强势行业不只集中在单一方向，说明资金扩散较健康。",
            "Supportive / 支持。整体行业环境支持更多类型的突破。",
        )
    return _diag(
        "不明确 / Mixed Sector Structure。",
        "进攻、防御、周期之间没有形成明确主导。",
        "Neutral / 中性。个股突破需要更多依赖 Theme、Style 或 Supply Chain 确认。",
    )


def _diagnose_theme(state: ModuleState, market_data: dict[str, dict]) -> ModuleDiagnosis:
    app_tickers = ("WCLD", "IGV", "CHAT", "AIQ")
    if any(_is_leader(state, ticker) for ticker in ("SOXX", "SMH")) and not any(_is_leader(state, ticker) for ticker in app_tickers):
        return _diag(
            "AI 硬件主导 / AI Hardware Leadership。",
            "半导体、芯片、算力领先，但软件和应用层没有明显跟随。",
            "Selective / 选择性支持。半导体、AI 硬件、算力相关突破确认度更高；软件应用类突破需要额外确认。",
        )
    if any(_strong(ticker, market_data) for ticker in ("SOXX", "SMH")) and any(_is_leader(state, ticker) for ticker in app_tickers):
        return _diag(
            "AI 应用扩散 / AI Application Expansion。",
            "AI 主线从硬件端扩散到软件、云计算和应用层。",
            "Supportive / 支持。AI 软件、云服务、企业软件突破的主题确认度提升。",
        )
    if sum(1 for ticker in ("URA", "NLR", "XLU", "IFRA", "PAVE") if _strong(ticker, market_data)) >= 2:
        return _diag(
            "AI 电力与基础设施主题 / AI Power and Infrastructure Theme。",
            "资金开始定价 AI 背后的电力、核能、铀矿、数据中心和基建需求。",
            "Supportive / 支持。电力、核能、基建、数据中心链条相关突破更有主题背景。",
        )
    non_tech = ("XBI", "IBB", "ITA", "XAR", "PPA", "URA", "NLR")
    if any(_is_leader(state, ticker) for ticker in non_tech) and not any(_strong(ticker, market_data) for ticker in ("SOXX", "SMH", "WCLD")):
        return _diag(
            "非科技主题领先 / Non-tech Theme Leadership。",
            "市场主线不在科技成长，而是转向医药、国防、电力、核能等主题。",
            "Selective / 选择性支持。当前领先主题内的突破确认度更高；科技股突破需要谨慎。",
        )
    if state.breadth20 < 60 and _leader_distinct_tag_count(state) >= 6:
        return _diag(
            "主题分散 / Theme Fragmentation。",
            "主题之间没有形成一致主线，资金较分散。",
            "Cautious / 谨慎。个股突破更可能是孤立事件，需要 Sector、Style 或 Supply Chain 进一步确认。",
        )
    return _diag(
        "不明确 / No Clear Theme。",
        "尚未看到明确的市场叙事集中。",
        "Neutral / 中性。主题确认度不足，个股突破需要更多分类交叉验证。",
    )


def _diagnose_value_chain(state: ModuleState, market_data: dict[str, dict]) -> ModuleDiagnosis:
    if _leader_tag_count(state, ["上游", "半导体", "AI硬件"]) >= 2 and _leader_tag_count(state, ["下游", "基础设施"]) == 0:
        return _diag(
            "上游主导 / Upstream-led。",
            "产业链强势集中在核心硬件或上游供给端。",
            "Selective / 选择性支持。上游硬件、芯片、设备类突破确认度更高；下游应用突破需要更多确认。",
        )
    if _leader_tag_count(state, ["上游"]) >= 1 and _leader_tag_count(state, ["中游"]) >= 1:
        return _diag(
            "平台确认 / Platform Confirmation。",
            "芯片上游和大科技平台 / 云平台同步强，说明需求侧或资本开支方确认。",
            "Supportive / 支持。AI 硬件和大型科技平台相关突破更有产业链背景。",
        )
    if _leader_tag_count(state, ["下游", "AI应用", "软件应用"]) >= 1 and not any(_weak(ticker, state, market_data) for ticker in ("SOXX", "SMH")):
        return _diag(
            "下游扩散 / Downstream Expansion。",
            "产业链开始从硬件扩散到软件、机器人、企业应用等下游环节。",
            "Supportive / 支持。软件应用、自动化、云软件突破的确认度提升。",
        )
    if _leader_tag_count(state, ["基础设施", "电力", "核能", "电网建设", "数据中心建设"]) >= 2:
        return _diag(
            "基础设施确认 / Infrastructure Confirmation。",
            "市场开始定价主线背后的电力、基建和材料需求。",
            "Supportive / 支持。电力、核能、基建、材料类突破更有产业链确认。",
        )
    if _leader_group_count(state, [["上游"], ["中游"], ["下游"], ["基础设施"]]) >= 3:
        return _diag(
            "全链条共振 / Full Chain Resonance。",
            "强势不只集中在一个环节，而是多个产业链环节同步确认。",
            "Supportive / 支持。主线质量较高，相关个股突破的背景更强。",
        )
    if _leaders_single_group_and_laggards_multi(state, [["上游"], ["中游"], ["下游"], ["基础设施"], ["能源上游", "综合能源", "能源中游", "能源下游", "油服"], ["房地产资产", "房屋建造"]]):
        return _diag(
            "链条断裂 / Broken Chain。",
            "产业链确认不足，行情仍停留在局部环节。",
            "Cautious / 谨慎。只接受强环节内突破；弱环节突破需要更多确认。",
        )
    return _diag(
        "不明确 / Mixed Value Chain。",
        "上游、中游、下游和基础设施之间没有形成清晰传导关系。",
        "Neutral / 中性。产业链确认度不足，需要结合 Theme 和 Supply Chain 判断。",
    )


def _diagnose_style(state: ModuleState, market_data: dict[str, dict], market_state: ModuleState | None) -> ModuleDiagnosis:
    if _leader_tag_count(state, ["成长", "动量"]) >= 1 and _laggard_tag_count(state, ["成长"]) < 2:
        return _diag(
            "成长动量占优 / Growth Momentum Regime。",
            "资金偏好成长股和趋势股。",
            "Supportive / 支持。成长股、趋势股、强动量股票突破更有风格支持。",
        )
    if _is_leader(state, "QUAL") or _leader_tag_count(state, ["质量", "盈利质量"]) >= 1:
        return _diag(
            "质量占优 / Quality Regime。",
            "市场偏好盈利质量、低负债和基本面稳定的资产。",
            "Supportive / 支持。高质量公司突破更有风格背景。",
        )
    if _leader_tag_count(state, ["价值", "低估值"]) >= 1 and not any(_is_leader(state, ticker) for ticker in ("QQQ", "VUG", "IWF")):
        return _diag(
            "价值轮动 / Value Rotation。",
            "资金偏向低估值和价值资产。",
            "Selective / 选择性支持。价值股突破确认度更高；高估值成长股突破需要更多确认。",
        )
    if _leader_tag_count(state, ["低波动", "防御", "股息增长", "高质量股息", "股息贵族", "稳定现金流"]) >= 2:
        return _diag(
            "防御风格 / Defensive Style。",
            "市场偏好低波动、股息和稳定现金流。",
            "Selective / 选择性支持。防御、股息、稳定现金流类突破更容易获得风格确认；进攻型突破需要谨慎。",
        )
    if _leader_tag_count(state, ["小盘", "风险扩散"]) >= 1 and not _weak("IWM", market_state or state, market_data):
        return _diag(
            "小盘扩散 / Small-cap Expansion。",
            "风险偏好从大盘扩散到小盘，市场宽度改善。",
            "Supportive / 支持。小盘股突破确认度提高。",
        )
    if all(_strong(ticker, market_data) for ticker in ("QQQ", "VUG", "IWF")) and all(_weak(ticker, state, market_data) for ticker in ("IWM", "VB", "SLY")):
        return _diag(
            "窄幅成长 / Narrow Growth。",
            "成长风格强，但只集中在大盘成长，小盘没有跟随。",
            "Selective / 选择性支持。大盘成长股突破更有风格确认，小盘成长股需要谨慎。",
        )
    return _diag(
        "不明确 / Mixed Style。",
        "当前市场没有明确奖励某一种股票风格。",
        "Neutral / 中性。个股突破不能单靠风格确认，需要回到 Sector 和 Theme 判断。",
    )


def _diagnose_business_model(state: ModuleState) -> ModuleDiagnosis:
    if _leader_tag_count(state, ["SaaS", "订阅软件", "企业软件", "轻资产", "平台型互联网"]) >= 2:
        return _diag(
            "轻资产成长模式 / Asset-light Growth Model。",
            "市场偏好软件、平台、订阅和网络效应资产。",
            "Supportive / 支持。SaaS、平台型、软件类个股突破确认度更高。",
        )
    if _leader_tag_count(state, ["平台型互联网", "网络效应", "大型科技", "互联网平台"]) >= 1:
        return _diag(
            "平台模式占优 / Platform Model Leadership。",
            "资金偏好大型平台和网络效应，而不一定扩散到所有软件公司。",
            "Selective / 选择性支持。大科技平台、互联网平台突破更有商业模式支持；非平台软件需要更多确认。",
        )
    if _leader_tag_count(state, ["电商", "可选消费", "消费弹性"]) >= 1:
        return _diag(
            "消费弹性改善 / Consumer Elasticity。",
            "市场开始奖励电商、线上零售和可选消费模式。",
            "Supportive / 支持。消费弹性相关突破确认度提升。",
        )
    if _leader_tag_count(state, ["资源生产商", "能源", "黄金矿商", "铜矿"]) >= 2:
        return _diag(
            "资源生产商占优 / Commodity Producer Regime。",
            "市场奖励资源生产商和商品价格暴露。",
            "Supportive / 支持。能源、黄金、铜矿、资源类个股突破更有商业模式确认。",
        )
    if _leader_tag_count(state, ["稳定现金流", "公用事业", "租金收入", "房地产现金流"]) >= 2:
        return _diag(
            "稳定现金流偏好 / Stable Cashflow Preference。",
            "市场偏好稳定现金流和利率敏感资产。",
            "Selective / 选择性支持。公用事业、REITs、稳定现金流资产突破更有背景；高成长资产需要更多确认。",
        )
    if _leader_tag_count(state, ["金融中介", "银行"]) >= 2:
        return _diag(
            "金融中介占优 / Financial Intermediary Regime。",
            "市场奖励银行、金融中介和信贷 / 利率相关商业模式。",
            "Supportive / 支持。银行、金融股突破确认度提升。",
        )
    return _diag(
        "不明确 / Mixed Business Model。",
        "当前市场没有明显奖励某一种商业模式。",
        "Neutral / 中性。商业模式层面确认不足，需要结合 Revenue Exposure 判断收入驱动。",
    )


def _diagnose_revenue(state: ModuleState, business_diagnosis: ModuleDiagnosis | None) -> ModuleDiagnosis:
    if _leader_tag_count(state, ["广告收入", "订阅收入", "软件收入", "芯片收入", "云平台收入", "成长收入"]) >= 2:
        return _diag(
            "成长收入占优 / Growth Revenue Regime。",
            "市场奖励广告、订阅、云、软件、芯片等成长型收入。",
            "Supportive / 支持。成长收入相关个股突破更有收入逻辑确认。",
        )
    if _leader_tag_count(state, ["利息收入", "银行收入", "净息差", "金融收入"]) >= 2:
        return _diag(
            "利息收入占优 / Interest Revenue Regime。",
            "市场在交易银行净息差、信贷周期或金融收入改善。",
            "Supportive / 支持。银行、区域银行、金融股突破确认度提升。",
        )
    if _leader_tag_count(state, ["商品收入", "黄金收入", "铜收入", "铀矿收入"]) >= 2:
        return _diag(
            "商品收入占优 / Commodity Revenue Regime。",
            "市场奖励商品价格相关收入。",
            "Supportive / 支持。能源、材料、矿业、资源股突破更有收入来源确认。",
        )
    if _leader_tag_count(state, ["租金收入", "地产收入", "REITs"]) >= 2:
        return _diag(
            "租金与利率敏感收入 / Rental and Rate-sensitive Revenue。",
            "市场开始奖励地产现金流或利率敏感收入。",
            "Supportive / 支持。REITs、房地产、租金现金流相关突破确认度提高。",
        )
    if _leader_tag_count(state, ["防御消费收入", "必需消费", "稳定消费"]) >= 1:
        return _diag(
            "防御消费收入 / Defensive Consumer Revenue。",
            "市场偏好稳定消费收入。",
            "Selective / 选择性支持。必需消费、防御消费相关突破更有收入逻辑支持；高 beta 成长收入需要更多确认。",
        )
    if _leader_tag_count(state, ["政府合同收入", "国防", "安全国防"]) >= 2:
        return _diag(
            "政府合同收入 / Government Contract Revenue。",
            "市场奖励政府订单、国防和安全相关收入。",
            "Supportive / 支持。军工、航空航天、国防承包商突破确认度更高。",
        )
    if business_diagnosis and not business_diagnosis.module.startswith("不明确"):
        return _diag(
            "收入与商业模式不匹配 / Revenue-Business Mismatch。",
            "市场奖励的商业模式和收入来源没有形成一致确认。",
            "Cautious / 谨慎。需要谨慎解释个股突破背后的基本面驱动。",
        )
    return _diag(
        "不明确 / Mixed Revenue Exposure。",
        "当前市场没有明显交易某一类收入来源。",
        "Neutral / 中性。收入来源层面确认不足，需要结合 Business Model 和 Sector 判断。",
    )


def _diagnose_supply_chain(state: ModuleState, market_data: dict[str, dict]) -> ModuleDiagnosis:
    if _leader_tag_count(state, ["核心资产"]) >= 1 and _leader_tag_count(state, ["客户侧确认", "应用层", "电力供应", "基础设施"]) == 0:
        return _diag(
            "核心资产单点行情 / Core-only Rally。",
            "行情集中在核心芯片或核心资产，供应链扩散不足。",
            "Selective / 选择性支持。核心资产突破确认度较高；上下游突破需要更多确认。",
        )
    if _leader_tag_count(state, ["核心资产"]) >= 1 and _leader_tag_count(state, ["客户侧确认"]) >= 1:
        return _diag(
            "客户侧确认 / Customer Confirmation。",
            "核心资产和客户 / 平台资本开支方同步强。",
            "Supportive / 支持。AI 硬件、大科技平台相关突破确认度提升。",
        )
    if _leader_tag_count(state, ["应用层"]) >= 1 and not any(_weak(ticker, state, market_data) for ticker in ("SOXX", "SMH")):
        return _diag(
            "应用层跟随 / Application Follow-through。",
            "行情从核心硬件向软件应用扩散。",
            "Supportive / 支持。AI 应用、云软件、企业软件突破确认度提高。",
        )
    if _leader_tag_count(state, ["电力供应", "基础设施", "基础设施材料"]) >= 2:
        return _diag(
            "电力基建跟随 / Power Infrastructure Follow-through。",
            "市场开始定价电力、核能、数据中心建设和材料需求。",
            "Supportive / 支持。电力、核能、基建、材料相关突破更有供应链确认。",
        )
    if _leader_group_count(state, [["核心资产"], ["客户侧确认"], ["应用层"], ["电力供应"], ["基础设施"]]) >= 3:
        return _diag(
            "供应链扩散 / Chain Expansion。",
            "行情已经从单点核心资产扩散到多个供应链角色。",
            "Supportive / 支持。整条供应链相关突破的确认度提高。",
        )
    if _leader_tag_count(state, ["核心资产"]) >= 1 and sum(1 for ticker in ("MAGS", "QQQ", "WCLD", "IGV", "XLU", "URA", "NLR", "IFRA", "PAVE") if _weak(ticker, state, market_data)) >= 5:
        return _diag(
            "供应链背离 / Chain Divergence。",
            "核心资产和供应链其他角色不同步。",
            "Cautious / 谨慎。主线仍是局部行情，非核心环节突破需要更多确认。",
        )
    return _diag(
        "不明确 / Mixed Supply Chain。",
        "当前没有看到清晰的链式扩散或上下游确认。",
        "Neutral / 中性。供应链确认不足，需要结合 Theme 和 Value Chain 判断。",
    )


def _diag(module: str, logic: str, breakout_context: str) -> ModuleDiagnosis:
    return ModuleDiagnosis(module=module, logic=logic, breakout_context=breakout_context)


def _section_key(section_title: str) -> str:
    return section_title.split(" / ", maxsplit=1)[0]


def _find_state(states: dict[str, ModuleState], section_key: str) -> ModuleState | None:
    for state in states.values():
        if state.section_key == section_key:
            return state
    return None


def _find_diagnosis(diagnoses: dict[str, ModuleDiagnosis], section_key: str) -> ModuleDiagnosis | None:
    for title, diagnosis in diagnoses.items():
        if _section_key(title) == section_key:
            return diagnosis
    return None


def _is_leader(state: ModuleState, ticker: str) -> bool:
    return ticker in state.leaders


def _is_laggard(state: ModuleState, ticker: str) -> bool:
    return ticker in state.laggards


def _strong(ticker: str, market_data: dict[str, dict]) -> bool:
    snapshot = market_data.get(ticker)
    return bool(snapshot and snapshot.get("ok") and snapshot["ma20_distance_pct"] > 0)


def _weak(ticker: str, state: ModuleState, market_data: dict[str, dict]) -> bool:
    return _is_laggard(state, ticker) or _below_20_or_50(ticker, market_data)


def _above_both(ticker: str, market_data: dict[str, dict]) -> bool:
    snapshot = market_data.get(ticker)
    return bool(snapshot and snapshot.get("ok") and snapshot["ma20_distance_pct"] > 0 and snapshot["ma50_distance_pct"] > 0)


def _below_both(ticker: str, market_data: dict[str, dict]) -> bool:
    snapshot = market_data.get(ticker)
    return bool(snapshot and snapshot.get("ok") and snapshot["ma20_distance_pct"] < 0 and snapshot["ma50_distance_pct"] < 0)


def _below_20_or_50(ticker: str, market_data: dict[str, dict]) -> bool:
    snapshot = market_data.get(ticker)
    return bool(snapshot and snapshot.get("ok") and (snapshot["ma20_distance_pct"] < 0 or snapshot["ma50_distance_pct"] < 0))


def _leader_tag_count(state: ModuleState, tags: list[str]) -> int:
    return sum(1 for ticker in state.leaders if _has_any_tag(state, ticker, tags))


def _laggard_tag_count(state: ModuleState, tags: list[str]) -> int:
    return sum(1 for ticker in state.laggards if _has_any_tag(state, ticker, tags))


def _has_any_tag(state: ModuleState, ticker: str, tags: list[str]) -> bool:
    ticker_tags = set(SECTION_TAGS.get(state.section_key, {}).get(ticker, []))
    return bool(ticker_tags.intersection(tags))


def _leader_distinct_tag_count(state: ModuleState) -> int:
    tags: set[str] = set()
    for ticker in state.leaders:
        tags.update(SECTION_TAGS.get(state.section_key, {}).get(ticker, []))
    return len(tags)


def _leader_group_count(state: ModuleState, groups: list[list[str]]) -> int:
    return sum(1 for group in groups if _leader_tag_count(state, group) >= 1)


def _laggard_group_count(state: ModuleState, groups: list[list[str]]) -> int:
    return sum(1 for group in groups if _laggard_tag_count(state, group) >= 1)


def _leaders_single_group_and_laggards_multi(state: ModuleState, groups: list[list[str]]) -> bool:
    leader_groups = _leader_group_count(state, groups)
    laggard_groups = _laggard_group_count(state, groups)
    return leader_groups == 1 and laggard_groups >= 2


SECTION_TAGS: dict[str, dict[str, list[str]]] = {
    "Market Benchmark": {
        "SPY": ["大盘基准", "整体市场环境", "风险偏好"],
        "QQQ": ["科技成长", "大盘成长股", "风险偏好"],
        "IWM": ["小盘股", "市场宽度", "风险扩散"],
        "DIA": ["传统蓝筹", "老经济", "蓝筹防御"],
    },
    "Sector Classification": {
        "XLK": ["进攻型", "科技", "成长核心"],
        "XLY": ["进攻型", "可选消费", "消费风险偏好"],
        "XLC": ["进攻型", "通信服务", "互联网平台", "广告"],
        "XLI": ["进攻型", "周期型", "工业", "经济活动"],
        "XLF": ["进攻型", "周期型", "金融", "利率敏感"],
        "XLP": ["防御型", "必需消费", "稳定消费"],
        "XLU": ["防御型", "公用事业", "稳定现金流", "利率敏感"],
        "XLV": ["防御型", "医疗保健", "防御成长"],
        "XLRE": ["防御型", "房地产", "租金现金流", "利率敏感"],
        "XLE": ["周期型", "能源", "油价", "商品"],
        "XLB": ["周期型", "原材料", "通胀", "工业需求"],
    },
    "Thematic Classification": {
        "SOXX": ["AI硬件", "半导体", "芯片", "算力"],
        "SMH": ["AI硬件", "半导体龙头", "芯片周期"],
        "WCLD": ["AI应用", "云计算", "SaaS"],
        "SKYY": ["云计算", "云基础设施"],
        "CLOU": ["云计算", "云软件"],
        "IGV": ["AI应用", "企业软件", "软件服务"],
        "CHAT": ["生成式AI", "AI应用"],
        "AIQ": ["人工智能", "技术创新"],
        "BOTZ": ["机器人", "自动化", "AI应用"],
        "ROBO": ["机器人", "工业自动化"],
        "CIBR": ["网络安全", "安全软件"],
        "HACK": ["网络安全", "安全主题"],
        "ICLN": ["清洁能源", "新能源"],
        "TAN": ["太阳能", "清洁能源"],
        "QCLN": ["新能源", "电动车", "清洁技术"],
        "URA": ["核能", "铀矿", "AI电力"],
        "URNM": ["铀矿", "核燃料"],
        "NLR": ["核能", "电力基础设施"],
        "XBI": ["生物科技", "高风险医药"],
        "IBB": ["生物科技龙头", "大型医药创新"],
        "ITA": ["国防", "航空航天", "政府合同"],
        "XAR": ["国防", "航空航天"],
        "PPA": ["国防安全", "政府支出"],
        "BITQ": ["加密股票", "高风险主题"],
        "BLOK": ["区块链", "加密基础设施"],
    },
    "Value Chain Classification": {
        "SOXX": ["上游", "半导体", "AI硬件"],
        "SMH": ["上游", "半导体龙头", "AI硬件"],
        "MAGS": ["中游", "大型科技平台", "AI资本开支方"],
        "QQQ": ["中游", "科技成长平台", "云平台"],
        "WCLD": ["下游", "云软件", "AI应用"],
        "IGV": ["下游", "企业软件", "软件应用"],
        "BOTZ": ["下游", "机器人", "自动化应用"],
        "XLU": ["基础设施", "电力", "数据中心用电"],
        "URA": ["基础设施", "核能", "铀矿"],
        "NLR": ["基础设施", "核能"],
        "IFRA": ["基础设施", "美国基建", "电网建设"],
        "PAVE": ["基础设施", "工程建设", "数据中心建设"],
        "XLB": ["基础设施", "材料", "建材"],
        "XOP": ["能源上游", "油气开采"],
        "XLE": ["综合能源", "能源大盘"],
        "AMLP": ["能源中游", "管道运输"],
        "MLPA": ["能源中游", "能源基础设施"],
        "CRAK": ["能源下游", "炼油"],
        "OIH": ["油服", "能源资本开支"],
        "XES": ["油服设备", "能源服务"],
        "XLRE": ["房地产资产", "REITs"],
        "VNQ": ["房地产资产", "租金现金流"],
        "IYR": ["房地产资产", "REITs"],
        "ITB": ["房屋建造", "住宅周期"],
        "XHB": ["房屋建造", "家装建材"],
    },
    "Style Factor Classification": {
        "VUG": ["成长", "大盘成长"],
        "IWF": ["成长", "Russell 1000成长"],
        "QQQ": ["成长", "科技成长"],
        "MTUM": ["动量", "趋势风格"],
        "QUAL": ["质量", "盈利质量", "低负债"],
        "VLUE": ["价值", "低估值"],
        "VTV": ["价值", "大盘价值"],
        "USMV": ["低波动", "防御"],
        "SPLV": ["低波动", "防御"],
        "SIZE": ["规模因子", "小市值倾向"],
        "IWM": ["小盘", "风险扩散"],
        "VB": ["小盘", "广泛小盘"],
        "SLY": ["小盘", "S&P小盘"],
        "VIG": ["股息增长", "稳定现金流"],
        "SCHD": ["高质量股息", "防御现金流"],
        "SDY": ["股息贵族", "稳定分红"],
    },
    "Business Model Classification": {
        "WCLD": ["SaaS", "订阅软件", "轻资产成长"],
        "IGV": ["企业软件", "软件服务", "轻资产"],
        "MAGS": ["平台型互联网", "网络效应", "大型科技"],
        "QQQ": ["平台科技", "成长平台"],
        "XLC": ["互联网平台", "广告平台", "媒体通信"],
        "IBUY": ["电商", "线上零售"],
        "XLY": ["可选消费", "消费弹性"],
        "XLP": ["防御消费品牌", "稳定消费"],
        "VCR": ["可选消费品牌", "消费平台"],
        "XLI": ["重资产制造", "工业"],
        "VIS": ["工业制造", "重资产"],
        "XLB": ["原材料生产", "材料周期"],
        "XLE": ["资源生产商", "能源"],
        "GDX": ["资源生产商", "黄金矿商"],
        "COPX": ["资源生产商", "铜矿"],
        "XLU": ["稳定现金流", "公用事业"],
        "VPU": ["稳定现金流", "公用事业"],
        "XLRE": ["租金收入", "REITs"],
        "VNQ": ["租金收入", "房地产现金流"],
        "IYR": ["租金收入", "REITs"],
        "XLF": ["金融中介", "银行保险券商"],
        "KRE": ["金融中介", "区域银行"],
        "KBE": ["金融中介", "银行"],
        "IFRA": ["基础设施运营", "基建"],
        "IGF": ["全球基础设施", "稳定资产"],
        "PAVE": ["基建工程", "建设需求"],
    },
    "Revenue Exposure Classification": {
        "XLC": ["广告收入", "互联网平台"],
        "FDN": ["互联网收入", "线上平台"],
        "MAGS": ["平台综合收入", "广告", "云", "硬件"],
        "WCLD": ["订阅收入", "SaaS"],
        "IGV": ["软件收入", "企业软件"],
        "XLK": ["科技收入", "硬件", "软件"],
        "SOXX": ["芯片收入", "半导体"],
        "SMH": ["芯片收入", "AI算力"],
        "QQQ": ["云平台收入", "成长收入"],
        "XLF": ["利息收入", "金融收入"],
        "KRE": ["利息收入", "区域银行"],
        "KBE": ["银行收入", "净息差"],
        "KIE": ["保费收入", "保险"],
        "XLE": ["商品收入", "能源"],
        "XLB": ["商品收入", "材料"],
        "GDX": ["黄金收入", "矿业"],
        "COPX": ["铜收入", "资源"],
        "URA": ["铀矿收入", "核能"],
        "XLRE": ["租金收入", "REITs"],
        "VNQ": ["租金收入", "房地产"],
        "IYR": ["地产收入", "REITs"],
        "XLP": ["防御消费收入", "必需消费"],
        "VDC": ["防御消费收入", "稳定消费"],
        "XLY": ["可选消费收入", "消费周期"],
        "VCR": ["可选消费收入", "消费弹性"],
        "ITA": ["政府合同收入", "国防"],
        "XAR": ["政府合同收入", "航空航天"],
        "PPA": ["政府合同收入", "安全国防"],
    },
    "Supply Chain Relationship Classification": {
        "SOXX": ["核心资产", "芯片", "AI算力"],
        "SMH": ["核心资产", "半导体龙头"],
        "MAGS": ["客户侧确认", "云平台", "AI资本开支方"],
        "QQQ": ["客户侧确认", "科技成长平台"],
        "WCLD": ["应用层", "云软件", "SaaS"],
        "IGV": ["应用层", "企业软件"],
        "XLU": ["电力供应", "数据中心用电"],
        "URA": ["电力供应", "核能", "铀矿"],
        "NLR": ["电力供应", "核能"],
        "IFRA": ["基础设施", "电网", "数据中心建设"],
        "PAVE": ["基础设施", "工程建设"],
        "XLB": ["基础设施材料", "材料需求"],
        "DRIV": ["电动车核心", "EV整车"],
        "IDRV": ["电动车生态", "EV供应链"],
        "QCLN": ["新能源车", "清洁能源"],
        "LIT": ["电池链", "锂"],
        "BATT": ["电池链", "储能"],
        "COPX": ["电气化材料", "铜"],
        "XLV": ["医药核心", "医疗保健"],
        "PPH": ["大药企", "成熟药企"],
        "XBI": ["创新药", "生物科技"],
        "IBB": ["生物科技龙头"],
        "IHI": ["医疗设备"],
        "IHF": ["医疗服务"],
    },
}


def _get_tags(section_title: str, ticker: str) -> list[str]:
    section_key = section_title.split(" / ", maxsplit=1)[0]
    return SECTION_TAGS.get(section_key, {}).get(ticker, [])


def _render_tags(tags: list[str]) -> str:
    if not tags:
        return ""
    tag_items = "".join(f"<span>{tag}</span>" for tag in tags)
    return f'<div class="tag-row">{tag_items}</div>'


def _unique_tickers() -> list[str]:
    seen: set[str] = set()
    tickers: list[str] = []
    for section in MARKET_SECTIONS:
        for item in section.items:
            if item.ticker not in seen:
                tickers.append(item.ticker)
                seen.add(item.ticker)
    return tickers


def _fetch_market_snapshot(tickers: list[str]) -> dict[str, dict]:
    latest = _fetch_latest_prices(tickers)
    moving_averages = _fetch_daily_moving_averages(tickers)

    snapshots: dict[str, dict] = {}
    for ticker in tickers:
        quote = latest.get(ticker, {})
        ma = moving_averages.get(ticker, {})
        current_price = quote.get("current_price")
        previous_close = quote.get("previous_close")
        ma20 = ma.get("ma20")
        ma50 = ma.get("ma50")

        if not all(_is_valid_number(value) for value in (current_price, previous_close, ma20, ma50)):
            snapshots[ticker] = {"ok": False}
            continue

        change_pct = (current_price - previous_close) / previous_close * 100
        ma20_distance_pct = (current_price - ma20) / ma20 * 100
        ma50_distance_pct = (current_price - ma50) / ma50 * 100
        snapshots[ticker] = {
            "ok": True,
            "current_price": current_price,
            "change_pct": change_pct,
            "ma20_distance_pct": ma20_distance_pct,
            "ma50_distance_pct": ma50_distance_pct,
            "trend": _trend_badge(current_price, ma20, ma50),
        }
    return snapshots


def _fetch_latest_prices(tickers: list[str]) -> dict[str, dict]:
    previous_closes = _fetch_previous_closes(tickers)
    try:
        frame = yf.download(
            tickers=tickers,
            period="5d",
            interval="1m",
            group_by="ticker",
            progress=False,
            threads=True,
            auto_adjust=False,
            prepost=False,
        )
    except Exception:
        return {ticker: _fetch_single_latest(ticker, previous_closes.get(ticker)) for ticker in tickers}

    if frame is None or frame.empty:
        return {ticker: _fetch_single_latest(ticker, previous_closes.get(ticker)) for ticker in tickers}

    latest: dict[str, dict] = {}
    for ticker in tickers:
        try:
            ticker_frame = frame[ticker] if isinstance(frame.columns, pd.MultiIndex) else frame
            close = pd.to_numeric(ticker_frame["Close"], errors="coerce").dropna()
            if close.empty:
                latest[ticker] = _fetch_single_latest(ticker, previous_closes.get(ticker))
                continue
            current_price = float(close.iloc[-1])
            previous_close = previous_closes.get(ticker)
            if not _is_valid_number(previous_close):
                previous_close = _fetch_single_latest(ticker).get("previous_close")
            latest[ticker] = {"current_price": current_price, "previous_close": previous_close}
        except Exception:
            latest[ticker] = _fetch_single_latest(ticker, previous_closes.get(ticker))
    return latest


def _fetch_single_latest(ticker: str, previous_close_override: float | None = None) -> dict:
    try:
        fast_info = yf.Ticker(ticker).fast_info
        current_price = fast_info.get("last_price") or fast_info.get("lastPrice")
        previous_close = previous_close_override
        if not _is_valid_number(previous_close):
            previous_close = fast_info.get("previous_close") or fast_info.get("previousClose")
        return {"current_price": float(current_price), "previous_close": float(previous_close)}
    except Exception:
        return {}


def _fetch_previous_closes(tickers: list[str]) -> dict[str, float]:
    try:
        frame = yf.download(
            tickers=tickers,
            period="10d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
            auto_adjust=False,
        )
    except Exception:
        return {}

    if frame is None or frame.empty:
        return {}

    previous_closes: dict[str, float] = {}
    for ticker in tickers:
        try:
            ticker_frame = frame[ticker] if isinstance(frame.columns, pd.MultiIndex) else frame
            close = pd.to_numeric(ticker_frame["Close"], errors="coerce").dropna()
            if len(close) >= 2:
                previous_closes[ticker] = float(close.iloc[-2])
        except Exception:
            continue
    return previous_closes


@st.cache_data(ttl=MA_CACHE_SECONDS, show_spinner=False)
def _fetch_daily_moving_averages(tickers: list[str]) -> dict[str, dict]:
    try:
        frame = yf.download(
            tickers=tickers,
            period="90d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
            auto_adjust=False,
        )
    except Exception:
        return {}

    averages: dict[str, dict] = {}
    for ticker in tickers:
        try:
            ticker_frame = frame[ticker] if isinstance(frame.columns, pd.MultiIndex) else frame
            close = pd.to_numeric(ticker_frame["Close"], errors="coerce").dropna()
            close = _drop_unclosed_daily_bar(close)
            if len(close) < 50:
                continue
            averages[ticker] = {"ma20": float(close.tail(20).mean()), "ma50": float(close.tail(50).mean())}
        except Exception:
            continue
    return averages


def _drop_unclosed_daily_bar(close: pd.Series) -> pd.Series:
    if close.empty:
        return close
    latest_daily_date = pd.Timestamp(close.index[-1]).date()
    market_today = datetime.now(MARKET_TZ).date()
    if latest_daily_date >= market_today and len(close) > 1:
        return close.iloc[:-1]
    return close


def _trend_badge(current_price: float, ma20: float, ma50: float) -> str:
    if current_price > ma20 and current_price > ma50:
        return "Strong / 强势"
    if current_price > ma20 and current_price < ma50:
        return "Short-term Rebound / 短期反弹"
    if current_price < ma20 and current_price > ma50:
        return "Pullback / 回调中"
    return "Weak / 弱势"


def _format_price(value: float) -> str:
    return f"${value:,.2f}" if _is_valid_number(value) else "N/A"


def _format_pct(value: float) -> str:
    if not _is_valid_number(value):
        return "N/A"
    return f"{value:+.2f}%"


def _format_ma_distance(period: int, value: float) -> str:
    if not _is_valid_number(value):
        return "N/A"
    prefix = ">" if value > 0 else "<" if value < 0 else "="
    return f"{prefix}{period} MA {value:+.2f}%"


def _value_class(value: float) -> str:
    if not _is_valid_number(value) or value == 0:
        return "neutral"
    return "positive" if value > 0 else "negative"


def _trend_class(trend: str) -> str:
    if trend.startswith("Strong"):
        return "trend-strong"
    if trend.startswith("Weak"):
        return "trend-weak"
    if trend.startswith("Short-term"):
        return "trend-rebound"
    return "trend-pullback"


def _is_valid_number(value: object) -> bool:
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def _market_css() -> str:
    return """
    <style>
    .market-hero {
        border-bottom: 1px solid rgba(49, 51, 63, 0.14);
        padding: 0.4rem 0 1rem 0;
        margin-bottom: 0.2rem;
    }
    .market-hero .eyebrow {
        color: #5f6368;
        font-size: 0.86rem;
        font-weight: 700;
        letter-spacing: 0;
        text-transform: uppercase;
    }
    .market-hero h1 {
        margin: 0.18rem 0 0.25rem 0;
        font-size: 2rem;
        line-height: 1.16;
    }
    .market-hero p {
        color: #4b5563;
        margin: 0;
        max-width: 980px;
    }
    .etf-card {
        min-height: 390px;
        border: 1px solid rgba(49, 51, 63, 0.16);
        border-radius: 8px;
        padding: 14px;
        margin: 0 0 14px 0;
        background: #ffffff;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    }
    .etf-card.unavailable {
        border-color: rgba(107,114,128,0.28);
        background: #fafafa;
    }
    .card-top {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 8px;
    }
    .ticker {
        color: #111827;
        font-size: 1.28rem;
        font-weight: 800;
        line-height: 1.1;
    }
    .name {
        color: #6b7280;
        font-size: 0.78rem;
        line-height: 1.25;
        margin-top: 3px;
    }
    .category {
        color: #374151;
        font-size: 0.82rem;
        font-weight: 700;
        line-height: 1.3;
        min-height: 34px;
        margin: 10px 0 8px 0;
    }
    .tag-row {
        display: flex;
        flex-wrap: wrap;
        gap: 5px;
        min-height: 28px;
        margin: 0 0 10px 0;
    }
    .tag-row span {
        border: 1px solid rgba(17,24,39,0.10);
        border-radius: 999px;
        background: #f8fafc;
        color: #374151;
        font-size: 0.68rem;
        font-weight: 700;
        line-height: 1.2;
        padding: 4px 7px;
    }
    .diagnosis-panel {
        border: 1px solid rgba(49, 51, 63, 0.14);
        border-radius: 8px;
        background: #fbfcfd;
        padding: 12px 14px;
        margin: 8px 0 16px 0;
    }
    .diagnosis-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-bottom: 10px;
    }
    .diagnosis-meta span {
        border: 1px solid rgba(17,24,39,0.10);
        border-radius: 999px;
        background: #ffffff;
        color: #4b5563;
        font-size: 0.74rem;
        padding: 4px 8px;
    }
    .diagnosis-grid {
        display: grid;
        grid-template-columns: 1fr 1.35fr 1.35fr;
        gap: 12px;
    }
    .diagnosis-grid div {
        min-width: 0;
    }
    .diagnosis-grid span {
        display: block;
        color: #6b7280;
        font-size: 0.72rem;
        font-weight: 700;
        margin-bottom: 4px;
    }
    .diagnosis-grid strong {
        color: #111827;
        font-size: 0.94rem;
        line-height: 1.35;
    }
    .diagnosis-grid p {
        color: #374151;
        font-size: 0.82rem;
        line-height: 1.42;
        margin: 0;
    }
    @media (max-width: 900px) {
        .diagnosis-grid {
            grid-template-columns: 1fr;
        }
    }
    .metrics {
        display: grid;
        gap: 7px;
        margin: 10px 0;
    }
    .metrics div {
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: 8px;
        border-bottom: 1px solid rgba(49, 51, 63, 0.08);
        padding-bottom: 5px;
    }
    .metrics span {
        color: #6b7280;
        font-size: 0.72rem;
        line-height: 1.25;
    }
    .metrics strong {
        color: #111827;
        font-size: 0.92rem;
        white-space: nowrap;
    }
    .positive { color: #c2410c !important; }
    .negative { color: #15803d !important; }
    .neutral { color: #6b7280 !important; }
    .status-badge {
        border-radius: 999px;
        padding: 3px 8px;
        font-size: 0.68rem;
        font-weight: 700;
        white-space: nowrap;
    }
    .status-ok {
        background: #eef2ff;
        color: #3730a3;
    }
    .status-bad {
        background: #f3f4f6;
        color: #6b7280;
    }
    .status-text {
        color: #6b7280;
        font-size: 0.8rem;
        font-weight: 800;
        margin: 6px 0 8px 0;
    }
    .trend {
        display: inline-block;
        border-radius: 6px;
        padding: 5px 8px;
        margin: 2px 0 10px 0;
        font-size: 0.78rem;
        font-weight: 800;
    }
    .trend-strong {
        background: #fee2e2;
        color: #b91c1c;
    }
    .trend-weak {
        background: #dcfce7;
        color: #166534;
    }
    .trend-rebound {
        background: #fff7ed;
        color: #c2410c;
    }
    .trend-pullback {
        background: #ecfdf5;
        color: #047857;
    }
    .static-line {
        color: #374151;
        font-size: 0.78rem;
        line-height: 1.38;
        margin-top: 8px;
    }
    .updated {
        color: #9ca3af;
        font-size: 0.68rem;
        margin-top: 10px;
    }
    </style>
    """
