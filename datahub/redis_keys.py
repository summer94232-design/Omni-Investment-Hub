# datahub/redis_keys.py
# Redis Key 命名規範與 TTL 常數

# TTL 常數（秒）
TTL_5MIN   = 300
TTL_10MIN  = 600
TTL_1H     = 3_600
TTL_24H    = 86_400
TTL_PERM   = 0          # 永久（不設 TTL）

# Key 產生函式
def key_market_regime_latest() -> str:
    return "market:regime:latest"

def key_market_regime_date(date: str) -> str:
    return f"market:regime:{date}"

def key_chip_crs(ticker: str, date: str) -> str:
    return f"chip:crs:{ticker}:{date}"

def key_chip_crs_latest(ticker: str) -> str:
    return f"chip:crs:{ticker}:latest"

def key_portfolio_state_latest() -> str:
    return "portfolio:state:latest"

def key_portfolio_exposure() -> str:
    return "portfolio:exposure:current"

def key_gate_pending(order_id: str) -> str:
    return f"gate:pending:{order_id}"

def key_wf_weights_current() -> str:
    return "wf:weights:current"

def key_signal(ticker: str, date: str) -> str:
    return f"signal:{ticker}:{date}"

def key_event_rules(date: str) -> str:
    return f"event:rules:{date}"

# [BUG 11 修正] 新增 market:basis:latest 的統一管理函式
# 原本 selection_engine.py:92 與 macro_filter.py:378 各自 hardcode 此字串
def key_market_basis_latest() -> str:
    """台指期基差快取 Key（TTL=300s，供 BasisFilter 快速讀取）"""
    return "market:basis:latest"