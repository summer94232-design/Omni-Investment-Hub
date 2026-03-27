# dashboard.py
import asyncio
import json
import yaml
import logging
from datetime import date, datetime
from decimal import Decimal
from aiohttp import web
from datahub.data_hub import DataHub

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def serialize(obj):
    if isinstance(obj, (date, datetime)):
        return str(obj)
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)

async def get_dashboard_data(hub):
    regime = await hub.fetchrow(
        "SELECT regime, mrs_score, vix_level, max_exposure_limit FROM market_regime ORDER BY trade_date DESC LIMIT 1")
    health = await hub.fetchrow(
        "SELECT nav, gross_exposure_pct, portfolio_var_95, circuit_breaker_triggered, drawdown_from_peak FROM portfolio_health ORDER BY snapshot_date DESC LIMIT 1")
    
    # Bug #2 修復：合併持倉顯示
    positions = await hub.fetch("""
        SELECT 
            ticker, MAX(state) as state, SUM(current_shares) as current_shares, 
            ROUND(SUM(avg_cost * current_shares) / NULLIF(SUM(current_shares), 0), 2) as avg_cost,
            SUM(unrealized_pnl) as unrealized_pnl, AVG(r_multiple_current) as r_multiple_current,
            ARRAY_AGG(id::text) as ids
        FROM positions WHERE is_open = TRUE GROUP BY ticker ORDER BY unrealized_pnl DESC NULLS LAST
    """)
    
    stocks = await hub.fetch(
        "SELECT ticker, total_score, score_momentum, score_chip, score_fundamental FROM stock_diagnostic WHERE trade_date = (SELECT MAX(trade_date) FROM stock_diagnostic) ORDER BY total_score DESC LIMIT 50")
    signals = await hub.fetch(
        "SELECT logged_at, decision_type, ticker, signal_source FROM decision_log ORDER BY logged_at DESC LIMIT 20")
    chips = await hub.fetch(
        "SELECT ticker, crs_total FROM chip_monitor WHERE trade_date = (SELECT MAX(trade_date) FROM chip_monitor) ORDER BY crs_total DESC LIMIT 8")
    
    return {
        'regime': dict(regime) if regime else {},
        'health': dict(health) if health else {},
        'positions': [dict(r) for r in positions],
        'stocks': [dict(r) for r in stocks],
        'signals': [dict(r) for r in signals],
        'chips': [dict(r) for r in chips],
    }

async def handle_index(request):
    # 編碼修復：指定 utf-8
    with open('static/index.html', encoding='utf-8') as f:
        content = f.read()
    return web.Response(text=content, content_type='text/html')

async def handle_api_data(request):
    hub = request.app['hub']
    settings = request.app['settings']
    data = await get_dashboard_data(hub)
    
    # Bug #3 修復：Universe 與門檻過濾
    stocks = data.get('stocks', [])
    if settings.get('universe') == 'WATCHLIST':
        watchlist = request.app.get('watchlist', [])
        stocks = [s for s in stocks if s['ticker'].strip() in watchlist]
    
    stocks = sorted(stocks, key=lambda x: x.get('total_score', 0), reverse=True)
    top_pct = settings.get('top_pct', 100)
    if top_pct < 100:
        limit = max(1, int(len(stocks) * (top_pct / 100)))
        stocks = stocks[:limit]
    
    score_thresh = settings.get('score_threshold', 60)
    for s in stocks:
        s['pass_threshold'] = s.get('total_score', 0) >= score_thresh
        
    data['stocks'] = stocks
    return web.Response(text=json.dumps(data, default=serialize, ensure_ascii=False), content_type='application/json')

async def handle_api_settings_save(request):
    body = await request.json()
    request.app['settings'].update(body)
    # 同步到 Redis 供 Engine 讀取
    await request.app['hub'].cache.set("portfolio:settings:current", request.app['settings'], ttl=0)
    return web.Response(text='{"ok":true}', content_type='application/json')

async def handle_api_close_position(request):
    hub = request.app['hub']
    body = await request.json()
    pids = body.get('position_ids', [])
    for pid in pids:
        await hub.execute("UPDATE positions SET is_open=FALSE, exit_date=$1, exit_price=$2 WHERE id=$3", 
                          date.today(), body.get('price', 0), int(pid))
    return web.Response(text='{"ok":true}', content_type='application/json')

async def handle_api_run_scheduler(request):
    # 編碼修復：指定 utf-8
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    from modules.macro_filter import MacroFilter
    from datahub.api_fred import FredAPI
    fred = FredAPI(cfg['fred']['api_key'])
    macro = MacroFilter(request.app['hub'], fred)
    res = await macro.run()
    await fred.close()
    return web.Response(text=json.dumps({'ok':True, 'regime':res['regime']}), content_type='application/json')

async def create_app():
    hub = DataHub('config.yaml')
    await hub.connect()
    app = web.Application()
    app['hub'] = hub
    app['watchlist'] = ['2330', '2317', '2454']
    app['settings'] = {
        'atr_mult': 3.0, 'trail_pct': 15, 'score_threshold': 60,
        'universe': 'ALL', 'top_pct': 100,
        'weights': {'momentum': 0.3, 'chip': 0.3, 'fundamental': 0.4}
    }
    app.router.add_get('/', handle_index)
    app.router.add_get('/api/data', handle_api_data)
    app.router.add_post('/api/settings/save', handle_api_settings_save)
    app.router.add_post('/api/close-position', handle_api_close_position)
    app.router.add_post('/api/run-scheduler', handle_api_run_scheduler)
    return app

if __name__ == '__main__':
    web.run_app(create_app(), port=8080)