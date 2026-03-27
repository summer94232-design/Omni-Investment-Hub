# dashboard.py
import asyncio
import json
import yaml
import logging
from datetime import date, datetime
from decimal import Decimal
from aiohttp import web
from datahub.data_hub import DataHub

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
        'regime':    dict(regime) if regime else {},
        'health':    dict(health) if health else {},
        'positions': [dict(r) for r in positions],
        'stocks':    [dict(r) for r in stocks],
        'signals':   [dict(r) for r in signals],
        'chips':     [dict(r) for r in chips],
    }


# ---------------------------------------------------------------------------
# GET handlers
# ---------------------------------------------------------------------------

async def handle_index(request):
    with open('static/index.html', encoding='utf-8') as f:
        content = f.read()
    return web.Response(text=content, content_type='text/html')


async def handle_api_data(request):
    hub = request.app['hub']
    settings = request.app['settings']
    data = await get_dashboard_data(hub)
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


async def handle_api_settings(request):
    return web.Response(
        text=json.dumps(request.app['settings'], ensure_ascii=False),
        content_type='application/json')


async def handle_api_watchlist(request):
    return web.Response(
        text=json.dumps(request.app['watchlist'], ensure_ascii=False),
        content_type='application/json')


# ---------------------------------------------------------------------------
# POST handlers
# ---------------------------------------------------------------------------

async def handle_api_settings_save(request):
    body = await request.json()
    request.app['settings'].update(body)
    await request.app['hub'].cache.set("portfolio:settings:current", request.app['settings'], ttl=0)
    return web.Response(text='{"ok":true}', content_type='application/json')


async def handle_api_close_position(request):
    hub = request.app['hub']
    body = await request.json()
    pid = body.get('position_id')
    price = float(body.get('price', 0))
    try:
        await hub.execute("""
            UPDATE positions SET is_open=FALSE, exit_date=$1, exit_price=$2,
            exit_reason='MANUAL_OVERRIDE', state='CLOSED' WHERE id=$3
        """, date.today(), price, int(pid))
        return web.Response(text='{"ok":true}', content_type='application/json')
    except Exception as e:
        return web.Response(text=f'{{"ok":false,"error":"{e}"}}', content_type='application/json')


async def handle_api_add_position(request):
    hub = request.app['hub']
    body = await request.json()
    ticker = body['ticker'].ljust(6)[:6]
    entry  = float(body['entry_price'])
    shares = int(body['shares'])
    atr    = float(body.get('atr', 15.0))
    mult   = float(body.get('atr_multiplier', 3.0))
    stop   = round(entry - atr * mult, 4)
    r      = round(atr * mult, 4)
    try:
        await hub.execute("""
            INSERT INTO positions (
                ticker, state, entry_date, entry_price,
                initial_shares, current_shares, signal_source,
                atr_at_entry, atr_multiplier, initial_stop_price,
                r_amount, current_stop_price, trail_pct, avg_cost
            ) VALUES ($1,'HOLD',$2,$3,$4,$4,'MANUAL',$5,$6,$7,$8,$7,0.15,$3)
        """, ticker, date.today(), entry, shares, atr, mult, stop, r)
        return web.Response(text='{"ok":true}', content_type='application/json')
    except Exception as e:
        return web.Response(text=f'{{"ok":false,"error":"{e}"}}', content_type='application/json')


async def handle_api_watchlist_add(request):
    body = await request.json()
    ticker = body.get('ticker', '').strip().upper()
    if ticker and ticker not in request.app['watchlist']:
        request.app['watchlist'].append(ticker)
    return web.Response(text='{"ok":true}', content_type='application/json')


async def handle_api_watchlist_remove(request):
    body = await request.json()
    ticker = body.get('ticker', '').strip().upper()
    if ticker in request.app['watchlist']:
        request.app['watchlist'].remove(ticker)
    return web.Response(text='{"ok":true}', content_type='application/json')


async def handle_api_run_scheduler(request):
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    try:
        from modules.macro_filter import MacroFilter
        from datahub.api_fred import FredAPI
        fred = FredAPI(cfg['fred']['api_key'])
        macro = MacroFilter(request.app['hub'], fred)
        res = await macro.run()
        await fred.close()
        return web.Response(
            text=json.dumps({'ok': True, 'regime': res.get('regime', ''), 'mrs': res.get('mrs_score', 0)}),
            content_type='application/json')
    except Exception as e:
        return web.Response(text=f'{{"ok":false,"error":"{e}"}}', content_type='application/json')


async def handle_api_ev_simulate(request):
    body = await request.json()
    ticker = body.get('ticker', 'TEST')
    score  = float(body.get('score', 60))
    try:
        from modules.ev_simulator import EVSimulator
        sim = EVSimulator(request.app['hub'])
        res = await sim.simulate(ticker=ticker, score=score)
        return web.Response(text=json.dumps(res, default=serialize, ensure_ascii=False), content_type='application/json')
    except Exception as e:
        return web.Response(text=f'{{"ok":false,"error":"{e}"}}', content_type='application/json')


async def handle_api_black_swan(request):
    try:
        from modules.black_swan import BlackSwanDetector
        detector = BlackSwanDetector(request.app['hub'])
        res = await detector.check()
        return web.Response(text=json.dumps(res, default=serialize, ensure_ascii=False), content_type='application/json')
    except Exception as e:
        return web.Response(text=f'{{"ok":false,"triggered":0,"error":"{e}"}}', content_type='application/json')


async def handle_api_walk_forward(request):
    try:
        from modules.walk_forward import WalkForward
        wf = WalkForward(request.app['hub'])
        res = await wf.run()
        return web.Response(text=json.dumps(res, default=serialize, ensure_ascii=False), content_type='application/json')
    except Exception as e:
        return web.Response(text=f'{{"ok":false,"error":"{e}"}}', content_type='application/json')


async def handle_api_add_event(request):
    hub = request.app['hub']
    body = await request.json()
    try:
        await hub.execute("""
            INSERT INTO calendar_events (event_date, event_type, name, impact_level)
            VALUES ($1, $2, $3, $4)
        """,
            date.fromisoformat(body['event_date']),
            body.get('event_type', 'OTHER'),
            body.get('name', ''),
            body.get('impact_level', 'MEDIUM'))
        return web.Response(text='{"ok":true}', content_type='application/json')
    except Exception as e:
        return web.Response(text=f'{{"ok":false,"error":"{e}"}}', content_type='application/json')


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

async def create_app():
    hub = DataHub('config.yaml')
    await hub.connect()
    app = web.Application()
    app['hub'] = hub
    app['watchlist'] = ['2330', '2317', '2454', '2412', '2303', '2382', '3711', '6669']
    app['settings'] = {
        'atr_mult': 3.0, 'trail_pct': 15, 'score_threshold': 60,
        'universe': 'ALL', 'top_pct': 100,
        'weights': {'momentum': 0.3, 'chip': 0.3, 'fundamental': 0.4}
    }

    # GET
    app.router.add_get('/',                   handle_index)
    app.router.add_get('/api/data',           handle_api_data)
    app.router.add_get('/api/settings',       handle_api_settings)
    app.router.add_get('/api/watchlist',      handle_api_watchlist)

    # POST
    app.router.add_post('/api/settings/save',    handle_api_settings_save)
    app.router.add_post('/api/close-position',   handle_api_close_position)
    app.router.add_post('/api/add-position',     handle_api_add_position)
    app.router.add_post('/api/watchlist/add',    handle_api_watchlist_add)
    app.router.add_post('/api/watchlist/remove', handle_api_watchlist_remove)
    app.router.add_post('/api/run-scheduler',    handle_api_run_scheduler)
    app.router.add_post('/api/ev-simulate',      handle_api_ev_simulate)
    app.router.add_post('/api/black-swan',       handle_api_black_swan)
    app.router.add_post('/api/walk-forward',     handle_api_walk_forward)
    app.router.add_post('/api/add-event',        handle_api_add_event)

    return app


if __name__ == '__main__':
    web.run_app(create_app(), port=8080)