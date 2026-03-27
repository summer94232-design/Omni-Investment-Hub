# dashboard.py
import asyncio
import json
import logging
import yaml
from datetime import date, datetime
from decimal import Decimal
from aiohttp import web
from datahub.data_hub import DataHub


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
    positions = await hub.fetch(
        "SELECT id, ticker, state, entry_price, current_stop_price, r_multiple_current, unrealized_pnl FROM positions WHERE is_open = TRUE ORDER BY unrealized_pnl DESC NULLS LAST")
    stocks = await hub.fetch(
        "SELECT ticker, total_score, score_momentum, score_chip, score_fundamental FROM stock_diagnostic WHERE trade_date = (SELECT MAX(trade_date) FROM stock_diagnostic) ORDER BY total_score DESC LIMIT 10")
    signals = await hub.fetch(
        "SELECT logged_at, decision_type, ticker, signal_source FROM decision_log ORDER BY logged_at DESC LIMIT 20")
    chips = await hub.fetch(
        "SELECT ticker, crs_total, three_way_resonance, alerts FROM chip_monitor WHERE trade_date = (SELECT MAX(trade_date) FROM chip_monitor) ORDER BY crs_total DESC LIMIT 8")
    return {
        'regime':    dict(regime)    if regime    else {},
        'health':    dict(health)    if health    else {},
        'positions': [dict(r) for r in positions],
        'stocks':    [dict(r) for r in stocks],
        'signals':   [dict(r) for r in signals],
        'chips':     [dict(r) for r in chips],
    }


async def handle_index(request):
    with open('static/index.html', encoding='utf-8') as f:
        content = f.read()
    return web.Response(text=content, content_type='text/html')


async def handle_api_data(request):
    hub = request.app['hub']
    data = await get_dashboard_data(hub)
    return web.Response(text=json.dumps(data, default=serialize, ensure_ascii=False), content_type='application/json')


async def handle_api_settings(request):
    return web.Response(text=json.dumps(request.app['settings'], ensure_ascii=False), content_type='application/json')


async def handle_api_settings_save(request):
    body = await request.json()
    request.app['settings'].update(body)
    return web.Response(text='{"ok":true}', content_type='application/json')


async def handle_api_close_position(request):
    hub = request.app['hub']
    body = await request.json()
    pid = body.get('position_id')
    await hub.execute("""
        UPDATE positions SET is_open=FALSE, exit_date=$1, exit_price=$2,
        exit_reason='MANUAL_OVERRIDE' WHERE id=$3
    """, date.today(), body.get('price', 0), pid)
    return web.Response(text='{"ok":true}', content_type='application/json')


async def handle_api_add_position(request):
    hub = request.app['hub']
    try:
        body   = await request.json()
        ticker = body['ticker'].ljust(6)[:6]
        entry  = float(body['entry_price'])
        shares = int(body['shares'])
        atr    = float(body.get('atr', 15.0))
        mult   = float(body.get('atr_multiplier', 3.0))
        stop   = round(entry - atr * mult, 4)
        r      = round(atr * mult, 4)
        await hub.execute("""
            INSERT INTO positions (
                ticker, state, entry_date, entry_price,
                initial_shares, current_shares, signal_source,
                atr_at_entry, atr_multiplier, initial_stop_price,
                r_amount, current_stop_price, trail_pct, avg_cost
            ) VALUES ($1,'S1_INITIAL_DEFENSE',$2,$3,$4,$4,'MANUAL',$5,$6,$7,$8,$7,0.15,$3)
        """, ticker, date.today(), entry, shares, atr, mult, stop, r)
        return web.Response(text='{"ok":true}', content_type='application/json')
    except Exception as e:
        logging.getLogger(__name__).error("add-position 失敗：%s", e)
        return web.Response(
            text=json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False),
            content_type='application/json'
        )


async def handle_api_add_event(request):
    hub = request.app['hub']
    body = await request.json()
    await hub.execute("""
        INSERT INTO calendar_events (event_date, event_type, name, impact_level)
        VALUES ($1,$2,$3,$4)
    """, date.fromisoformat(body['event_date']), body['event_type'], body['name'], int(body['impact_level']))
    return web.Response(text='{"ok":true}', content_type='application/json')


async def handle_api_watchlist(request):
    return web.Response(text=json.dumps(request.app['watchlist'], ensure_ascii=False), content_type='application/json')


async def handle_api_watchlist_add(request):
    body = await request.json()
    t = body.get('ticker', '').strip().upper()
    if t and t not in request.app['watchlist']:
        request.app['watchlist'].append(t)
    return web.Response(text=json.dumps(request.app['watchlist']), content_type='application/json')


async def handle_api_watchlist_remove(request):
    body = await request.json()
    t = body.get('ticker', '').strip().upper()
    if t in request.app['watchlist']:
        request.app['watchlist'].remove(t)
    return web.Response(text=json.dumps(request.app['watchlist']), content_type='application/json')


async def handle_api_run_scheduler(request):
    hub = request.app['hub']
    try:
        with open('config.yaml', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
        from datahub.api_fred import FredAPI
        from modules.macro_filter import MacroFilter
        fred   = FredAPI(cfg['fred']['api_key'])
        macro  = MacroFilter(hub, fred)
        result = await macro.run()
        await fred.close()
        return web.Response(text=json.dumps({'ok': True, 'regime': result['regime'], 'mrs': result['mrs_score']}), content_type='application/json')
    except Exception as e:
        return web.Response(text=json.dumps({'ok': False, 'error': str(e)}), content_type='application/json')


async def handle_api_ev_simulate(request):
    hub  = request.app['hub']
    body = await request.json()
    from modules.ev_simulator import EVSimulator
    ev  = EVSimulator(hub)
    res = await ev.run(body.get('ticker', 'TEST'), float(body.get('score', 60)))
    return web.Response(text=json.dumps(res, default=serialize, ensure_ascii=False), content_type='application/json')


async def handle_api_black_swan(request):
    hub = request.app['hub']
    try:
        with open('config.yaml', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
        from datahub.api_telegram import TelegramBot
        from modules.black_swan import BlackSwan
        telegram = TelegramBot(cfg['telegram']['bot_token'], cfg['telegram']['chat_id'])
        bs  = BlackSwan(hub, telegram)
        res = await bs.run()
        await telegram.close()
        return web.Response(text=json.dumps(res, default=serialize, ensure_ascii=False), content_type='application/json')
    except Exception as e:
        return web.Response(text=json.dumps({'ok': False, 'error': str(e)}), content_type='application/json')


async def handle_api_walk_forward(request):
    hub = request.app['hub']
    try:
        from modules.walk_forward import WalkForward
        wf  = WalkForward(hub)
        res = await wf.run()
        return web.Response(text=json.dumps(res, default=serialize, ensure_ascii=False), content_type='application/json')
    except Exception as e:
        return web.Response(text=json.dumps({'ok': False, 'error': str(e)}), content_type='application/json')


async def create_app():
    hub = DataHub('config.yaml')
    await hub.connect()

    app = web.Application()
    app['hub'] = hub
    app['watchlist'] = ['2330', '2303', '2454', '2412', '2317', '2382', '3711', '6669']
    app['settings']  = {
        'mode': 'PAPER', 'atr_mult': 3.0,
        'trail_pct': 15, 'score_threshold': 60, 'ev_threshold': 0.5
    }

    app.router.add_get('/',                      handle_index)
    app.router.add_get('/api/data',              handle_api_data)
    app.router.add_get('/api/settings',          handle_api_settings)
    app.router.add_get('/api/watchlist',         handle_api_watchlist)
    app.router.add_post('/api/settings/save',    handle_api_settings_save)
    app.router.add_post('/api/close-position',   handle_api_close_position)
    app.router.add_post('/api/add-position',     handle_api_add_position)
    app.router.add_post('/api/add-event',        handle_api_add_event)
    app.router.add_post('/api/watchlist/add',    handle_api_watchlist_add)
    app.router.add_post('/api/watchlist/remove', handle_api_watchlist_remove)
    app.router.add_post('/api/run-scheduler',    handle_api_run_scheduler)
    app.router.add_post('/api/ev-simulate',      handle_api_ev_simulate)
    app.router.add_post('/api/black-swan',       handle_api_black_swan)
    app.router.add_post('/api/walk-forward',     handle_api_walk_forward)

    return app


if __name__ == '__main__':
    async def main():
        app = await create_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', 8080)
        await site.start()
        print("Dashboard 啟動：http://localhost:8080")
        print("按 Ctrl+C 停止")
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            pass
        finally:
            await runner.cleanup()

    asyncio.run(main())