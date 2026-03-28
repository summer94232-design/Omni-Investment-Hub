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

    # 先撈全部開倉列，在 Python 層做 ticker 合併
    # 完全避開 asyncpg array_agg 型別序列化問題
    raw_pos = await hub.fetch("""
        SELECT
            id::text         AS id,
            TRIM(ticker)     AS ticker,
            state,
            current_shares,
            avg_cost,
            unrealized_pnl,
            realized_pnl,
            r_multiple_current,
            current_stop_price,
            entry_price,
            entry_date
        FROM positions
        WHERE is_open = TRUE
        ORDER BY entry_date ASC
    """)

    # Python 層按 ticker 合併
    ticker_map = {}
    for row in raw_pos:
        r = dict(row)
        t = r['ticker']
        shares = int(r['current_shares'] or 0)
        avg    = float(r['avg_cost'] or r['entry_price'] or 0)
        r_mult = float(r['r_multiple_current'] or 0)

        if t not in ticker_map:
            ticker_map[t] = {
                'ticker':             t,
                'ids':                [],
                'current_shares':     0,
                'weighted_cost_sum':  0.0,
                'unrealized_pnl':     0.0,
                'realized_pnl':       0.0,
                'r_weighted_sum':     0.0,
                'state':              r['state'],
                'current_stop_price': r['current_stop_price'],
                'entry_price':        r['entry_price'],
                '_max_shares':        0,
            }
        b = ticker_map[t]
        b['ids'].append(r['id'])
        b['current_shares']    += shares
        b['weighted_cost_sum'] += avg * shares
        b['unrealized_pnl']    += float(r['unrealized_pnl'] or 0)
        b['realized_pnl']      += float(r['realized_pnl'] or 0)
        b['r_weighted_sum']    += r_mult * shares
        if shares > b['_max_shares']:
            b['_max_shares']        = shares
            b['state']              = r['state']
            b['current_stop_price'] = r['current_stop_price']

    positions = []
    for t, b in ticker_map.items():
        total = b['current_shares']
        positions.append({
            'ticker':             t,
            'ids':                b['ids'],
            'current_shares':     total,
            'avg_cost':           round(b['weighted_cost_sum'] / total, 4) if total else 0,
            'unrealized_pnl':     round(b['unrealized_pnl'], 2),
            'realized_pnl':       round(b['realized_pnl'], 2),
            'r_multiple_current': round(b['r_weighted_sum'] / total, 3) if total else 0,
            'state':              b['state'],
            'current_stop_price': b['current_stop_price'],
            'entry_price':        b['entry_price'],
        })
    positions.sort(key=lambda x: x['unrealized_pnl'], reverse=True)

    stocks = await hub.fetch(
        "SELECT ticker, total_score, score_momentum, score_chip, score_fundamental FROM stock_diagnostic WHERE trade_date = (SELECT MAX(trade_date) FROM stock_diagnostic) ORDER BY total_score DESC LIMIT 10")
    signals = await hub.fetch(
        "SELECT logged_at, decision_type, ticker, signal_source FROM decision_log ORDER BY logged_at DESC LIMIT 20")
    chips = await hub.fetch(
        "SELECT ticker, crs_total, three_way_resonance, alerts FROM chip_monitor WHERE trade_date = (SELECT MAX(trade_date) FROM chip_monitor) ORDER BY crs_total DESC LIMIT 8")

    return {
        'regime':    dict(regime)    if regime    else {},
        'health':    dict(health)    if health    else {},
        'positions': positions,
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
    hub  = request.app['hub']
    body = await request.json()
    try:
        ids = body.get('position_ids') or (
            [body['position_id']] if body.get('position_id') else []
        )
        if not ids:
            return web.Response(
                text=json.dumps({"ok": False, "error": "未傳入 position_id"}),
                content_type='application/json', status=400
            )

        exit_price = float(body.get('price', 0))
        today      = date.today()

        for pid in ids:
            row = await hub.fetchrow(
                "SELECT avg_cost, current_shares, realized_pnl FROM positions WHERE id = $1", pid
            )
            if not row:
                continue
            avg_cost      = float(row['avg_cost'] or 0)
            shares        = int(row['current_shares'] or 0)
            prev_realized = float(row['realized_pnl'] or 0)
            realized_pnl  = round((exit_price - avg_cost) * shares + prev_realized, 2)

            await hub.execute("""
                UPDATE positions SET
                    is_open        = FALSE,
                    state          = 'CLOSED',
                    exit_date      = $1,
                    exit_price     = $2,
                    exit_reason    = 'MANUAL_OVERRIDE',
                    realized_pnl   = $3,
                    unrealized_pnl = 0,
                    current_shares = 0
                WHERE id = $4
            """, today, exit_price, realized_pnl, pid)

        return web.Response(text='{"ok":true}', content_type='application/json')

    except Exception as e:
        logging.getLogger(__name__).error("close-position 失敗：%s", e)
        return web.Response(
            text=json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False),
            content_type='application/json', status=500
        )


async def handle_api_add_position(request):
    hub = request.app['hub']
    try:
        body   = await request.json()
        ticker = body['ticker'].strip().ljust(6)[:6]
        entry  = float(body['entry_price'])
        shares = int(body['shares'])
        atr    = float(body.get('atr', 15.0))
        mult   = float(body.get('atr_multiplier', 3.0))
        stop   = round(entry - atr * mult, 4)
        r      = round(atr * mult, 4)

        existing = await hub.fetchrow("""
            SELECT id, current_shares, avg_cost
            FROM positions
            WHERE ticker = $1 AND is_open = TRUE
            ORDER BY entry_date DESC LIMIT 1
        """, ticker)

        if existing:
            old_shares   = int(existing['current_shares'])
            old_avg_cost = float(existing['avg_cost'] or entry)
            new_shares   = old_shares + shares
            new_avg_cost = round(
                (old_shares * old_avg_cost + shares * entry) / new_shares, 4
            )
            await hub.execute("""
                UPDATE positions SET
                    current_shares = $1,
                    avg_cost       = $2,
                    addon_done     = TRUE,
                    addon_shares   = addon_shares + $3,
                    addon_price    = $4
                WHERE id = $5
            """, new_shares, new_avg_cost, shares, entry, str(existing['id']))
            action = 'ADDON'
        else:
            await hub.execute("""
                INSERT INTO positions (
                    ticker, state, entry_date, entry_price,
                    initial_shares, current_shares, signal_source,
                    atr_at_entry, atr_multiplier, initial_stop_price,
                    r_amount, current_stop_price, trail_pct, avg_cost,
                    realized_pnl
                ) VALUES (
                    $1, 'S1_INITIAL_DEFENSE', $2, $3,
                    $4, $4, 'MANUAL',
                    $5, $6, $7,
                    $8, $7, 0.15, $3,
                    0
                )
            """, ticker, date.today(), entry, shares, atr, mult, stop, r)
            action = 'NEW_POSITION'

        return web.Response(
            text=json.dumps({"ok": True, "action": action}, ensure_ascii=False),
            content_type='application/json'
        )

    except Exception as e:
        logging.getLogger(__name__).error("add-position 失敗：%s", e)
        return web.Response(
            text=json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False),
            content_type='application/json', status=500
        )


async def handle_api_add_event(request):
    hub  = request.app['hub']
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
    app['hub']       = hub
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