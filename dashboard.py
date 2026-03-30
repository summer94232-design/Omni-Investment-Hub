# dashboard.py  ── v3.3.1 完整版
# ═══════════════════════════════════════════════════════════════════════════════
# Bug 修正（v3.1）：
# - [BUG 8]  handle_api_run_scheduler 改為呼叫 Scheduler.run_daily()
# - [BUG 12] 所有檔案路徑改用 BASE_DIR 絕對路徑
# 新增（v3.3.1）：
# - [🟢 修復] handle_api_approve_order：POST /api/approve-order/{order_id}
#             讓操盤手透過 Dashboard 確認大單，修復 gate3_human_approval 永遠逾時問題
# - [🟢 修復] create_app() 中同步載入 Watchlist（從 DB，降級使用靜態清單）
# ═══════════════════════════════════════════════════════════════════════════════

import asyncio
import json
import logging
import os
import yaml
from datetime import date, datetime
from decimal import Decimal
from aiohttp import web
from datahub.data_hub import DataHub
from datahub import redis_keys as rk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

WATCHLIST_FALLBACK = ['2330', '2303', '2454', '2412', '2317', '2382', '3711', '6669']


def serialize(obj):
    if isinstance(obj, (date, datetime)):
        return str(obj)
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)


# =============================================================================
# Dashboard 資料聚合
# =============================================================================

async def get_dashboard_data(hub):
    regime = await hub.fetchrow("""
        SELECT regime, mrs_score, vix_level, max_exposure_limit,
               tx_futures_price, tsec_index_price, dividend_adjustment
        FROM market_regime ORDER BY trade_date DESC LIMIT 1
    """)
    health = await hub.fetchrow("""
        SELECT nav, gross_exposure_pct, portfolio_var_95,
               circuit_breaker_triggered, drawdown_from_peak
        FROM portfolio_health ORDER BY snapshot_date DESC LIMIT 1
    """)
    positions = await hub.fetch("""
        SELECT id, ticker, state, entry_price, current_stop_price,
               r_multiple_current, unrealized_pnl, current_shares,
               s1_atr_mult_applied, trail_pct, atr_at_entry
        FROM positions WHERE is_open = TRUE
        ORDER BY unrealized_pnl DESC NULLS LAST
    """)
    closed_positions = await hub.fetch("""
        SELECT ticker, r_multiple_current, realized_pnl,
               regime_at_entry, exit_date, individual_alpha
        FROM positions
        WHERE is_open = FALSE AND exit_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY exit_date DESC LIMIT 20
    """)
    stocks = await hub.fetch("""
        SELECT ticker, total_score, score_momentum, score_chip,
               score_fundamental, sector, sector_bonus, sector_beta_mult,
               basis_filter_reason
        FROM stock_diagnostic
        WHERE trade_date = (SELECT MAX(trade_date) FROM stock_diagnostic)
        ORDER BY total_score DESC LIMIT 10
    """)
    signals = await hub.fetch("""
        SELECT logged_at, decision_type, ticker, signal_source, individual_alpha
        FROM decision_log ORDER BY logged_at DESC LIMIT 20
    """)
    chips = await hub.fetch("""
        SELECT ticker, crs_total, three_way_resonance, alerts
        FROM chip_monitor
        WHERE trade_date = (SELECT MAX(trade_date) FROM chip_monitor)
        ORDER BY crs_total DESC LIMIT 8
    """)
    attribution_row = await hub.fetchrow("""
        SELECT notes FROM wf_results
        ORDER BY run_at DESC LIMIT 1
    """)
    attribution_notes = {}
    if attribution_row and attribution_row['notes']:
        try:
            attribution_notes = json.loads(attribution_row['notes'])
        except Exception:
            pass

    liquidity_signals = await hub.fetch("""
        SELECT logged_at, ticker, signal_source, notes
        FROM decision_log
        WHERE signal_source = 'BLACK_SWAN'
          AND logged_at >= NOW() - INTERVAL '7 days'
        ORDER BY logged_at DESC LIMIT 5
    """)

    regime_thresholds = {}
    if attribution_notes.get('regime_ev_feedback'):
        for reg, fb in attribution_notes['regime_ev_feedback'].items():
            if fb.get('triggered'):
                regime_thresholds[reg] = fb.get('new_threshold', 65)

    return {
        'regime':            dict(regime)  if regime  else {},
        'health':            dict(health)  if health  else {},
        'positions':         [dict(r) for r in positions],
        'closed_positions':  [dict(r) for r in closed_positions],
        'stocks':            [dict(r) for r in stocks],
        'signals':           [dict(r) for r in signals],
        'chips':             [dict(r) for r in chips],
        'attribution':       attribution_notes,
        'liquidity_signals': [dict(r) for r in liquidity_signals],
        'regime_thresholds': regime_thresholds,
    }


# =============================================================================
# 路由 Handlers
# =============================================================================

async def handle_index(request):
    index_path = os.path.join(BASE_DIR, 'static', 'index.html')
    with open(index_path, encoding='utf-8') as f:
        content = f.read()
    return web.Response(text=content, content_type='text/html')


async def handle_api_data(request):
    hub = request.app['hub']
    try:
        data = await get_dashboard_data(hub)
        return web.Response(
            text=json.dumps(data, default=serialize, ensure_ascii=False),
            content_type='application/json'
        )
    except Exception as e:
        logger.exception('handle_api_data 失敗')
        return web.Response(
            text=json.dumps({'error': str(e)}, ensure_ascii=False),
            content_type='application/json',
            status=500,
        )


async def handle_api_settings(request):
    return web.Response(
        text=json.dumps(request.app['settings'], ensure_ascii=False),
        content_type='application/json'
    )


async def handle_api_settings_save(request):
    body = await request.json()
    request.app['settings'].update(body)
    return web.Response(text='{"ok":true}', content_type='application/json')


async def handle_api_watchlist(request):
    return web.Response(
        text=json.dumps(request.app['watchlist'], ensure_ascii=False),
        content_type='application/json'
    )


async def handle_api_watchlist_add(request):
    hub  = request.app['hub']
    body = await request.json()
    t    = body.get('ticker', '').strip().upper()
    if t and t not in request.app['watchlist']:
        request.app['watchlist'].append(t)
        # 同步寫入 DB（若 watchlist 資料表存在）
        try:
            await hub.execute("""
                INSERT INTO watchlist (ticker, is_active, added_at)
                VALUES ($1, TRUE, NOW())
                ON CONFLICT (ticker) DO UPDATE SET is_active = TRUE
            """, t)
        except Exception:
            pass  # 資料表不存在時靜默忽略
    return web.Response(
        text=json.dumps(request.app['watchlist']),
        content_type='application/json'
    )


async def handle_api_watchlist_remove(request):
    hub  = request.app['hub']
    body = await request.json()
    t    = body.get('ticker', '').strip().upper()
    if t in request.app['watchlist']:
        request.app['watchlist'].remove(t)
        try:
            await hub.execute("""
                UPDATE watchlist SET is_active = FALSE WHERE ticker = $1
            """, t)
        except Exception:
            pass
    return web.Response(
        text=json.dumps(request.app['watchlist']),
        content_type='application/json'
    )


async def handle_api_close_position(request):
    hub  = request.app['hub']
    body = await request.json()
    pid  = body.get('position_id')
    await hub.execute("""
        UPDATE positions SET is_open=FALSE, exit_date=$1, exit_price=$2,
        exit_reason='MANUAL_OVERRIDE', state='CLOSED' WHERE id=$3
    """, date.today(), body.get('price', 0), pid)
    return web.Response(text='{"ok":true}', content_type='application/json')


async def handle_api_reduce_position(request):
    hub  = request.app['hub']
    body = await request.json()
    pid         = body.get('position_id')
    exit_shares = int(body.get('exit_shares', 0))
    exit_price  = float(body.get('exit_price', 0))
    if not pid or exit_shares <= 0 or exit_price <= 0:
        return web.Response(
            text=json.dumps({'ok': False, 'error': '請填入 position_id、exit_shares、exit_price'}),
            content_type='application/json', status=400,
        )
    try:
        from modules.position_manager import PositionManager
        pm     = PositionManager(hub)
        result = await pm.partial_exit(pid, exit_shares, exit_price)
        return web.Response(
            text=json.dumps({'ok': True, **result}, default=serialize, ensure_ascii=False),
            content_type='application/json',
        )
    except ValueError as e:
        return web.Response(
            text=json.dumps({'ok': False, 'error': str(e)}),
            content_type='application/json', status=400,
        )
    except Exception as e:
        return web.Response(
            text=json.dumps({'ok': False, 'error': str(e)}),
            content_type='application/json', status=500,
        )


async def handle_api_merge_positions(request):
    """
    相同 ticker 的多筆開倉合併為一筆。
    Body: { "ticker": "2330" }（不傳則合併所有重複 ticker）
    """
    hub  = request.app['hub']
    body = await request.json()
    target_ticker = body.get('ticker', '').strip().upper()

    try:
        if target_ticker:
            ticker_padded = target_ticker.ljust(6)[:6]
            dup_rows = await hub.fetch("""
                SELECT ticker FROM positions
                WHERE is_open = TRUE AND ticker = $1
                GROUP BY ticker HAVING COUNT(*) > 1
            """, ticker_padded)
        else:
            dup_rows = await hub.fetch("""
                SELECT ticker FROM positions
                WHERE is_open = TRUE
                GROUP BY ticker HAVING COUNT(*) > 1
            """)

        if not dup_rows:
            return web.Response(
                text=json.dumps({'ok': True, 'merged': 0, 'message': '沒有需要合併的重複持倉'}),
                content_type='application/json'
            )

        merged_count  = 0
        merged_detail = []

        for row in dup_rows:
            tk        = row['ticker']
            positions = await hub.fetch("""
                SELECT id, ticker, current_shares, avg_cost, initial_shares,
                       realized_pnl, entry_date
                FROM positions
                WHERE is_open = TRUE AND ticker = $1
                ORDER BY entry_date ASC
            """, tk)

            if len(positions) < 2:
                continue

            main_pos  = positions[0]
            sub_poses = positions[1:]

            total_shares       = sum(int(p['current_shares']) for p in positions)
            total_cost         = sum(int(p['current_shares']) * float(p['avg_cost'] or 0) for p in positions)
            new_avg_cost       = round(total_cost / total_shares, 4) if total_shares > 0 else float(main_pos['avg_cost'] or 0)
            total_init_shares  = sum(int(p['initial_shares']) for p in positions)
            total_realized_pnl = sum(float(p['realized_pnl'] or 0) for p in positions)

            await hub.execute("""
                UPDATE positions SET
                    current_shares = $1,
                    initial_shares = $2,
                    avg_cost       = $3,
                    realized_pnl   = $4
                WHERE id = $5
            """, total_shares, total_init_shares, new_avg_cost,
                total_realized_pnl, main_pos['id'])

            for sub in sub_poses:
                await hub.execute("""
                    UPDATE positions SET
                        is_open     = FALSE,
                        exit_date   = $1,
                        exit_reason = 'MERGED',
                        state       = 'CLOSED'
                    WHERE id = $2
                """, date.today(), sub['id'])

            merged_count += 1
            merged_detail.append({
                'ticker':         tk.strip(),
                'merged_into_id': main_pos['id'],
                'total_shares':   total_shares,
                'new_avg_cost':   new_avg_cost,
                'sub_closed':     len(sub_poses),
            })

        return web.Response(
            text=json.dumps({
                'ok':     True,
                'merged': merged_count,
                'detail': merged_detail,
            }, default=serialize, ensure_ascii=False),
            content_type='application/json'
        )

    except Exception as e:
        logger.exception('handle_api_merge_positions 失敗')
        return web.Response(
            text=json.dumps({'ok': False, 'error': str(e)}, ensure_ascii=False),
            content_type='application/json', status=500,
        )


async def handle_api_add_position(request):
    hub  = request.app['hub']
    body = await request.json()
    try:
        ticker = body['ticker'].ljust(6)[:6]
        entry  = float(body['entry_price'])
        shares = int(body['shares'])
        atr    = float(body.get('atr', 15.0))
        mult   = float(body.get('atr_multiplier', 3.0))
        stop   = entry - atr * mult
        r      = atr * mult
        await hub.execute("""
            INSERT INTO positions (
                ticker, state, entry_date, entry_price,
                initial_shares, current_shares, signal_source,
                atr_at_entry, atr_multiplier, initial_stop_price,
                r_amount, current_stop_price, trail_pct, avg_cost,
                s1_atr_mult_applied
            ) VALUES ($1,'S1_INITIAL_DEFENSE',$2,$3,$4,$4,'MANUAL',$5,$6,$7,$8,$7,0.15,$3,$6)
        """, ticker, date.today(), entry, shares, atr, mult, stop, r)
        return web.Response(text='{"ok":true}', content_type='application/json')
    except Exception as e:
        return web.Response(
            text=json.dumps({'ok': False, 'error': str(e)}, ensure_ascii=False),
            content_type='application/json', status=500,
        )


async def handle_api_add_event(request):
    hub  = request.app['hub']
    body = await request.json()
    await hub.execute("""
        INSERT INTO calendar_events (event_date, event_type, name, impact_level)
        VALUES ($1,$2,$3,$4)
    """, date.fromisoformat(body['event_date']), body['event_type'],
         body['name'], int(body['impact_level']))
    return web.Response(text='{"ok":true}', content_type='application/json')


async def handle_api_run_scheduler(request):
    """
    [BUG 8 修正] 完整執行 Scheduler.run_daily()（Steps 0–7）
    [BUG 12 修正] 使用 BASE_DIR 絕對路徑
    """
    try:
        config_path = os.path.join(BASE_DIR, 'config.yaml')
        from modules.scheduler import Scheduler
        sched = Scheduler(config_path)
        await sched.connect()
        try:
            result = await sched.run_daily()
        finally:
            await sched.close()

        return web.Response(
            text=json.dumps({
                'ok':    True,
                'regime': result.get('steps', {}).get('macro', {}).get('regime', ''),
                'mrs':    result.get('steps', {}).get('macro', {}).get('mrs', 0),
                'steps':  result.get('steps', {}),
            }, default=serialize, ensure_ascii=False),
            content_type='application/json'
        )
    except Exception as e:
        logger.exception('handle_api_run_scheduler 失敗')
        return web.Response(
            text=json.dumps({'ok': False, 'error': str(e)}),
            content_type='application/json'
        )


async def handle_api_ev_simulate(request):
    hub  = request.app['hub']
    body = await request.json()
    from modules.ev_simulator import EVSimulator
    ev  = EVSimulator(hub)
    res = await ev.run(body.get('ticker', 'TEST'), float(body.get('score', 60)))
    return web.Response(
        text=json.dumps(res, default=serialize, ensure_ascii=False),
        content_type='application/json'
    )


async def handle_api_black_swan(request):
    hub = request.app['hub']
    try:
        config_path = os.path.join(BASE_DIR, 'config.yaml')
        with open(config_path, encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
        from datahub.api_telegram import TelegramBot
        from modules.black_swan import BlackSwan
        telegram = TelegramBot(cfg['telegram']['bot_token'], cfg['telegram']['chat_id'])
        bs  = BlackSwan(hub, telegram)
        res = await bs.run()
        await telegram.close()
        return web.Response(
            text=json.dumps(res, default=serialize, ensure_ascii=False),
            content_type='application/json'
        )
    except Exception as e:
        return web.Response(
            text=json.dumps({'ok': False, 'error': str(e)}),
            content_type='application/json'
        )


async def handle_api_walk_forward(request):
    hub = request.app['hub']
    try:
        from modules.walk_forward import WalkForward
        wf  = WalkForward(hub)
        res = await wf.run()
        return web.Response(
            text=json.dumps(res, default=serialize, ensure_ascii=False),
            content_type='application/json'
        )
    except Exception as e:
        return web.Response(
            text=json.dumps({'ok': False, 'error': str(e)}),
            content_type='application/json'
        )


async def handle_api_attribution(request):
    hub = request.app['hub']
    try:
        from modules.attribution import Attribution
        attr   = Attribution(hub)
        days   = int(request.rel_url.query.get('days', 30))
        result = await attr.analyze_closed_positions(days=days)
        return web.Response(
            text=json.dumps(result, default=serialize, ensure_ascii=False),
            content_type='application/json'
        )
    except Exception as e:
        return web.Response(
            text=json.dumps({'ok': False, 'error': str(e)}),
            content_type='application/json'
        )


async def handle_api_reset_regime_threshold(request):
    hub  = request.app['hub']
    body = await request.json()
    regime = body.get('regime', '')
    if regime:
        await hub.cache.delete(f"regime:score_threshold_override:{regime}")
    else:
        for r in ['BULL_TREND', 'WEAK_BULL', 'CHOPPY', 'BEAR_TREND']:
            await hub.cache.delete(f"regime:score_threshold_override:{r}")
    return web.Response(text='{"ok":true}', content_type='application/json')


async def handle_api_approve_order(request):
    """
    🟢 修復：人工確認大單審核端點。
    修復前：gate3_human_approval() 等待 Redis status=APPROVED，
            但無任何端點能寫入，所有大單（>10萬）永遠逾時被拒絕。
    修復後：操盤手透過 Dashboard 呼叫此端點，寫入 APPROVED/REJECTED。

    POST /api/approve-order/{order_id}
    Body: {"action": "approve"} 或 {"action": "reject"}
    """
    hub      = request.app['hub']
    order_id = request.match_info['order_id']

    try:
        body   = await request.json()
        action = body.get('action', 'approve').lower()
    except Exception:
        action = 'approve'

    cache_key = rk.key_gate_pending(order_id)

    try:
        cached = await hub.cache.get(cache_key)
        if cached is None:
            return web.Response(
                text=json.dumps({'ok': False, 'error': f'order_id={order_id} 不存在或已逾時'}),
                content_type='application/json',
                status=404,
            )

        if isinstance(cached, dict):
            cached['status'] = 'APPROVED' if action == 'approve' else 'REJECTED'
        else:
            cached = {'status': 'APPROVED' if action == 'approve' else 'REJECTED'}

        # 延長 TTL 至 5 分鐘，讓 ExecutionGate 有足夠時間讀取
        await hub.cache.set(cache_key, cached, ttl=300)

        logger.info("人工審核：order_id=%s → %s", order_id, cached['status'])
        return web.Response(
            text=json.dumps({'ok': True, 'order_id': order_id, 'status': cached['status']}),
            content_type='application/json',
        )
    except Exception as e:
        logger.exception("handle_api_approve_order 失敗")
        return web.Response(
            text=json.dumps({'ok': False, 'error': str(e)}),
            content_type='application/json',
            status=500,
        )


# =============================================================================
# App 初始化
# =============================================================================

async def _load_watchlist_from_db(hub: DataHub) -> list:
    """啟動時從 DB 載入 Watchlist，失敗時使用靜態備用清單。"""
    try:
        rows = await hub.fetch("""
            SELECT ticker FROM watchlist
            WHERE is_active = TRUE
            ORDER BY added_at ASC
        """)
        if rows:
            return [str(r["ticker"]).strip() for r in rows]
    except Exception:
        pass
    return list(WATCHLIST_FALLBACK)


async def create_app():
    config_path = os.path.join(BASE_DIR, 'config.yaml')
    hub = DataHub(config_path)
    await hub.connect()

    # 🟢 修復：Watchlist 從 DB 動態載入
    watchlist = await _load_watchlist_from_db(hub)

    app = web.Application()
    app['hub']       = hub
    app['watchlist'] = watchlist
    app['settings']  = {
        'mode': 'PAPER',
        'atr_mult_bull': 3.0, 'atr_mult_choppy': 2.0, 'atr_mult_bear': 1.5,
        'trail_pct': 15, 'score_threshold': 60, 'ev_threshold': 0.5,
    }

    # ── 路由註冊 ──────────────────────────────────────────────────────────────
    app.router.add_get('/',                                   handle_index)
    app.router.add_get('/api/data',                           handle_api_data)
    app.router.add_get('/api/settings',                       handle_api_settings)
    app.router.add_get('/api/watchlist',                      handle_api_watchlist)
    app.router.add_get('/api/attribution',                    handle_api_attribution)
    app.router.add_post('/api/settings/save',                 handle_api_settings_save)
    app.router.add_post('/api/close-position',                handle_api_close_position)
    app.router.add_post('/api/reduce-position',               handle_api_reduce_position)
    app.router.add_post('/api/merge-positions',               handle_api_merge_positions)
    app.router.add_post('/api/add-position',                  handle_api_add_position)
    app.router.add_post('/api/add-event',                     handle_api_add_event)
    app.router.add_post('/api/watchlist/add',                 handle_api_watchlist_add)
    app.router.add_post('/api/watchlist/remove',              handle_api_watchlist_remove)
    app.router.add_post('/api/run-scheduler',                 handle_api_run_scheduler)
    app.router.add_post('/api/ev-simulate',                   handle_api_ev_simulate)
    app.router.add_post('/api/black-swan',                    handle_api_black_swan)
    app.router.add_post('/api/walk-forward',                  handle_api_walk_forward)
    app.router.add_post('/api/reset-regime-threshold',        handle_api_reset_regime_threshold)
    app.router.add_post('/api/approve-order/{order_id}',      handle_api_approve_order)  # 🟢 新增

    return app


if __name__ == '__main__':
    async def main():
        app = await create_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', 8080)
        await site.start()
        print("Dashboard v3.3.1 啟動：http://localhost:8080")
        print("按 Ctrl+C 停止")
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            pass
        finally:
            await runner.cleanup()

    asyncio.run(main())