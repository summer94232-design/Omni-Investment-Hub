import asyncio
import yaml

async def test_fred():
    from datahub.api_fred import FredAPI
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    api = FredAPI(cfg['fred']['api_key'])
    result = await api.get_latest('FEDFUNDS')
    print(f'FRED 連線成功！Fed Funds Rate = {result}%')
    await api.close()

async def test_telegram():
    from datahub.api_telegram import TelegramBot
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    bot = TelegramBot(cfg['telegram']['bot_token'], cfg['telegram']['chat_id'])
    result = await bot.ping()
    print(f'Telegram 連線{"成功" if result else "失敗"}！')
    await bot.close()

async def test_finmind():
    from datahub.api_finmind import FinMindAPI
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    api = FinMindAPI(cfg['finmind']['token'])
    df = await api.get_stock_price('2330', '2024-01-01', '2024-01-31')
    print(f'FinMind 連線成功！台積電筆數：{len(df)} 筆')
    await api.close()

async def main():
    await test_fred()
    await test_telegram()
    await test_finmind()

asyncio.run(main())