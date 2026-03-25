# test_selection.py
import asyncio
import yaml
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI
from modules.selection_engine import SelectionEngine

async def main():
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    hub = DataHub('config.yaml')
    await hub.connect()
    finmind = FinMindAPI(cfg['finmind']['token'])

    engine = SelectionEngine(hub, finmind)

    tickers = ['2330', '2303', '2454', '2412', '2317']
    results = []
    for ticker in tickers:
        result = await engine.run(ticker)
        results.append(result)
        print(f"\n=== {ticker} 選股分析 ===")
        print(f"總分：{result['total_score']}")
        print(f"動能：{result['score_momentum']} | 籌碼：{result['score_chip']} | 基本面：{result['score_fundamental']}")
        print(f"宏觀環境：{result['regime_at_calc']} MRS={result['mrs_at_calc']}")

    # 排行榜
    print("\n=== 選股排行榜 ===")
    ranked = sorted(results, key=lambda x: x['total_score'], reverse=True)
    for i, r in enumerate(ranked, 1):
        print(f"{i}. {r['ticker']}  總分：{r['total_score']}")

    await finmind.close()
    await hub.close()

asyncio.run(main())