import psycopg2

try:
    conn = psycopg2.connect(
        host='127.0.0.1',
        port=5433,
        user='omni',
        password='omni2024secure',
        dbname='omni_investment_hub'
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
    count = cur.fetchone()[0]
    print(f'連線成功！資料表數量：{count}')
    conn.close()
except Exception as e:
    print(f'連線失敗：{e}')