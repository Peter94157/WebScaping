import ccxt
import mysql.connector
import pandas as pd

# ===== Conexão MySQL =====
conn = mysql.connector.connect(
    host="localhost",      # Docker expõe na sua máquina
    port=3000,
    user="henrique",
    password="123",
    database="meuBanco"
)
cursor = conn.cursor()

# Cria tabela se não existir
cursor.execute("""
CREATE TABLE IF NOT EXISTS ohlcv (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp BIGINT,
    open DECIMAL(18,8),
    high DECIMAL(18,8),
    low DECIMAL(18,8),
    close DECIMAL(18,8),
    volume DECIMAL(18,8),
    symbol VARCHAR(20),
    timeframe VARCHAR(10)
)
""")

# ===== Conexão CCXT (Bybit como exemplo) =====
exchange = ccxt.bybit()
symbol = "BTC/USDT"
timeframe = "1h"

# Coleta 100 candles
ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=100)

# Transforma em DataFrame para facilitar
df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])

# Insere linha por linha no banco
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO ohlcv (timestamp, open, high, low, close, volume, symbol, timeframe)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (int(row["timestamp"]), row["open"], row["high"], row["low"], row["close"], row["volume"], symbol, timeframe))

conn.commit()
cursor.close()
conn.close()

print(f"{len(df)} candles inseridos no banco com sucesso 🚀")
