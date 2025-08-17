import tkinter as tk
from tkinter import ttk
from crypto_ohlcv_pipeline import pipeline

def run_pipeline():
    symbol = combo_symbol.get()
    timeframe = combo_timeframe.get()
    start = combo_start.get()
    end = combo_end.get()
    to_csv = var_csv.get()
    to_xlsx = var_xlsx.get()

    pipeline(symbol, timeframe, start, end, to_csv=to_csv, to_xlsx=to_xlsx)

# Janela principal
root = tk.Tk()
root.title("Crypto OHLCV Collector")

# --- Símbolo ---
ttk.Label(root, text="Símbolo:").grid(row=0, column=0, padx=5, pady=5)
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
combo_symbol = ttk.Combobox(root, values=symbols)
combo_symbol.set(symbols[0])  # valor padrão
combo_symbol.grid(row=0, column=1, padx=5, pady=5)

# --- Timeframe ---
ttk.Label(root, text="Timeframe:").grid(row=1, column=0, padx=5, pady=5)
timeframes = ["1m", "5m", "15m", "1h", "4h", "1d"]
combo_timeframe = ttk.Combobox(root, values=timeframes)
combo_timeframe.set(timeframes[1])
combo_timeframe.grid(row=1, column=1, padx=5, pady=5)

# --- Data inicial ---
ttk.Label(root, text="Data inicial:").grid(row=2, column=0, padx=5, pady=5)
dates_start = ["2024-01-01", "2024-06-01", "2024-12-01"]
combo_start = ttk.Combobox(root, values=dates_start)
combo_start.set(dates_start[0])
combo_start.grid(row=2, column=1, padx=5, pady=5)

# --- Data final ---
ttk.Label(root, text="Data final:").grid(row=3, column=0, padx=5, pady=5)
dates_end = ["2024-06-30", "2024-12-31", "2025-01-01"]
combo_end = ttk.Combobox(root, values=dates_end)
combo_end.set(dates_end[1])
combo_end.grid(row=3, column=1, padx=5, pady=5)

# --- Export options ---
var_csv = tk.BooleanVar(value=True)
var_xlsx = tk.BooleanVar(value=False)
ttk.Checkbutton(root, text="Exportar CSV", variable=var_csv).grid(row=4, column=0, padx=5, pady=5)
ttk.Checkbutton(root, text="Exportar XLSX", variable=var_xlsx).grid(row=4, column=1, padx=5, pady=5)

# --- Botão ---
btn_run = ttk.Button(root, text="Executar", command=run_pipeline)
btn_run.grid(row=5, column=0, columnspan=2, pady=10)

root.mainloop()
