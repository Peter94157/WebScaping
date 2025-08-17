import tkinter as tk
from tkinter import ttk, messagebox
from crypto_ohlcv_pipeline import pipeline
import threading

# --- Função do pipeline em thread ---
def run_pipeline_thread(symbol, timeframe, start, end, to_csv, to_xlsx):
    pipeline(symbol, timeframe, start, end, to_csv=to_csv, to_xlsx=to_xlsx)
    # Volta para tela de seleção
    frame_loading.pack_forget()
    frame_selection.pack(fill="both", expand=True)
    messagebox.showinfo("Concluído", "Pipeline executado com sucesso!")

# --- Função para iniciar o pipeline ---
def run_pipeline_gui():
    symbol = combo_symbol.get()
    timeframe = combo_timeframe.get()
    start = combo_start.get()
    end = combo_end.get()
    to_csv = var_csv.get()
    to_xlsx = var_xlsx.get()

    # Esconde a tela de seleção e mostra loading
    frame_selection.pack_forget()
    frame_loading.pack(fill="both", expand=True)

    threading.Thread(
        target=run_pipeline_thread,
        args=(symbol, timeframe, start, end, to_csv, to_xlsx)
    ).start()

# --- Janela principal ---
root = tk.Tk()
root.title("Crypto OHLCV Collector")
root.geometry("350x250")

# --- Frame de seleção ---
frame_selection = tk.Frame(root)

ttk.Label(frame_selection, text="Símbolo:").grid(row=0, column=0, padx=5, pady=5)
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
combo_symbol = ttk.Combobox(frame_selection, values=symbols)
combo_symbol.set(symbols[0])
combo_symbol.grid(row=0, column=1, padx=5, pady=5)

ttk.Label(frame_selection, text="Timeframe:").grid(row=1, column=0, padx=5, pady=5)
timeframes = ["1m", "5m", "15m", "1h", "4h", "1d"]
combo_timeframe = ttk.Combobox(frame_selection, values=timeframes)
combo_timeframe.set(timeframes[1])
combo_timeframe.grid(row=1, column=1, padx=5, pady=5)

ttk.Label(frame_selection, text="Data inicial:").grid(row=2, column=0, padx=5, pady=5)
dates_start = ["2024-01-01", "2024-06-01", "2024-12-01"]
combo_start = ttk.Combobox(frame_selection, values=dates_start)
combo_start.set(dates_start[0])
combo_start.grid(row=2, column=1, padx=5, pady=5)

ttk.Label(frame_selection, text="Data final:").grid(row=3, column=0, padx=5, pady=5)
dates_end = ["2024-06-30", "2024-12-31", "2025-01-01"]
combo_end = ttk.Combobox(frame_selection, values=dates_end)
combo_end.set(dates_end[1])
combo_end.grid(row=3, column=1, padx=5, pady=5)

var_csv = tk.BooleanVar(value=True)
var_xlsx = tk.BooleanVar(value=False)
ttk.Checkbutton(frame_selection, text="Exportar CSV", variable=var_csv).grid(row=4, column=0, padx=5, pady=5)
ttk.Checkbutton(frame_selection, text="Exportar XLSX", variable=var_xlsx).grid(row=4, column=1, padx=5, pady=5)

btn_run = ttk.Button(frame_selection, text="Executar", command=run_pipeline_gui)
btn_run.grid(row=5, column=0, columnspan=2, pady=10)

frame_selection.pack(fill="both", expand=True)

# --- Frame de carregamento ---
frame_loading = tk.Frame(root)
tk.Label(frame_loading, text="⏳ Executando pipeline, aguarde...").pack(expand=True)

root.mainloop()
