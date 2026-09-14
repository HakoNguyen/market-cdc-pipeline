import yfinance as yf

for sym in ["FPT.VN", "VNM.VN", "HPG.VN", "VCB.VN"]:
    t = yf.Ticker(sym)
    h = t.history(period="max", auto_adjust=False)
    print(sym, h.index.min(), h.index.max(), len(h))
    print(t.actions)

    adj = t.history(period="2y", auto_adjust=True)
    raw = t.history(period="2y", auto_adjust=False)
    print("delta:", (adj["Close"] - raw["Close"]).abs().sum())