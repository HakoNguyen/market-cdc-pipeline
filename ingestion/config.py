import os

# Rổ VN30 theo VNDirect /v4/stocks?q=indexCode:VN30 ngày 2026-11-02 (HOSE review rổ 6 tháng/lần)
VN30_TICKERS = [
    "ACB", "BID", "BSR", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", "LPB",
    "MBB", "MCH", "MSN", "MWG", "SAB", "SHB", "SSB", "SSI", "STB", "TCB",
    "TCX", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VPL", "VRE",
]

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5435")
DB_NAME = os.getenv("POSTGRES_DB", "market_db")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

DATA_RAW_DIR = "data/raw"
