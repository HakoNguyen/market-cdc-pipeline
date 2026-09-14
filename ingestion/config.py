import os

VN30_TICKERS = [
    "FPT.VN", "VNM.VN", "HPG.VN", "VCB.VN", "SSI.VN", "TCB.VN", 
    "MWG.VN", "MBB.VN", "VIC.VN", "VHM.VN", "STB.VN", "VPB.VN", 
    "ACB.VN", "MSN.VN", "HDB.VN", "BID.VN", "CTG.VN", "GAS.VN", 
    "PLX.VN", "VRE.VN", "SAB.VN", "POW.VN", "BCM.VN", "GVR.VN", 
    "VIB.VN", "SHB.VN", "TPB.VN", "SSB.VN"
]

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "market_db")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

DATA_RAW_DIR = "data/raw"
