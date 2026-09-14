# Phase 0 Verification Notes — Data Source (yfinance)

**Date verified:** 2026-09-11  
**Target market:** Vietnam (HOSE / VN30)  
**Sample symbols tested:** `FPT.VN`, `VNM.VN`, `HPG.VN`, `VCB.VN`

---

## 1. Kết quả kiểm tra dữ liệu yfinance

| Mã CP | Ngày bắt đầu | Ngày mới nhất | Tổng số phiên | Số đợt corporate action |
|---|---|---|---|---|
| **FPT.VN** | 2010-02-22 | 2026-09-11 | 4,297 | 29 |
| **VNM.VN** | 2012-04-03 | 2026-09-11 | 3,737 | 37 |
| **HPG.VN** | 2010-02-22 | 2026-09-11 | 4,297 | 16 |
| **VCB.VN** | 2010-02-22 | 2026-09-11 | 4,288 | 15 |

---

## 2. Các phát hiện quan trọng (Key Insights)

### A. Độ sâu lịch sử & độ tươi (Freshness)
- Dữ liệu dài 14–16 năm, đầy đủ cho backfill và tính toán rolling metrics.
- Dữ liệu cập nhật real-time/hàng ngày đến ngày hiện tại (`2026-09-11`), không bị stale.

### B. Retroactive Price Adjustment thật
- Tổng chênh lệch tuyệt đối `sum(|Close_adj - Close_raw|)` (cột `delta`) rất lớn:
  - FPT: ~1.22 triệu VND
  - VNM: ~2.08 triệu VND
  - HPG: ~174k VND
  - VCB: ~423k VND
- Chứng tỏ Yahoo Finance **có thực hiện retroactive price adjustment** khi phát sinh sự kiện doanh nghiệp.

### C. Đặc thù 1: Stock Split là hệ số cổ tức bằng cổ phiếu (Decimal Ratios)
- Trên thị trường Mỹ, split thường là số nguyên 2:1 (`2.0`), 3:1 (`3.0`).
- Trên thị trường Việt Nam, `Stock Splits` trong yfinance mang các giá trị như `1.15`, `1.20`, `1.276`, `1.495`...
- **Bản chất:** Đây là cổ tức bằng cổ phiếu (thưởng 15%, 20%, 27.6%, 49.5%...).
- **Quy tắc xử lý:** Factor điều chỉnh phải là **tích dồn (cumulative product)** của các hệ số thập phân này, không được giả định split là số nguyên.

### D. Đặc thù 2: Cột Dividends tự nó cũng bị Retroactive Adjustment!
- Xem xét lịch sử `Dividends` của FPT.VN:
  - Năm 2012: ~137.8 VND
  - Năm 2025: 1,000.0 VND
- Trên thực tế, FPT trả cổ tức tiền mặt khoảng 1,000 – 2,000 VND/cp trong suốt lịch sử. Con số 137.8 VND vào 2012 là giá trị lịch sử **đã bị điều chỉnh ngược (adjusted backwards)** bởi các đợt chia thưởng cổ phiếu sau đó.
- **Ý nghĩa dự án:** 
  - *Ngay cả cột dividend cũng không immutable*.
  - Bài toán chuyển từ "giá bị điều chỉnh" thành **"nhiều cột bị điều chỉnh ngược đồng thời"**.
  - Củng cố lý do bắt buộc dùng CDC (Debezium + StarRocks PK) thay vì chỉ so sánh `max(trade_date)`.

---

## 3. Danh sách mã theo dõi đề xuất (VN30 Universe)

Thay vì chỉ dùng 4 mã ban đầu, mở rộng danh sách khoảng **20–30 mã VN30** để kiểm thử tính đa dạng (ngành nghề, ngân hàng vs sản xuất, cổ tức tiền vs cổ tức cổ phiếu, mã delist/thay đổi rổ index):

`FPT.VN`, `VNM.VN`, `HPG.VN`, `VCB.VN`, `SSI.VN`, `TCB.VN`, `MWG.VN`, `MBB.VN`, `VIC.VN`, `VHM.VN`, `STB.VN`, `VPB.VN`, `ACB.VN`, `MSN.VN`, `HDB.VN`, `BID.VN`, `CTG.VN`, `GAS.VN`, `PLX.VN`, `VRE.VN`, `SAB.VN`, `POW.VN`, `BCM.VN`, `GVR.VN`, `VIB.VN`, `SHB.VN`, `TPB.VN`, `SSB.VN`
