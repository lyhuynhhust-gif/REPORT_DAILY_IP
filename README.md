# 📊 IPSS Daily Production Report

Hệ thống báo cáo sản xuất tự động cho dây chuyền IPSS — theo dõi WIP, Movement, Input, PR Rework và DF Rate.

## 🚀 Cài đặt & Chạy

### Yêu cầu
- Python 3.9+
- pip install -r requirements.txt

### Chạy app
```bash
streamlit run app.py
```
hoặc double-click `run.bat` trên Windows.

## 📁 Cấu trúc thư mục

```
IPSS REPORT/
├── app.py                  # Main Streamlit dashboard
├── config.json             # Cấu hình đường dẫn RAW data & email
├── requirements.txt        # Python dependencies
├── run.bat                 # Windows launcher
├── modules/
│   ├── calculator.py       # Logic tính toán WIP, Move, PR RW
│   ├── data_loader.py      # Load file RAW (HOLD, WIP, MOVE, INPUT)
│   ├── excel_updater.py    # Tạo/cập nhật file Excel báo cáo
│   ├── email_sender.py     # Gửi email Outlook tự động
│   └── scheduler.py        # Auto-schedule hàng ngày
├── DATA BASE/
│   └── 20260424_IPSS_DAILY_REPORT_V03.xlsx   # Export ngày 24/4
└── 20260415_IPSS_DAILY_REPORT_V03.xlsx        # Template gốc V03
```

## ⚙️ Cấu hình (config.json)

Mở **Settings** trong app → nhập đường dẫn thư mục RAW data cho từng loại file:
- HOLD HISTORY
- WIP Snapshot
- MOVEMENT
- INPUT

## 📊 Dashboard Tabs

| Tab | Nội dung |
|-----|----------|
| 🏠 Dashboard | KPI tổng quan, Alert, 4 biểu đồ chính |
| ⚠️ Quality | DF Rate Control Chart, Pareto PR Code |
| 📈 Production | Input/Shipment, Movement by Step |
| 📦 WIP | WIP Snapshot, Heatmap, Trend |
| 🔍 Lot Detail | Danh sách lot PR RW chi tiết |
| ⚙️ Settings | Cấu hình đường dẫn, tuần, limit |
| 📧 Email | Gửi báo cáo qua Outlook |

## 🔄 Logic cập nhật hàng ngày

1. Load 4 file RAW (HOLD, WIP, MOVE, INPUT) theo ngày
2. Tính toán KPI: WIP, Movement, Input, PR RW, DF Rate
3. Upsert 1 row/ngày vào sheet SUMMARY
4. Rebuild WEEKLY + MONTHLY từ SUMMARY
5. Cập nhật PR RW LOT LIST (dedup by Lot + Date + Code)
6. Lưu file `YYYYMMDD_IPSS_DAILY_REPORT_V03.xlsx`

## 📝 Changelog

Xem [CHANGE_LOG.md](CHANGE_LOG.md)
