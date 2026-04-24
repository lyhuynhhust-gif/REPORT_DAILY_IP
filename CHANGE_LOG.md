# IPSS Daily Report — Change Log

---

## [2026-04-24] Session 1 — Code Cleanup & Formula Freeze

### Yêu cầu
1. Bỏ chế độ load "1 file tổng hợp" trong `data_loader.py`, chỉ giữ mode 4 file riêng lẻ.
2. Logic bảo toàn công thức: chỉ giữ **10 dòng cuối** mỗi sheet có hàm, các dòng cũ hơn chuyển thành giá trị tĩnh (tránh Excel chậm).

### Actions
- **`modules/data_loader.py`**: Xóa `SHEET_MAP` và hàm `load_single_file()`. Cập nhật docstring.
- **`app.py`**: Xóa import `load_single_file`, xóa UI input "File tổng hợp", `process_and_export()` chỉ còn 1 nhánh dùng `raw_folders`. Thêm `os.makedirs(folder, exist_ok=True)`.
- **`config.json`**: Sửa `output_folder` và `template_file` từ đường dẫn cũ (`D:\IPSS DAILY REPORT\...`) sang đúng (`D:\IPSS REPORT\DATA BASE\`).
- **`modules/excel_updater.py`**:
  - `_copy_row_format`: tăng `max_col` từ 60 → 100 (SUMMARY có 77 cột).
  - Thêm hàm `_freeze_summary_rows(ws_sum, ws_val_sum, keep_rows=10, header_row=2)` — hybrid approach:
    - Col 5 (MONTH label): tính trực tiếp từ Year + Month (plain values).
    - Col 10 (Total WIP): SUM các WIP step cols (plain values).
    - Cols 43–76 (PR codes): dùng `wb_val` vì là SUMIFS tham chiếu sheet khác.
    - Col 13 (PR RW qty): dùng `wb_val`, fallback tổng PR cols.
    - Col 14 (DF Rate): tính từ PR RW / E3157.
  - Sửa call trong `update_report_v3`: truyền `wb_val["SUMMARY"]` vào `_freeze_summary_rows`.

### Bugs fixed
| Bug | Nguyên nhân | Fix |
|-----|-------------|-----|
| File không export ra DATA BASE | `output_folder` trong config trỏ sai path cũ | Sửa config.json |
| Frozen rows hiển thị PR RW = 0 | PR code cols (43–76) là SUMIFS, `_si(formula)` = 0 | Hybrid approach dùng wb_val |
| Cols 61–77 thiếu format khi copy row | `max_col=60` cắt mất 17 cột cuối | Tăng lên 100 |

---

## [2026-04-24] Session 2 — Đổi Step DI Rinse: E3100 → E1500

### Yêu cầu
- Từ 2026-04-17 trở đi, step **DI Rinse** đổi mã từ `E3100` → `E1500` và được chuyển lên trước `E2000` trong flow.
- Data cũ (E3100) giữ nguyên, không xóa.
- Logic đếm E1500 = **E1500 + E3100** (tổng cả step mới và step cũ).
- Cập nhật luôn các file từ ngày 17.

### Thay đổi STEPS list
```
Cũ: [E1100, E2000, E2010, E3100, E3150, E3153, E3157, E3160, E3170, E3250, E3300, E3400, E3430, E3500]
Mới: [E1100, E1500, E2000, E2010, E3150, E3153, E3157, E3160, E3170, E3250, E3300, E3400, E3430, E3500]
```

### Actions
- **`modules/calculator.py`**:
  - STEPS: thay `E3100` → `E1500`, đặt trước `E2000`.
  - STEP_NAMES: thêm `"E1500": "DI Rinse"`, giữ `"E3100": "DI Rinse (Legacy)"`.
  - Thêm `DI_RINSE_STEPS = ["E1500", "E3100"]`.
  - `calc_wip_by_step`: khi step == E1500, sum cả E1500 lẫn E3100 từ RAW.
  - `calc_movement_by_step`: tương tự.
- **`modules/excel_updater.py`**:
  - Import thêm `DI_RINSE_STEPS`.
  - `calc_daily_row_from_raw`: WIP loop và Move loop cho E1500 = tổng E1500 + E3100.
- **`config.json`**: Cập nhật `steps` và `step_names`.

### Migration Excel files
Cấu trúc cột SUMMARY thay đổi (WEEKLY/MONTHLY chỉ cập nhật header — formulas tự align):

| Vị trí | Cũ | Mới |
|--------|-----|-----|
| WIP col 16 | E2000 | **E1500** (data = old E3100) |
| WIP col 17 | E2010 | E2000 (data = old E2000) |
| WIP col 18 | E3100 | E2010 (data = old E2010) |
| Move col 30 | E2000 | **E1500** |
| Move col 31 | E2010 | E2000 |
| Move col 32 | E3100 | E2010 |

**Files migrated:**

| File | Status |
|------|--------|
| 20260417_IPSS_DAILY_REPORT_V03.xlsx | ✅ Migrated |
| 20260418_IPSS_DAILY_REPORT_V03.xlsx | ✅ Migrated |
| 20260419_IPSS_DAILY_REPORT_V03.xlsx | ❌ Corrupt (cần regenerate) |
| 20260420_IPSS_DAILY_REPORT_V03.xlsx | ❌ Corrupt (cần regenerate) |
| 20260421_IPSS_DAILY_REPORT_V03.xlsx | ✅ Migrated |
| 20260422_IPSS_DAILY_REPORT_V03.xlsx | ✅ Migrated |
| 20260423_IPSS_DAILY_REPORT_V03.xlsx | ✅ Migrated |
| 20260424_IPSS_DAILY_REPORT_V03.xlsx | ✅ Migrated |

---

## Template ghi log cho lần sau

```
## [YYYY-MM-DD] Session N — <Tên công việc>

### Yêu cầu
- ...

### Actions
- **file**: mô tả thay đổi

### Bugs fixed / Notes
| Bug | Nguyên nhân | Fix |
|-----|-------------|-----|
```
