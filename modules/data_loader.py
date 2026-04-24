"""
Data loader — 4 file RAW riêng lẻ (hold_history, wip, move, input).
"""
import pandas as pd
from datetime import datetime, timedelta
import os

# Map Vietnamese display labels → internal column names
VI_TO_CODE = {
    "Ngày":           "Date",
    "Năm":            "Year",
    "Tháng":          "Month",
    "Tuần ISO":       "Week",
    "Week (custom)":  "WeekLabel",
    "Input IPSS":     "Input_IPSS",
    "Input PSS":      "Input_PSS",
    "EPI Shipment":   "EPI_Shipment",
    "Total WIP":      "Total_WIP",
    "Total Move":     "Total_Move",
    "Develop Move":   "Develop_Move",
    "PR RW (qty)":    "PR_RW_Qty",
    "PR RW Rate %":   "PR_RW_Rate",
    "Active Hold":    "Active_Hold",
}


def _read_sheet(filepath: str, sheet_name: str, with_header: bool = True) -> pd.DataFrame:
    try:
        kw = {} if with_header else {"header": None}
        df = pd.read_excel(filepath, sheet_name=sheet_name, engine="openpyxl", **kw)
        if with_header:
            df.columns = df.columns.astype(str).str.strip()
        return df
    except:
        return pd.DataFrame()


def find_raw_file(folder: str, target_date: datetime, pattern: str = "*") -> str:
    """
    Search for a file in 'folder' that matches the target_date (YYYY-MM-DD or YYYYMMDD)
    and complies with the wildcard pattern.
    """
    import glob
    if not folder or not os.path.exists(folder):
        return ""
    
    # Common date formats in filenames
    date_strs = [
        target_date.strftime("%Y-%m-%d"),
        target_date.strftime("%Y%m%d"),
        target_date.strftime("%Y.%m.%d"),
    ]
    
    # Search with pattern
    search_pattern = pattern if pattern else "*"
    if "*" not in search_pattern:
        search_pattern = f"*{search_pattern}*"
        
    full_pattern = os.path.join(folder, search_pattern)
    files = glob.glob(full_pattern)
    
    # If no files found with specific pattern, try a broader search
    if not files:
        files = glob.glob(os.path.join(folder, "*"))
    
    for f in sorted(files, reverse=True):
        base = os.path.basename(f).upper()
        # Look for date string in filename
        if any(ds in base.replace("-","").replace(".","") for ds in [d.replace("-","").replace(".","") for d in date_strs]):
             return f
             
    # Fallback: if only one file matches the date strings regardless of pattern
    all_files = glob.glob(os.path.join(folder, "*"))
    for f in all_files:
        base = os.path.basename(f)
        if any(ds in base for ds in date_strs):
            return f
            
    return ""


def load_separate_files(paths_or_folders: dict, today: datetime = None, patterns: dict = None) -> dict:
    """
    Load 4 separate RAW files. 
    If value is a file, load it. 
    If value is a folder, search for a file matching 'today' and optional 'pattern'.
    """
    if today is None:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    results = {}
    mapping = {
        "hold_history": "RAW_HOLD HISTORY",
        "wip":          "RAW_WIP",
        "move":         "RAW_MOVE",
        "input":        "RAW_INPUT",
    }
    
    for key, _ in mapping.items():
        path_val = paths_or_folders.get(key, "")
        final_path = ""
        
        if path_val and os.path.isfile(path_val):
            final_path = path_val
        elif path_val and os.path.isdir(path_val):
            pattern = patterns.get(key, "*") if patterns else "*"
            final_path = find_raw_file(path_val, today, pattern)
        
        if final_path and os.path.exists(final_path):
            results[key] = _read_sheet(final_path, 0, with_header=True)
            results[f"{key}_path"] = final_path # Keep track of which file was found
        else:
            results[key] = pd.DataFrame()
            
    return results


def load_trend_df(report_path: str) -> pd.DataFrame:
    """
    Load TREND sheet from report file.
    Returns DataFrame with internal column names (Date, Input_IPSS, WIP_E1100, etc.)
    Handles: section header row, Vietnamese labels, duplicate step columns.
    """
    if not report_path or not os.path.exists(report_path):
        return pd.DataFrame()
    try:
        from modules.calculator import STEPS

        # Read raw without any header to inspect structure
        df_raw = pd.read_excel(report_path, sheet_name="TREND", engine="openpyxl", header=None)
        if df_raw.empty:
            return pd.DataFrame()

        # Detect header row: find row containing "Date" or "Ngày"
        header_row = None
        for ri in range(min(5, len(df_raw))):
            row_vals = [str(v).strip() for v in df_raw.iloc[ri].tolist()]
            if "Date" in row_vals or "Ngày" in row_vals:
                header_row = ri
                break
        if header_row is None:
            return pd.DataFrame()

        # Read with detected header
        df = pd.read_excel(report_path, sheet_name="TREND", engine="openpyxl", header=header_row)
        df.columns = [str(c).strip() for c in df.columns]

        # Rename Vietnamese labels → code names
        df = df.rename(columns=VI_TO_CODE)

        # Fix step columns: assign WIP_ and Move_ in order
        # The TREND sheet has: [base_cols][WIP steps][Move steps][PR cols]
        # Step cols appear twice: first batch = WIP, second batch = Move
        step_positions = []  # (col_index, col_name)
        for ci, col in enumerate(df.columns):
            base = str(col).split(".")[0].strip()  # handles E1100.1 → E1100
            if base in STEPS:
                step_positions.append((ci, col, base))

        # Split into two groups of 14 (WIP and Move)
        n_steps = len(STEPS)
        wip_positions  = step_positions[:n_steps]
        move_positions = step_positions[n_steps:n_steps*2]

        rename_map = {}
        for ci, col, base in wip_positions:
            rename_map[col] = f"WIP_{base}"
        for ci, col, base in move_positions:
            target = f"Move_{base}"
            # avoid collision if already renamed
            if col in rename_map:
                rename_map[col + f"__move"] = target
            else:
                rename_map[col] = target

        # Apply renames using position-based approach to handle duplicates
        new_cols = list(df.columns)
        for ci, col, base in wip_positions:
            new_cols[ci] = f"WIP_{base}"
        for ci, col, base in move_positions:
            new_cols[ci] = f"Move_{base}"
        df.columns = new_cols

        # Fix PR columns (PR08 → PR_PR08, but avoid double-prefix)
        final_cols = []
        for col in df.columns:
            c = str(col)
            if (c.startswith("PR") and not c.startswith("PR_") and
                    c not in ("PR_RW_Qty","PR_RW_Rate") and
                    not c.startswith("WIP_") and not c.startswith("Move_")):
                final_cols.append(f"PR_{c}")
            else:
                final_cols.append(c)
        df.columns = final_cols

        # Drop any remaining duplicate columns (keep first occurrence)
        df = df.loc[:, ~df.columns.duplicated(keep="first")]

        # Validate Date column
        if "Date" not in df.columns:
            return pd.DataFrame()
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["Date"])

        if len(df) < 2:
            return pd.DataFrame()

        # Must have real production data
        for col in ["Total_WIP", "Total_Move", "Input_IPSS", "PR_RW_Qty"]:
            if col in df.columns:
                if pd.to_numeric(df[col], errors="coerce").fillna(0).sum() > 0:
                    return df
        return pd.DataFrame()

    except Exception:
        return pd.DataFrame()


def build_trend_from_historical(raw: dict, week_start_day: int = 4) -> pd.DataFrame:
    """
    Build TREND DataFrame from pre-calculated sheets
    (DAILY MOVEMENT, DAILY WIP, DAILY INPUT).
    """
    from modules.calculator import STEPS, get_week_label, upsert_trend
    import numpy as np

    dm = raw.get("daily_movement", pd.DataFrame())
    dw = raw.get("daily_wip",      pd.DataFrame())
    di = raw.get("daily_input",    pd.DataFrame())

    if dm.empty and dw.empty:
        return pd.DataFrame()

    def _parse_sheet(df_raw, data_start_row=6):
        if df_raw.empty or len(df_raw) < data_start_row:
            return [], pd.DataFrame()
        step_keys = [str(s).strip() for s in df_raw.iloc[4, 2:].tolist()]
        data = df_raw.iloc[data_start_row:].copy().reset_index(drop=True)
        n = len(step_keys)
        data.columns = (["_", "date"] + step_keys[:n] +
                        [f"_x{i}" for i in range(max(0, len(data.columns)-2-n))])
        data["date"] = pd.to_datetime(data["date"], errors="coerce")
        data = data[data["date"].notna() & (data["date"] >= pd.Timestamp("2025-01-01"))]
        return step_keys, data

    move_steps, move_df = _parse_sheet(dm)
    wip_steps,  wip_df  = _parse_sheet(dw)

    all_dates = set()
    if not move_df.empty: all_dates |= set(move_df["date"].dt.normalize().unique())
    if not wip_df.empty:  all_dates |= set(wip_df["date"].dt.normalize().unique())

    inp_df = pd.DataFrame()
    if not di.empty and len(di) > 7:
        inp_df = di.iloc[7:].copy()
        inp_df["date"] = pd.to_datetime(inp_df.iloc[:, 1], errors="coerce")
        inp_df = inp_df[inp_df["date"].notna() & (inp_df["date"] >= pd.Timestamp("2025-01-01"))]

    df_trend = pd.DataFrame()
    for dt_ts in sorted(all_dates):
        dt = dt_ts.to_pydatetime() if hasattr(dt_ts, "to_pydatetime") else dt_ts
        ts = pd.Timestamp(dt_ts)
        row = {
            "Date":      ts.strftime("%Y-%m-%d"),
            "Year":      ts.year, "Month": ts.month,
            "Week":      ts.isocalendar()[1],
            "WeekLabel": get_week_label(dt, week_start_day),
        }
        m = move_df[move_df["date"].dt.normalize() == ts] if not move_df.empty else pd.DataFrame()
        total_move = 0
        for step in STEPS:
            v = 0
            if len(m) > 0 and step in m.columns:
                try: v = 0 if pd.isna(m.iloc[0][step]) else int(str(m.iloc[0][step]).replace(",",""))
                except: v = 0
            row[f"Move_{step}"] = v; total_move += v
        row["Total_Move"]   = total_move
        row["Develop_Move"] = sum(row.get(f"Move_{s}",0) for s in ["E3150","E3153","E3157","E3160","E3170"])

        w = wip_df[wip_df["date"].dt.normalize() == ts] if not wip_df.empty else pd.DataFrame()
        total_wip = 0
        for step in STEPS:
            v = 0
            if len(w) > 0 and step in w.columns:
                try: v = 0 if pd.isna(w.iloc[0][step]) else int(str(w.iloc[0][step]).replace(",",""))
                except: v = 0
            row[f"WIP_{step}"] = v; total_wip += v
        row["Total_WIP"] = total_wip

        row["Input_PSS"]=0; row["Input_IPSS"]=0; row["EPI_Shipment"]=0
        if not inp_df.empty:
            i = inp_df[inp_df["date"].dt.normalize() == ts]
            if len(i) > 0:
                for ci, k in [(2,"Input_PSS"),(3,"Input_IPSS"),(4,"EPI_Shipment")]:
                    try:
                        v = i.iloc[0, ci]
                        row[k] = 0 if pd.isna(v) else int(float(str(v).replace(",","")))
                    except: pass

        row["PR_RW_Qty"]=0; row["PR_RW_Rate"]=0.0; row["Active_Hold"]=0
        df_trend = upsert_trend(df_trend, row)

    return df_trend
