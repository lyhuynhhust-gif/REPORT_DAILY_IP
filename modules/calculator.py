"""
Core calculation engine for IPSS Daily Report.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

STEPS = ["E1100","E1500","E2000","E2010","E3150","E3153","E3157",
         "E3160","E3170","E3250","E3300","E3400","E3430","E3500"]

# E1500 = DI Rinse (mới từ 2026-04-17, thay thế E3100)
# E3100 = DI Rinse (legacy, vẫn giữ để đếm tổng vào E1500)
STEP_NAMES = {
    "E1100":"Initial Cleanup","E1500":"DI Rinse",
    "E2000":"SiO2 Deposition","E2010":"SiO2 Thickness",
    "E3100":"DI Rinse (Legacy)",
    "E3150":"Photo Coating","E3153":"Photo Exposure",
    "E3157":"Photo Development","E3160":"Dev Inspection",
    "E3170":"AOI Inspection","E3250":"PSS Etch",
    "E3300":"PR Cleaning","E3400":"AOI Inspection 2",
    "E3430":"ALN Deposition","E3500":"PSS BANK",
}
# Steps cũ của DI Rinse (cả hai đều được tính vào E1500)
DI_RINSE_STEPS = ["E1500", "E3100"]
DEVELOP_STEPS = ["E3150","E3153","E3157","E3160","E3170"]


def _norm_date(series):
    return pd.to_datetime(series, errors="coerce")

def _si(v):
    if pd.isna(v) or v == "": return 0
    try:
        s = str(v).replace(",","").replace(" ","").replace("\xa0","").strip()
        if not s: return 0
        return int(float(s))
    except:
        return 0


def calc_wip_by_step(df_wip: pd.DataFrame) -> dict:
    if df_wip.empty or "Step" not in df_wip.columns:
        return {s: 0 for s in STEPS}
    df = df_wip.copy()
    df["Step"] = df["Step"].astype(str).str.strip()
    if "Product Group" in df.columns:
        df = df[df["Product Group"].astype(str).str.upper().str.contains("IPSS", na=False)]
    result = {}
    for step in STEPS:
        if step == "E1500":
            # E1500 = tổng DI Rinse mới (E1500) + DI Rinse cũ (E3100)
            qty = sum(
                (df.loc[df["Step"] == s, "Qty"].sum() if "Qty" in df.columns else 0)
                for s in DI_RINSE_STEPS
            )
        else:
            qty = df.loc[df["Step"] == step, "Qty"].sum() if "Qty" in df.columns else 0
        result[step] = int(qty) if not pd.isna(qty) else 0
    return result


def calc_movement_by_step(df_move: pd.DataFrame, date: datetime) -> dict:
    if df_move.empty or "Step" not in df_move.columns:
        return {s: 0 for s in STEPS}
    df = df_move.copy()
    if "Date" in df.columns:
        df["Date"] = _norm_date(df["Date"])
    date_only = pd.Timestamp(date).normalize()
    mask = df["Date"].dt.normalize() == date_only
    if "EventName" in df.columns:
        mask &= df["EventName"].astype(str).str.contains("TrackOut", na=False)
    if "Product Group" in df.columns:
        mask &= df["Product Group"].astype(str).str.upper().str.contains("IPSS", na=False)
    df_day = df[mask]
    result = {}
    for step in STEPS:
        if step == "E1500":
            # E1500 = tổng DI Rinse mới (E1500) + DI Rinse cũ (E3100)
            qty = sum(
                (df_day.loc[df_day["Step"].astype(str).str.strip() == s, "Qty"].sum()
                 if "Qty" in df_day.columns else 0)
                for s in DI_RINSE_STEPS
            )
        else:
            s_mask = df_day["Step"].astype(str).str.strip() == step
            qty = df_day.loc[s_mask, "Qty"].sum() if "Qty" in df_day.columns else 0
        result[step] = int(qty) if not pd.isna(qty) else 0
    return result


def calc_develop_move(df_move: pd.DataFrame, date: datetime) -> int:
    if df_move.empty:
        return 0
    df = df_move.copy()
    if "Date" in df.columns:
        df["Date"] = _norm_date(df["Date"])
    date_only = pd.Timestamp(date).normalize()
    mask = df["Date"].dt.normalize() == date_only
    if "EventName" in df.columns:
        mask &= df["EventName"].astype(str).str.contains("TrackOut", na=False)
    if "Step" in df.columns:
        mask &= df["Step"].astype(str).isin(DEVELOP_STEPS)
    if "Product Group" in df.columns:
        mask &= df["Product Group"].astype(str).str.upper().str.contains("IPSS", na=False)
    qty = df.loc[mask, "Qty"].sum() if "Qty" in df.columns else 0
    return int(qty) if not pd.isna(qty) else 0


def calc_input(df_input: pd.DataFrame, date: datetime) -> dict:
    if df_input.empty:
        return {"IPSS": 0, "PSS": 0}
    df = df_input.copy()
    if "Date" in df.columns:
        df["Date"] = _norm_date(df["Date"])
    date_only = pd.Timestamp(date).normalize()
    mask = df["Date"].dt.normalize() == date_only
    if "Step" in df.columns:
        mask &= df["Step"].astype(str).str.strip() == "E1100"
    df_day = df[mask]
    if "Qty" in df_day.columns:
        df_day = df_day.copy()
        df_day["Qty"] = pd.to_numeric(df_day["Qty"], errors="coerce").fillna(0)
    else:
        return {"IPSS": 0, "PSS": 0}
    ipss_mask = df_day.get("Product Group", pd.Series(dtype=str)).astype(str).str.upper().str.contains("IPSS", na=False)
    return {
        "IPSS": int(df_day.loc[ipss_mask, "Qty"].sum()),
        "PSS":  int(df_day.loc[~ipss_mask, "Qty"].sum()),
    }


def calc_shipment(df_daily_input_raw: pd.DataFrame, date: datetime) -> int:
    if df_daily_input_raw is None or df_daily_input_raw.empty:
        return 0
    date_only = pd.Timestamp(date).normalize()
    try:
        for _, row in df_daily_input_raw.iterrows():
            try:
                row_date = pd.Timestamp(row.iloc[1]).normalize()
                if row_date == date_only:
                    val = row.iloc[4]
                    if pd.notna(val):
                        return int(float(val))
            except:
                continue
    except:
        pass
    return 0


def calc_pr_rw(df_hold: pd.DataFrame, date: datetime) -> dict:
    if df_hold.empty:
        return {"total_hold_qty": 0, "pr_rw_qty": 0, "by_code": {}, "active_hold": 0}
    df = df_hold.copy()
    hold_col = next((c for c in df.columns if "Hold date" in str(c)), None)
    if not hold_col:
        return {"total_hold_qty": 0, "pr_rw_qty": 0, "by_code": {}, "active_hold": 0}
    df[hold_col] = _norm_date(df[hold_col])
    date_only = pd.Timestamp(date).normalize()
    day_mask = df[hold_col].dt.normalize() == date_only
    if "Product Group" in df.columns:
        day_mask &= df["Product Group"].astype(str).str.upper().str.contains("IPSS", na=False)
    df_day = df[day_mask]
    total_hold = int(df_day["Qty"].sum()) if "Qty" in df_day.columns else 0
    pr_mask = df_day.get("Hold Code", pd.Series(dtype=str)).astype(str).str.upper().str.startswith("PR")
    pr_df = df_day[pr_mask]
    pr_rw_qty = int(pr_df["Qty"].sum()) if "Qty" in pr_df.columns else 0
    by_code = {}
    if not pr_df.empty and "Hold Code" in pr_df.columns:
        for code, grp in pr_df.groupby("Hold Code"):
            by_code[str(code)] = int(grp["Qty"].sum())
    release_col = next((c for c in df.columns if "Release time" in str(c)), None)
    active_hold = 0
    if release_col:
        act_mask = df["Qty"].notna()
        if "Product Group" in df.columns:
            act_mask &= df["Product Group"].astype(str).str.upper().str.contains("IPSS", na=False)
        active_hold = int(df.loc[act_mask & df[release_col].isna(), "Qty"].sum())
    return {"total_hold_qty": total_hold, "pr_rw_qty": pr_rw_qty, "by_code": by_code, "active_hold": active_hold}


def get_week_label(dt: datetime, week_start_day: int = 4) -> str:
    # Use ISO calendar from the date itself to match the Year/Week columns
    iso_yr, iso_wk, _ = dt.isocalendar()
    return f"{iso_yr}-W{iso_wk:02d}"


def calc_daily_summary(raw_data: dict, date: datetime, week_start_day: int = 4) -> dict:
    df_wip         = raw_data.get("wip",          pd.DataFrame())
    df_move        = raw_data.get("move",         pd.DataFrame())
    df_input       = raw_data.get("input",        pd.DataFrame())
    df_hold        = raw_data.get("hold_history", pd.DataFrame())
    df_daily_input = raw_data.get("daily_input",  pd.DataFrame())

    for df, cols in [
        (df_move,  ["Date"]),
        (df_input, ["Date"]),
        (df_hold,  [c for c in df_hold.columns if "Hold date" in str(c)]),
    ]:
        for col in cols:
            if not df.empty and col in df.columns:
                df[col] = _norm_date(df[col])

    wip        = calc_wip_by_step(df_wip)
    move_today = calc_movement_by_step(df_move, date)
    dev_move   = calc_develop_move(df_move, date)
    inp        = calc_input(df_input, date)
    pr_rw      = calc_pr_rw(df_hold, date)
    shipment   = calc_shipment(df_daily_input, date)

    total_wip  = sum(wip.values())
    total_move = sum(move_today.values())
    pr_rw_rate = round(pr_rw["pr_rw_qty"] / dev_move * 100, 2) if dev_move > 0 else 0.0

    row = {
        "Date":         date.strftime("%Y-%m-%d"),
        "Year":         date.year,
        "Month":        date.month,
        "Week":         date.isocalendar()[1],
        "WeekLabel":    get_week_label(date, week_start_day),
        "Input_IPSS":   inp["IPSS"],
        "Input_PSS":    inp["PSS"],
        "EPI_Shipment": shipment,
        "Total_WIP":    total_wip,
        "Total_Move":   total_move,
        "Develop_Move": dev_move,
        "PR_RW_Qty":    pr_rw["pr_rw_qty"],
        "PR_RW_Rate":   pr_rw_rate,
        "Active_Hold":  pr_rw["active_hold"],
    }
    for step in STEPS:
        row[f"WIP_{step}"]  = wip.get(step, 0)
        row[f"Move_{step}"] = move_today.get(step, 0)
    for code, qty in sorted(pr_rw["by_code"].items()):
        row[f"PR_{code}"] = qty
    return row


def upsert_trend(df_trend: pd.DataFrame, new_row: dict) -> pd.DataFrame:
    date_key = str(new_row.get("Date", ""))
    if df_trend.empty or "Date" not in df_trend.columns:
        return pd.DataFrame([new_row])
    df_trend = df_trend.copy()
    # Remove duplicate columns before any operation
    df_trend = df_trend.loc[:, ~df_trend.columns.duplicated(keep="first")]
    df_trend["Date"] = df_trend["Date"].astype(str)
    mask = df_trend["Date"] == date_key
    if mask.any():
        idx = df_trend.index[mask][0]
        for col, val in new_row.items():
            if col not in df_trend.columns:
                df_trend[col] = None
            
            # Simple Smart Update logic:
            # 1. If existing value is > 0 and new val is 0, DON'T overwrite (preserve history/manual input)
            #    Exceptions: Steps (WIP/Move) and PR are always updated as they come from RAW snapshots.
            is_step = col.startswith(("WIP_", "Move_", "PR_"))
            current_val = _si(df_trend.at[idx, col])
            new_val = _si(val)
            
            if is_step:
                # Steps are always overwritten with latest RAW state
                df_trend.at[idx, col] = val
            elif new_val != 0 or current_val == 0:
                # Update if new has data OR if old was empty
                df_trend.at[idx, col] = val
            # Else: keep old value (e.g. don't overwrite manual shipment with 0)
    else:
        df_trend = pd.concat([df_trend, pd.DataFrame([new_row])], ignore_index=True)
    df_trend["_dt"] = pd.to_datetime(df_trend["Date"], errors="coerce")
    df_trend = df_trend.sort_values("_dt", ascending=False).drop(columns=["_dt"])
    return df_trend.reset_index(drop=True)
