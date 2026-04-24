"""
IPSS Daily Production Report — Streamlit App
Full pipeline: Load RAW files → Update 4 RAW sheets → Tính toán → Update SUMMARY/WEEKLY/MONTHLY
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json, os, sys, glob, traceback
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from modules.data_loader  import load_separate_files
from modules.calculator   import STEPS, STEP_NAMES, get_week_label, upsert_trend
from modules.excel_updater import update_report_v3, _dfr, PR_COLS, PR_DESC
from modules.email_sender import send_via_outlook, preview_email
from modules.scheduler    import start_scheduler, stop_scheduler, is_running, log_job, get_log

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IPSS Daily Report",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Design System ──────────────────────────────────────────────────────────────
C_NAVY   = "#1F3864"
C_BLUE   = "#1565C0"
C_LBLUE  = "#1E88E5"
C_TEAL   = "#00695C"
C_GREEN  = "#2E7D32"
C_AMBER  = "#F57F17"
C_RED    = "#C62828"
C_LRED   = "#EF5350"
C_PURPLE = "#6A1B9A"
C_GRAY   = "#546E7A"
C_BG     = "#F5F7FA"
C_CARD   = "#FFFFFF"

# Plotly layout defaults
_LAY = dict(
    plot_bgcolor="white",
    paper_bgcolor="white",
    hovermode="x unified",
    font=dict(family="Arial, sans-serif", size=11, color="#333333"),
    legend=dict(orientation="h", y=1.12, font=dict(size=10)),
    margin=dict(t=60, b=50, l=55, r=25),
    xaxis=dict(showgrid=False, linecolor="#DEE2E6", linewidth=1),
    yaxis=dict(gridcolor="#F0F0F0", linecolor="#DEE2E6", linewidth=1, gridwidth=1),
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{ font-family:'IBM Plex Sans',sans-serif; background:#F5F7FA; }
[data-testid="stSidebar"]{ background:#1F3864; }
[data-testid="stSidebar"] *{ color:#E8EDF2 !important; }
[data-testid="stSidebar"] .stButton button{
    background:#2F5597; border:none; border-radius:6px; color:#fff;
    font-weight:600; width:100%;
}
[data-testid="stSidebar"] .stButton button:hover{ background:#1565C0; }

/* Banner */
.rpt-banner{
    background:linear-gradient(135deg,#1F3864 0%,#1565C0 100%);
    color:#fff; padding:14px 24px; border-radius:10px;
    font-size:18px; font-weight:700; margin-bottom:16px;
    display:flex; align-items:center; gap:10px;
    box-shadow:0 2px 8px rgba(31,56,100,.25);
}
.rpt-date{ font-size:13px; font-weight:400; opacity:.85; margin-left:auto; }

/* KPI Card */
.kpi-wrap{ display:flex; gap:10px; margin-bottom:16px; }
.kpi-card{
    flex:1; background:#fff; border-radius:10px; padding:14px 12px 12px;
    box-shadow:0 1px 4px rgba(0,0,0,.08);
    border-top:4px solid var(--kc); position:relative; overflow:hidden;
}
.kpi-card::after{
    content:''; position:absolute; right:-10px; top:-10px;
    width:60px; height:60px; border-radius:50%;
    background:var(--kc); opacity:.06;
}
.kpi-icon{ font-size:18px; margin-bottom:4px; }
.kpi-lbl{ font-size:10px; color:#6B7280; text-transform:uppercase;
           letter-spacing:.06em; font-weight:600; }
.kpi-val{ font-size:24px; font-weight:700; color:var(--kc);
          font-family:'IBM Plex Mono',monospace; line-height:1.1; margin:2px 0 4px; }
.kpi-unit{ font-size:10px; color:#9CA3AF; }
.kpi-delta-up{ color:#2E7D32; font-size:11px; font-weight:600; }
.kpi-delta-dn{ color:#C62828; font-size:11px; font-weight:600; }
.kpi-delta-flat{ color:#9CA3AF; font-size:11px; }
.kpi-vs{ font-size:10px; color:#D1D5DB; margin-top:2px; }

/* Section header */
.sec-hdr{
    background:linear-gradient(90deg,#1F3864,#2F5597);
    color:#fff; padding:7px 16px; border-radius:6px;
    font-weight:700; margin:14px 0 8px 0; font-size:12px;
    letter-spacing:.06em; text-transform:uppercase;
}

/* Alert */
.alert-red{
    background:#FEF2F2; border-left:4px solid #C62828; border-radius:6px;
    padding:10px 14px; color:#7F1D1D; font-size:13px; margin-bottom:8px;
}
.alert-amber{
    background:#FFFBEB; border-left:4px solid #F57F17; border-radius:6px;
    padding:10px 14px; color:#78350F; font-size:13px; margin-bottom:8px;
}
.alert-green{
    background:#F0FDF4; border-left:4px solid #2E7D32; border-radius:6px;
    padding:10px 14px; color:#14532D; font-size:13px; margin-bottom:8px;
}

/* Status badge */
.badge-red{ background:#FEE2E2; color:#991B1B; padding:2px 8px;
            border-radius:12px; font-size:11px; font-weight:600; }
.badge-amber{ background:#FEF3C7; color:#92400E; padding:2px 8px;
              border-radius:12px; font-size:11px; font-weight:600; }
.badge-green{ background:#D1FAE5; color:#065F46; padding:2px 8px;
              border-radius:12px; font-size:11px; font-weight:600; }

/* Data table */
.stDataFrame { border-radius:8px; overflow:hidden; }
</style>
""", unsafe_allow_html=True)

# ── Config ─────────────────────────────────────────────────────────────────────
CFG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
DEFAULT_CFG = {
    "template_file": "",
    "raw_paths": {"hold_history":"","wip":"","move":"","input":""},
    "output_folder": "reports",
    "report_prefix": "IPSS_DAILY_REPORT_V03",
    "week_start_day": 4,
    "week_start_label": "Thứ 6 (Friday)",
    "chart_days": 30,
    "df_rate_limit": 15,
    "email": {
        "to":[], "cc":[],
        "subject_template":"[IPSS Daily Report] {date}",
        "body_template":(
            "Kính gửi,\n\nBáo cáo sản xuất IPSS ngày {date}:\n\n"
            "📥 Input IPSS   : {input_ipss} wafers\n"
            "🚢 EPI Shipment : {shipment} wafers\n"
            "📊 DF Rate      : {pr_rw_rate}%\n"
            "🔄 Total Move   : {total_move} wafers\n"
            "📦 Total WIP    : {total_wip} wafers\n\n"
            "Vui lòng xem file đính kèm.\n\nTrân trọng,\n{sender_name}"
        ),
        "sender_name":"IPSS Report System",
        "schedule_time":"08:00",
        "auto_send": False,
    },
}

def load_cfg():
    if os.path.exists(CFG_PATH):
        with open(CFG_PATH,"r",encoding="utf-8") as f: d=json.load(f)
        for k,v in DEFAULT_CFG.items():
            if k not in d: d[k]=v
        return d
    return DEFAULT_CFG.copy()

def save_cfg(cfg):
    with open(CFG_PATH,"w",encoding="utf-8") as f:
        json.dump(cfg,f,ensure_ascii=False,indent=2)

# ── File helpers ───────────────────────────────────────────────────────────────
def find_report_by_date(cfg, target_date: datetime):
    folder = cfg.get("output_folder","reports")
    prefix = cfg.get("report_prefix","IPSS_DAILY_REPORT_V03")
    if not os.path.exists(folder): return ""
    for ds in [target_date.strftime("%Y%m%d"), target_date.strftime("%Y-%m-%d")]:
        files = glob.glob(os.path.join(folder, f"*{ds}*{prefix}*.xlsx"))
        if files: return sorted(files, reverse=True)[0]
    return ""

def find_latest_report(cfg, before_date=None):
    folder = cfg.get("output_folder","reports")
    prefix = cfg.get("report_prefix","IPSS_DAILY_REPORT_V03")
    if not os.path.exists(folder): return ""
    valid = []
    for f in glob.glob(os.path.join(folder, f"*{prefix}*.xlsx")):
        try:
            ds = os.path.basename(f).split("_")[0]
            fd = datetime.strptime(ds, "%Y%m%d")
            if before_date and fd >= before_date: continue
            valid.append((fd, f))
        except: continue
    return sorted(valid, key=lambda x: x[0], reverse=True)[0][1] if valid else ""

# ── Data loaders ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_summary_df(report_path: str) -> pd.DataFrame:
    if not report_path or not os.path.exists(report_path): return pd.DataFrame()
    try:
        ws_raw = pd.read_excel(report_path, sheet_name="SUMMARY",
                               engine="openpyxl", header=None)
        header_row_idx = None
        for r in range(min(15, ws_raw.shape[0])):
            row_vals = [str(ws_raw.iloc[r,ci] or "").strip() for ci in range(min(10, ws_raw.shape[1]))]
            if "Input IPSS" in row_vals or "Input PSS" in row_vals:
                header_row_idx = r; break
            if str(ws_raw.iloc[r,0] or "").strip() in ("Ngày","Date","Label"):
                header_row_idx = r; break
        if header_row_idx is None: return pd.DataFrame()
        headers = [str(ws_raw.iloc[header_row_idx,ci] or "").strip() for ci in range(ws_raw.shape[1])]
        df = ws_raw.iloc[header_row_idx+1:].copy()
        df.columns = headers[:len(df.columns)]
        df = df[df.iloc[:,0].notna()].copy()
        df = df[df.iloc[:,0].astype(str).str.strip() != ""].copy()
        df = df.rename(columns={df.columns[0]: "Date"})
        df.columns = [c if c else f"col{i}" for i,c in enumerate(df.columns)]
        rename = {
            "Năm":"Year","Tháng":"Month","Tuần ISO":"Week_ISO",
            "MONTH":"MONTH_Label","Week":"Week_Label",
            "Input PSS":"Input_PSS","Input IPSS":"Input_IPSS",
            "EPI Shipment":"EPI_Shipment","Total WIP":"Total_WIP","Total Move":"Total_Move",
            "E3157 (Develop)":"E3157_Move","PR RW (qty)":"PR_RW_Qty","DF Rate %":"DF_Rate",
        }
        df = df.rename(columns=rename)
        seen_steps = []; new_cols = []
        for c in df.columns:
            base = str(c).replace(".1","").replace(".2","")
            if base in STEPS:
                if base not in seen_steps: new_cols.append(f"WIP_{base}"); seen_steps.append(base)
                else: new_cols.append(f"Move_{base}")
            else: new_cols.append(str(c))
        df.columns = new_cols
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", format='mixed')
        df = df.dropna(subset=["Date"]).sort_values("Date")
        if "E3157_Move" in df.columns and "PR_RW_Qty" in df.columns:
            e3 = pd.to_numeric(df["E3157_Move"], errors="coerce").fillna(0)
            pr = pd.to_numeric(df["PR_RW_Qty"],  errors="coerce").fillna(0)
            df["DF_Rate"] = np.where(e3>0,(pr/e3*100).round(2),0.0)
        num_cols = (["Input_PSS","Input_IPSS","EPI_Shipment","Total_WIP","Total_Move",
                     "E3157_Move","PR_RW_Qty","DF_Rate"] +
                    [f"WIP_{s}" for s in STEPS] + [f"Move_{s}" for s in STEPS] +
                    [f"PR_{c}" for c in PR_COLS if f"PR_{c}" in df.columns])
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=60)
def load_lot_list(report_path: str) -> pd.DataFrame:
    if not report_path or not os.path.exists(report_path): return pd.DataFrame()
    try:
        df = pd.read_excel(report_path, sheet_name="PR RW LOT LIST", engine="openpyxl")
        df.columns = df.columns.astype(str).str.strip()
        if "DAY" in df.columns: df["DAY"] = pd.to_datetime(df["DAY"], errors="coerce")
        return df
    except: return pd.DataFrame()

# ── Chart helpers ──────────────────────────────────────────────────────────────
def _prep(df, days=None):
    d = df.copy()
    d["_dt"] = pd.to_datetime(d.get("Date", pd.Series(dtype=str)), errors="coerce")
    d = d.dropna(subset=["_dt"]).sort_values("_dt")
    if days:
        cutoff = d["_dt"].max() - timedelta(days=days)
        d = d[d["_dt"] >= cutoff]
    return d

def _wgrp(df, wsd, cols):
    d = df.copy(); d["WL"] = d["_dt"].apply(lambda x: get_week_label(x, wsd))
    return d.groupby("WL")[cols].sum().reset_index().rename(columns={"WL":"x"})

def _mgrp(df, cols):
    d = df.copy(); d["ML"] = d["_dt"].dt.strftime("%Y-%m")
    return d.groupby("ML")[cols].sum().reset_index().rename(columns={"ML":"x"})

def _moving_avg(series, window=7):
    return series.rolling(window=window, min_periods=1).mean()

def _bar_colors(values, limit, warn=None):
    """Color bars: green < warn, amber warn–limit, red > limit"""
    warn = warn or limit * 0.7
    colors = []
    for v in values:
        if v > limit:   colors.append(C_RED)
        elif v > warn:  colors.append(C_AMBER)
        else:           colors.append(C_GREEN)
    return colors

# ── CHART 1: Input & Shipment ─────────────────────────────────────────────────
def chart_input(df, view, wsd, days):
    df = _prep(df, days if view=="Daily" else None)
    fig = go.Figure()
    base_lay = dict(**_LAY)
    base_lay["legend"] = dict(orientation="h", y=1.12, font=dict(size=10))

    if view == "Daily":
        x = df["Date"].dt.strftime("%m/%d")
        ipss = df.get("Input_IPSS", pd.Series(0, index=df.index)).fillna(0)
        pss  = df.get("Input_PSS",  pd.Series(0, index=df.index)).fillna(0)
        ship = df.get("EPI_Shipment", pd.Series(0, index=df.index)).fillna(0)
        ma7  = _moving_avg(ipss+pss)

        fig.add_bar(x=x, y=ipss, name="Input IPSS", marker_color=C_BLUE,
                    marker_line_width=0, opacity=0.9)
        fig.add_bar(x=x, y=pss,  name="Input PSS",  marker_color="#90CAF9",
                    marker_line_width=0, opacity=0.85)
        fig.add_scatter(x=x, y=ship, name="EPI Shipment",
                        mode="lines+markers", line=dict(color=C_RED, width=2.5),
                        marker=dict(size=7, symbol="diamond", color=C_RED))
        fig.add_scatter(x=x, y=ma7, name="7-day MA (Input)",
                        mode="lines", line=dict(color=C_AMBER, width=1.5, dash="dot"),
                        opacity=0.8)
        fig.update_layout(barmode="stack", **base_lay)

    elif view == "Weekly":
        grp = _wgrp(df, wsd, ["Input_IPSS","Input_PSS","EPI_Shipment"]).tail(16)
        fig.add_bar(x=grp["x"], y=grp["Input_IPSS"], name="Input IPSS",
                    marker_color=C_BLUE, marker_line_width=0)
        fig.add_bar(x=grp["x"], y=grp["Input_PSS"],  name="Input PSS",
                    marker_color="#90CAF9", marker_line_width=0)
        fig.add_scatter(x=grp["x"], y=grp["EPI_Shipment"], name="EPI Shipment",
                        mode="lines+markers", line=dict(color=C_RED, width=2.5),
                        marker=dict(size=7, symbol="diamond", color=C_RED))
        fig.update_layout(barmode="stack", **base_lay)

    else:
        grp = _mgrp(df, ["Input_IPSS","Input_PSS","EPI_Shipment"])
        fig.add_bar(x=grp["x"], y=grp["Input_IPSS"], name="Input IPSS",
                    marker_color=C_BLUE, marker_line_width=0)
        fig.add_bar(x=grp["x"], y=grp["Input_PSS"],  name="Input PSS",
                    marker_color="#90CAF9", marker_line_width=0)
        fig.add_scatter(x=grp["x"], y=grp["EPI_Shipment"], name="EPI Shipment",
                        mode="lines+markers", line=dict(color=C_RED, width=2.5),
                        marker=dict(size=7, symbol="diamond", color=C_RED))
        fig.update_layout(barmode="stack", **base_lay)

    fig.update_layout(
        title=dict(text=f"<b>INPUT & EPI SHIPMENT</b>  <span style='font-size:11px;color:#666'>({view})</span>",
                   font=dict(size=13, color=C_NAVY)),
        height=340, yaxis_title="Wafers",
    )
    fig.update_xaxes(tickangle=-30)
    return fig

# ── CHART 2: DF Rate Control Chart ────────────────────────────────────────────
def chart_dfrate(df, view, wsd, days, limit=15):
    df_p = _prep(df, days if view=="Daily" else None)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    warn = limit * 0.7

    def _plot_control(x, dev, pr, dfr_vals):
        # Bar colors based on DF Rate value
        bc = _bar_colors(dfr_vals, limit, warn)
        # E3157 background bars
        fig.add_bar(x=x, y=dev, name="E3157 Move", marker_color="#B3E5FC",
                    marker_line_width=0, opacity=0.6, secondary_y=False)
        fig.add_bar(x=x, y=pr, name="PR RW Qty",
                    marker_color=C_LRED, marker_line_width=0, secondary_y=False)
        # DF Rate line with colored markers
        fig.add_scatter(x=x, y=dfr_vals, name="DF Rate %",
                        mode="lines+markers",
                        line=dict(color=C_NAVY, width=2),
                        marker=dict(size=8, color=bc,
                                    line=dict(color=C_NAVY, width=1)),
                        secondary_y=True)
        # Mean line
        mean_val = np.mean([v for v in dfr_vals if v > 0]) if any(v > 0 for v in dfr_vals) else 0
        if mean_val > 0:
            fig.add_hline(y=mean_val, line_dash="dot", line_color=C_GRAY,
                          line_width=1, secondary_y=True,
                          annotation_text=f" Avg:{mean_val:.1f}%",
                          annotation_font_color=C_GRAY, annotation_font_size=10)

    if view == "Daily":
        x = df_p["Date"].dt.strftime("%m/%d")
        dev = df_p.get("E3157_Move", pd.Series(0, index=df_p.index)).fillna(0).tolist()
        pr  = df_p.get("PR_RW_Qty",  pd.Series(0, index=df_p.index)).fillna(0).tolist()
        dfr = df_p.get("DF_Rate",    pd.Series(0, index=df_p.index)).fillna(0).tolist()
        _plot_control(x.tolist(), dev, pr, dfr)
    elif view == "Weekly":
        grp = _wgrp(df_p, wsd, ["E3157_Move","PR_RW_Qty"]).tail(16)
        grp["DF"] = np.where(grp["E3157_Move"]>0,
                             (grp["PR_RW_Qty"]/grp["E3157_Move"]*100).round(2), 0)
        _plot_control(grp["x"].tolist(), grp["E3157_Move"].tolist(),
                      grp["PR_RW_Qty"].tolist(), grp["DF"].tolist())
    else:
        grp = _mgrp(df_p, ["E3157_Move","PR_RW_Qty"])
        grp["DF"] = np.where(grp["E3157_Move"]>0,
                             (grp["PR_RW_Qty"]/grp["E3157_Move"]*100).round(2), 0)
        _plot_control(grp["x"].tolist(), grp["E3157_Move"].tolist(),
                      grp["PR_RW_Qty"].tolist(), grp["DF"].tolist())

    # Limit & warning lines
    fig.add_hline(y=limit, line_dash="dash", line_color=C_RED, line_width=2,
                  secondary_y=True,
                  annotation_text=f" Limit:{limit}%",
                  annotation_font_color=C_RED, annotation_font_size=10)
    fig.add_hline(y=warn, line_dash="dot", line_color=C_AMBER, line_width=1.5,
                  secondary_y=True,
                  annotation_text=f" Warn:{warn:.0f}%",
                  annotation_font_color=C_AMBER, annotation_font_size=10)

    lay = dict(**_LAY)
    lay.pop("yaxis", None)
    fig.update_layout(
        title=dict(text=f"<b>DF RATE — CONTROL CHART</b>  <span style='font-size:11px;color:#666'>({view})</span>",
                   font=dict(size=13, color=C_NAVY)),
        barmode="overlay", height=340,
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
        font=dict(family="Arial, sans-serif", size=11, color="#333"),
        legend=dict(orientation="h", y=1.12, font=dict(size=10)),
        margin=dict(t=60, b=50, l=55, r=25),
    )
    fig.update_yaxes(title_text="Wafers", secondary_y=False, gridcolor="#F0F0F0")
    fig.update_yaxes(title_text="DF Rate %", secondary_y=True, showgrid=False,
                     tickformat=".1f")
    fig.update_xaxes(showgrid=False, tickangle=-30)
    return fig

# ── CHART 3: WIP Snapshot (horizontal bar sorted) ─────────────────────────────
def chart_wip_snapshot(today_row):
    wc  = [f"WIP_{s}" for s in STEPS]
    vals = [float(today_row.get(c, 0) or 0) for c in wc]
    names = [f"{s} ({STEP_NAMES.get(s,'')[:10]})" for s in STEPS]
    total = sum(vals) or 1
    pcts  = [v/total*100 for v in vals]

    # Sort descending
    paired = sorted(zip(vals, pcts, names, STEPS), reverse=True)
    vals_s, pcts_s, names_s, steps_s = zip(*paired) if paired else ([],[],[],[])

    # Color by % threshold
    colors = []
    for p in pcts_s:
        if p > 20:   colors.append(C_RED)
        elif p > 12: colors.append(C_AMBER)
        elif p > 5:  colors.append(C_BLUE)
        else:        colors.append("#B0BEC5")

    fig = go.Figure(go.Bar(
        x=list(pcts_s), y=list(names_s),
        orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        text=[f"{v:,.0f}  ({p:.1f}%)" for v, p in zip(vals_s, pcts_s)],
        textposition="outside",
        textfont=dict(size=10),
        cliponaxis=False,
    ))
    fig.update_layout(
        title=dict(text=f"<b>WIP DISTRIBUTION BY STEP</b>  <span style='font-size:11px;color:#666'>Total: {int(total):,} wafers</span>",
                   font=dict(size=13, color=C_NAVY)),
        height=400, xaxis_title="%",
        plot_bgcolor="white", paper_bgcolor="white",
        font=dict(family="Arial", size=10, color="#333"),
        margin=dict(t=55, b=40, l=200, r=120),
        xaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
        yaxis=dict(autorange="reversed"),
        showlegend=False,
    )
    # Threshold lines
    fig.add_vline(x=20, line_dash="dash", line_color=C_RED, line_width=1.5,
                  annotation_text="20%", annotation_font_color=C_RED, annotation_font_size=10)
    fig.add_vline(x=12, line_dash="dot", line_color=C_AMBER, line_width=1,
                  annotation_text="12%", annotation_font_color=C_AMBER, annotation_font_size=10)
    return fig

# ── CHART 4: WIP Trend (stacked area) ────────────────────────────────────────
def chart_wip_trend(df, view, wsd, days):
    wc = [f"WIP_{s}" for s in STEPS if f"WIP_{s}" in df.columns]
    if not wc: return go.Figure()
    df_p = _prep(df, days if view=="Daily" else None)
    palette = px.colors.qualitative.Plotly + px.colors.qualitative.Set2
    fig = go.Figure()

    if view == "Daily":
        for i, c in enumerate(wc):
            fig.add_scatter(x=df_p["Date"].dt.strftime("%m/%d"), y=df_p[c].fillna(0),
                            name=c.replace("WIP_",""), mode="lines",
                            line=dict(width=1.2, color=palette[i % len(palette)]),
                            stackgroup="one", fillcolor=palette[i % len(palette)])
    elif view == "Weekly":
        grp = _wgrp(df_p, wsd, wc).tail(12)
        for i, c in enumerate(wc):
            fig.add_bar(x=grp["x"], y=grp[c].fillna(0), name=c.replace("WIP_",""),
                        marker_color=palette[i % len(palette)], marker_line_width=0)
        fig.update_layout(barmode="stack")
    else:
        grp = _mgrp(df_p, wc)
        for i, c in enumerate(wc):
            fig.add_bar(x=grp["x"], y=grp[c].fillna(0), name=c.replace("WIP_",""),
                        marker_color=palette[i % len(palette)], marker_line_width=0)
        fig.update_layout(barmode="stack")

    lay = dict(**_LAY)
    lay["legend"] = dict(orientation="h", y=-0.28, font=dict(size=9))
    lay["margin"] = dict(t=55, b=100, l=55, r=25)
    fig.update_layout(
        title=dict(text=f"<b>WIP TREND BY STEP</b>  <span style='font-size:11px;color:#666'>({view})</span>",
                   font=dict(size=13, color=C_NAVY)),
        height=370, yaxis_title="Wafers", **lay)
    fig.update_xaxes(tickangle=-30)
    return fig

# ── CHART 5: Movement (stacked area) ─────────────────────────────────────────
def chart_movement(df, view, wsd, days):
    mc = [c for c in df.columns if c.startswith("Move_")]
    if not mc: return go.Figure()
    df_p = _prep(df, days if view=="Daily" else None)
    palette = px.colors.qualitative.Plotly + px.colors.qualitative.Set2
    fig = go.Figure()

    if view == "Daily":
        for i, c in enumerate(mc):
            fig.add_scatter(x=df_p["Date"].dt.strftime("%m/%d"), y=df_p[c].fillna(0),
                            name=c.replace("Move_",""), mode="lines",
                            line=dict(width=1.2, color=palette[i % len(palette)]),
                            stackgroup="one")
    elif view == "Weekly":
        grp = _wgrp(df_p, wsd, mc).tail(12)
        for i, c in enumerate(mc):
            fig.add_bar(x=grp["x"], y=grp[c].fillna(0), name=c.replace("Move_",""),
                        marker_color=palette[i % len(palette)], marker_line_width=0)
        fig.update_layout(barmode="stack")
    else:
        grp = _mgrp(df_p, mc)
        for i, c in enumerate(mc):
            fig.add_bar(x=grp["x"], y=grp[c].fillna(0), name=c.replace("Move_",""),
                        marker_color=palette[i % len(palette)], marker_line_width=0)
        fig.update_layout(barmode="stack")

    lay = dict(**_LAY)
    lay["legend"] = dict(orientation="h", y=-0.28, font=dict(size=9))
    lay["margin"] = dict(t=55, b=100, l=55, r=25)
    fig.update_layout(
        title=dict(text=f"<b>MOVEMENT BY STEP</b>  <span style='font-size:11px;color:#666'>({view})</span>",
                   font=dict(size=13, color=C_NAVY)),
        height=370, yaxis_title="Wafers", **lay)
    fig.update_xaxes(tickangle=-30)
    return fig

# ── CHART 6: E3157 vs PR RW (dual axis) ──────────────────────────────────────
def chart_e3157_prrw(df, days):
    df_p = _prep(df, days)
    if df_p.empty: return go.Figure()
    x   = df_p["Date"].dt.strftime("%m/%d")
    dev = df_p.get("E3157_Move", pd.Series(0, index=df_p.index)).fillna(0)
    pr  = df_p.get("PR_RW_Qty",  pd.Series(0, index=df_p.index)).fillna(0)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_bar(x=x, y=dev, name="E3157 (Develop)", marker_color="#B3E5FC",
                marker_line_width=0, secondary_y=False)
    fig.add_scatter(x=x, y=pr, name="PR RW Qty",
                    mode="lines+markers", line=dict(color=C_RED, width=2.5),
                    marker=dict(size=6, color=C_RED), secondary_y=True)
    fig.add_scatter(x=x, y=_moving_avg(pr, 7), name="PR RW 7-day MA",
                    mode="lines", line=dict(color=C_AMBER, width=1.5, dash="dot"),
                    secondary_y=True)
    fig.update_layout(
        title=dict(text="<b>E3157 DEVELOP MOVE  vs  PR REWORK</b>",
                   font=dict(size=13, color=C_NAVY)),
        barmode="overlay", height=310,
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
        font=dict(family="Arial", size=11),
        margin=dict(t=55, b=50, l=55, r=25),
        legend=dict(orientation="h", y=1.12, font=dict(size=10)),
    )
    fig.update_yaxes(title_text="E3157 Wafers", secondary_y=False, gridcolor="#F0F0F0")
    fig.update_yaxes(title_text="PR RW Qty", secondary_y=True, showgrid=False)
    fig.update_xaxes(showgrid=False, tickangle=-30)
    return fig

# ── CHART 7: PR Pareto ────────────────────────────────────────────────────────
def chart_pr_pareto(df, period="All"):
    pr_cols = [f"PR_{c}" for c in PR_COLS if f"PR_{c}" in df.columns]
    if not pr_cols: return go.Figure()

    if period == "MTD":
        df_p = _prep(df)
        curr_mo = df_p["_dt"].max().strftime("%Y-%m")
        df_p = df_p[df_p["_dt"].dt.strftime("%Y-%m") == curr_mo]
        totals = df_p[pr_cols].sum()
    elif period == "Last 30d":
        df_p = _prep(df, 30)
        totals = df_p[pr_cols].sum()
    else:
        totals = df[pr_cols].sum()

    totals = totals[totals > 0].sort_values(ascending=False)
    if totals.empty: return go.Figure()

    grand = totals.sum()
    cum_pct = (totals.cumsum() / grand * 100).values
    labels = [f"{c.replace('PR_','')} {PR_DESC.get(c.replace('PR_',''),'')[:15]}"
              for c in totals.index]

    # Color: first bars forming 80% in deep red, rest lighter
    bar_colors = []
    for cp in cum_pct:
        if cp <= 80:   bar_colors.append(C_RED)
        elif cp <= 95: bar_colors.append(C_AMBER)
        else:          bar_colors.append("#B0BEC5")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_bar(x=labels, y=totals.values, name="PR RW Qty",
                marker=dict(color=bar_colors, line=dict(width=0)),
                secondary_y=False,
                text=[f"{int(v):,}" for v in totals.values],
                textposition="outside", textfont=dict(size=9))
    fig.add_scatter(x=labels, y=cum_pct, name="Cumulative %",
                    mode="lines+markers", line=dict(color=C_NAVY, width=2),
                    marker=dict(size=6, color=C_NAVY), secondary_y=True)
    # 80% Pareto line
    fig.add_hline(y=80, line_dash="dash", line_color=C_RED, line_width=1.5,
                  secondary_y=True,
                  annotation_text=" 80% Pareto",
                  annotation_font_color=C_RED, annotation_font_size=10)
    fig.update_layout(
        title=dict(text=f"<b>PR CODE PARETO ANALYSIS</b>  <span style='font-size:11px;color:#666'>({period}) — Total: {int(grand):,}</span>",
                   font=dict(size=13, color=C_NAVY)),
        height=380, plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
        font=dict(family="Arial", size=10),
        margin=dict(t=55, b=80, l=55, r=25),
        legend=dict(orientation="h", y=1.12, font=dict(size=10)),
        showlegend=True,
    )
    fig.update_xaxes(tickangle=-45, showgrid=False)
    fig.update_yaxes(title_text="Qty (wafers)", secondary_y=False, gridcolor="#F0F0F0")
    fig.update_yaxes(title_text="Cumulative %", secondary_y=True, range=[0, 105],
                     showgrid=False, ticksuffix="%")
    return fig

# ── CHART 8: PR Trend (top codes) ────────────────────────────────────────────
def chart_pr_trend(df, days, top_n=6):
    pr_cols = [f"PR_{c}" for c in PR_COLS if f"PR_{c}" in df.columns]
    if not pr_cols: return go.Figure()
    df_p = _prep(df, days)
    totals = df[pr_cols].sum().sort_values(ascending=False)
    top_codes = totals[totals > 0].head(top_n).index.tolist()
    if not top_codes: return go.Figure()

    palette = [C_RED, C_AMBER, C_BLUE, C_TEAL, C_PURPLE, C_GRAY]
    fig = go.Figure()
    for i, code in enumerate(top_codes):
        if code not in df_p.columns: continue
        lbl = f"{code.replace('PR_','')} – {PR_DESC.get(code.replace('PR_',''),'')}"
        fig.add_scatter(x=df_p["Date"].dt.strftime("%m/%d"), y=df_p[code].fillna(0),
                        name=lbl, mode="lines+markers",
                        line=dict(width=2, color=palette[i % len(palette)]),
                        marker=dict(size=5))
    lay = dict(**_LAY)
    lay["legend"] = dict(orientation="h", y=-0.28, font=dict(size=10))
    lay["margin"] = dict(t=55, b=110, l=55, r=25)
    fig.update_layout(
        title=dict(text=f"<b>TOP {top_n} PR CODES — DAILY TREND</b>",
                   font=dict(size=13, color=C_NAVY)),
        height=340, yaxis_title="Qty", **lay)
    fig.update_xaxes(tickangle=-30)
    return fig

# ── CHART 9: DF Rate daily bar (colored) ─────────────────────────────────────
def chart_dfrate_bar(df, days, limit=15):
    df_p = _prep(df, days)
    if df_p.empty: return go.Figure()
    x   = df_p["Date"].dt.strftime("%m/%d")
    dfr = df_p.get("DF_Rate", pd.Series(0, index=df_p.index)).fillna(0)
    bc  = _bar_colors(dfr.tolist(), limit)
    fig = go.Figure()
    fig.add_bar(x=x, y=dfr, name="DF Rate %",
                marker=dict(color=bc, line=dict(width=0)),
                text=[f"{v:.1f}%" for v in dfr],
                textposition="outside", textfont=dict(size=9))
    mean_val = dfr[dfr > 0].mean() if (dfr > 0).any() else 0
    if mean_val:
        fig.add_hline(y=mean_val, line_dash="dot", line_color=C_GRAY, line_width=1.5,
                      annotation_text=f" Avg {mean_val:.1f}%",
                      annotation_font_color=C_GRAY, annotation_font_size=10)
    fig.add_hline(y=limit, line_dash="dash", line_color=C_RED, line_width=2,
                  annotation_text=f" Limit {limit}%",
                  annotation_font_color=C_RED, annotation_font_size=10)
    fig.update_layout(
        title=dict(text="<b>DF RATE % — DAILY</b>",
                   font=dict(size=13, color=C_NAVY)),
        height=290, yaxis_title="DF Rate %",
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
        font=dict(family="Arial", size=10),
        margin=dict(t=55, b=50, l=55, r=80),
        xaxis=dict(showgrid=False, tickangle=-30),
        yaxis=dict(gridcolor="#F0F0F0"),
        showlegend=False,
    )
    return fig

# ── CHART 10: WIP Heatmap ─────────────────────────────────────────────────────
def chart_wip_heatmap(df, n=21):
    wip_cols = [f"WIP_{s}" for s in STEPS if f"WIP_{s}" in df.columns]
    df_p = _prep(df).tail(n)
    if df_p.empty or not wip_cols: return go.Figure()
    heat = df_p[wip_cols].fillna(0).T
    heat.index = [i.replace("WIP_","") for i in heat.index]
    heat.columns = df_p["Date"].dt.strftime("%m/%d").tolist()
    fig = go.Figure(go.Heatmap(
        z=heat.values, x=heat.columns, y=heat.index,
        colorscale=[[0,"#EFF6FF"],[0.3,"#93C5FD"],[0.7,"#1D4ED8"],[1,"#1E3A5F"]],
        hoverongaps=False,
        hovertemplate="Step: %{y}<br>Date: %{x}<br>WIP: %{z:,}<extra></extra>",
        colorbar=dict(title="WIP", thickness=12, len=0.8),
    ))
    fig.update_layout(
        title=dict(text=f"<b>WIP HEATMAP</b>  <span style='font-size:11px;color:#666'>(Last {n} days)</span>",
                   font=dict(size=13, color=C_NAVY)),
        height=380, margin=dict(t=55, b=40, l=65, r=60),
        font=dict(family="Arial", size=10),
        plot_bgcolor="white", paper_bgcolor="white",
        xaxis=dict(tickangle=-45),
    )
    return fig

# ── Process & Export ────────────────────────────────────────────────────────────
def process_and_export(cfg, today_dt):
    try:
        folders  = cfg.get("raw_folders", {})
        patterns = cfg.get("raw_patterns", {})
        raw = load_separate_files(folders, today_dt, patterns) if folders \
              else load_separate_files(cfg.get("raw_paths", {}), today_dt)

        if not raw or all(df.empty for k,df in raw.items() if not k.endswith("_path")):
            missing = [k for k,df in raw.items() if not k.endswith("_path") and df.empty]
            return {}, "", (f"❌ Không tìm thấy file RAW ngày {today_dt.strftime('%d/%m/%Y')}.\n"
                           f"Thiếu: {', '.join(missing)}")

        yesterday_dt = today_dt - timedelta(days=1)
        prev_report  = find_report_by_date(cfg, yesterday_dt)
        template     = prev_report or find_latest_report(cfg, before_date=today_dt) or cfg.get("template_file","")
        if not template:
            return {}, "", "❌ Không tìm thấy file template V03."

        folder  = cfg.get("output_folder","reports")
        prefix  = cfg.get("report_prefix","IPSS_DAILY_REPORT_V03")
        fname   = f"{today_dt.strftime('%Y%m%d')}_{prefix}.xlsx"
        out_path = os.path.join(folder, fname)
        os.makedirs(folder, exist_ok=True)

        out_path = update_report_v3(
            template_path=template, output_path=out_path,
            raw_data=raw, today_dt=today_dt,
            week_start_day=cfg.get("week_start_day", 4),
        )
        load_summary_df.clear(); load_lot_list.clear()
        df_s = load_summary_df(out_path)
        today_str = today_dt.strftime("%Y-%m-%d")
        today_row = {}
        if not df_s.empty:
            tr = df_s[df_s["Date"].astype(str).str.startswith(today_str)]
            if not tr.empty: today_row = tr.iloc[0].to_dict()
        return today_row, out_path, None
    except Exception as e:
        return {}, "", traceback.format_exc()

# ══════════════════════════════════════════════════════════════════════════════
# MAIN UI
# ══════════════════════════════════════════════════════════════════════════════
def main():
    cfg = load_cfg()

    # ── Sidebar ────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## 📊 IPSS Report")
        st.markdown("---")
        today_date = st.date_input("📅 Ngày báo cáo", value=datetime.now().date())
        today_dt   = datetime.combine(today_date, datetime.min.time())
        st.markdown("---")
        st.markdown(f"**Scheduler:** {'🟢 Running' if is_running() else '🔴 Stopped'}")
        st.markdown("---")

        if st.button("▶️ Xử lý & Xuất báo cáo", type="primary", use_container_width=True):
            with st.spinner("Đang xử lý..."):
                row, path, err = process_and_export(cfg, today_dt)
            if err:
                st.error("❌ Lỗi!")
                st.code(err, language="python")
            else:
                st.success(f"✅ Xong! `{os.path.basename(path)}`")
                st.session_state["last_path"] = path
                st.session_state["last_row"]  = row
                log_job(f"Exported: {path}")
                st.rerun()

        latest_path = st.session_state.get("last_path","") or find_latest_report(cfg)
        if latest_path and os.path.exists(latest_path):
            with open(latest_path,"rb") as f:
                st.download_button("⬇️ Tải file Excel", data=f.read(),
                    file_name=os.path.basename(latest_path),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True)
            st.caption(f"📁 {os.path.basename(latest_path)}")

    # ── Load data ──────────────────────────────────────────────────────────────
    latest_path = st.session_state.get("last_path","") or find_latest_report(cfg)
    df_sum  = load_summary_df(latest_path)
    df_lot  = load_lot_list(latest_path)

    wsd   = cfg.get("week_start_day", 4)
    days  = cfg.get("chart_days", 30)
    limit = cfg.get("df_rate_limit", 15)

    today_str = today_dt.strftime("%Y-%m-%d")
    yest_str  = (today_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    today_row = st.session_state.get("last_row", {})
    yest_row  = {}
    if not df_sum.empty:
        tr = df_sum[df_sum["Date"].astype(str).str.startswith(today_str)]
        yr = df_sum[df_sum["Date"].astype(str).str.startswith(yest_str)]
        if not tr.empty and not today_row: today_row = tr.iloc[0].to_dict()
        if not yr.empty: yest_row = yr.iloc[0].to_dict()

    # Compute quick metrics
    dfr_today   = float(today_row.get("DF_Rate", 0) or 0)
    unreleased  = int(df_lot["Release time"].isna().sum()) if not df_lot.empty and "Release time" in df_lot.columns else 0
    pr_rw_today = int(today_row.get("PR_RW_Qty", 0) or 0)

    # ── Header Banner ─────────────────────────────────────────────────────────
    shift_str = "Day Shift" if datetime.now().hour < 20 else "Night Shift"
    st.markdown(f"""
    <div class="rpt-banner">
        <span>📊 IPSS DAILY PRODUCTION REPORT</span>
        <span class="rpt-date">📅 {today_date.strftime("%A, %d %B %Y")} &nbsp;|&nbsp; {shift_str}</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Alerts ────────────────────────────────────────────────────────────────
    if not df_sum.empty:
        alerts = []
        if dfr_today > limit:
            alerts.append(("red", f"🚨 DF Rate hôm nay <b>{dfr_today:.2f}%</b> — vượt giới hạn {limit}%! Cần action ngay."))
        elif dfr_today > limit * 0.7:
            alerts.append(("amber", f"⚠️ DF Rate hôm nay <b>{dfr_today:.2f}%</b> — đang tiếp cận giới hạn {limit}%."))
        else:
            alerts.append(("green", f"✅ DF Rate hôm nay <b>{dfr_today:.2f}%</b> — trong giới hạn kiểm soát."))
        if unreleased > 0:
            alerts.append(("amber", f"⚠️ <b>{unreleased} lot</b> PR Rework chưa Release — cần theo dõi."))
        if pr_rw_today == 0 and today_row:
            alerts.append(("green", "✅ Hôm nay không có PR Rework."))

        alert_html = "".join(f'<div class="alert-{lvl}">{msg}</div>' for lvl, msg in alerts)
        st.markdown(alert_html, unsafe_allow_html=True)

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_dash, tab_quality, tab_production, tab_wip, tab_lot, tab_cfg, tab_email = st.tabs([
        "🏠 Dashboard", "⚠️ Quality (DF/PR)", "📈 Production", "📦 WIP",
        "🔍 Lot Detail", "⚙️ Settings", "📧 Email"
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1: DASHBOARD — Executive Overview
    # ══════════════════════════════════════════════════════════════════════════
    with tab_dash:
        if df_sum.empty:
            st.info("📂 Chưa có dữ liệu. Nhấn **▶️ Xử lý & Xuất báo cáo** ở sidebar.")
        else:
            # ── KPI Cards ─────────────────────────────────────────────────────
            st.markdown('<div class="sec-hdr">📌 KPI HÔM NAY</div>', unsafe_allow_html=True)

            def _kpi_html(icon, label, key, unit, color, fmt="int"):
                v = float(today_row.get(key, 0) or 0)
                y = float(yest_row.get(key, 0)  or 0)
                dv = f"{v:.2f}" if fmt=="float" else f"{int(v):,}"
                dy = f"{int(y):,}"
                try:
                    d = v - y; p = d/y*100 if y != 0 else 0
                    a = "▲" if d >= 0 else "▼"
                    cls = "kpi-delta-up" if d >= 0 else "kpi-delta-dn"
                    delta_html = f'<div class="{cls}">{a} {abs(d):.1f} ({p:+.1f}%)</div>'
                except:
                    delta_html = ""
                return f"""
                <div class="kpi-card" style="--kc:{color}">
                  <div class="kpi-icon">{icon}</div>
                  <div class="kpi-lbl">{label}</div>
                  <div class="kpi-val">{dv}</div>
                  <div class="kpi-unit">{unit}</div>
                  {delta_html}
                  <div class="kpi-vs">Hôm qua: {dy}</div>
                </div>"""

            kpi_defs = [
                ("📥","Input IPSS",  "Input_IPSS",   "wafers", C_BLUE,   "int"),
                ("🚢","EPI Shipment","EPI_Shipment",  "wafers", C_GREEN,  "int"),
                ("🔄","Total Move",  "Total_Move",    "wafers", "#E65100","int"),
                ("📦","Total WIP",   "Total_WIP",     "wafers", C_PURPLE, "int"),
                ("📸","E3157 Move",  "E3157_Move",    "wafers", C_TEAL,   "int"),
                ("🔧","PR RW Qty",   "PR_RW_Qty",     "wafers", C_LRED,   "int"),
                ("📊","DF Rate",     "DF_Rate",       "%",      C_RED if dfr_today>limit else C_AMBER if dfr_today>limit*0.7 else C_GREEN, "float"),
            ]
            cols = st.columns(7)
            for i, (icon, lbl, key, unit, color, fmt) in enumerate(kpi_defs):
                cols[i].markdown(
                    f'<div class="kpi-wrap">{_kpi_html(icon,lbl,key,unit,color,fmt)}</div>',
                    unsafe_allow_html=True)

            st.markdown("")

            # ── View selector ─────────────────────────────────────────────────
            vc, _ = st.columns([2, 10])
            view = vc.radio("", ["Daily","Weekly","Monthly"],
                            horizontal=True, key="dash_view", label_visibility="collapsed")

            # ── Row 1: Input & DF Rate ────────────────────────────────────────
            c1, c2 = st.columns(2)
            with c1:
                st.plotly_chart(chart_input(df_sum, view, wsd, days),
                                use_container_width=True, key="d_input")
            with c2:
                st.plotly_chart(chart_dfrate(df_sum, view, wsd, days, limit),
                                use_container_width=True, key="d_dfrate")

            # ── Row 2: WIP Snapshot & Movement Summary ────────────────────────
            c3, c4 = st.columns(2)
            with c3:
                if today_row:
                    st.plotly_chart(chart_wip_snapshot(today_row),
                                    use_container_width=True, key="d_wip")
                else:
                    st.info("Chưa có WIP data hôm nay")
            with c4:
                st.plotly_chart(chart_movement(df_sum, view, wsd, days),
                                use_container_width=True, key="d_move")

            # ── Summary Table ─────────────────────────────────────────────────
            with st.expander("📋 Bảng tổng hợp SUMMARY (30 ngày gần nhất)"):
                sc = ["Date","Input_IPSS","Input_PSS","EPI_Shipment",
                      "Total_WIP","Total_Move","E3157_Move","PR_RW_Qty","DF_Rate"]
                sc = [c for c in sc if c in df_sum.columns]
                disp = df_sum[sc].tail(30).sort_values("Date", ascending=False).copy()
                disp["Date"] = disp["Date"].dt.strftime("%Y-%m-%d")
                # Color DF_Rate column
                def _style_df(val):
                    if isinstance(val, float):
                        if val > limit: return "background-color:#FEE2E2;color:#991B1B"
                        if val > limit*0.7: return "background-color:#FEF3C7;color:#92400E"
                    return ""
                st.dataframe(
                    disp.style.applymap(_style_df, subset=["DF_Rate"])
                    .format({"DF_Rate":"{:.2f}%","Total_WIP":"{:,.0f}",
                             "Total_Move":"{:,.0f}","E3157_Move":"{:,.0f}",
                             "PR_RW_Qty":"{:,.0f}","Input_IPSS":"{:,.0f}"}),
                    use_container_width=True, height=320
                )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2: QUALITY — DF Rate & PR Rework Deep Dive
    # ══════════════════════════════════════════════════════════════════════════
    with tab_quality:
        if df_sum.empty:
            st.info("Chưa có dữ liệu")
        else:
            # Metrics row
            m1, m2, m3, m4 = st.columns(4)
            pr_cols_avail = [f"PR_{c}" for c in PR_COLS if f"PR_{c}" in df_sum.columns]
            df_30 = _prep(df_sum, 30)

            avg_dfr_30 = df_30["DF_Rate"].mean() if "DF_Rate" in df_30.columns else 0
            days_over  = int((df_30["DF_Rate"] > limit).sum()) if "DF_Rate" in df_30.columns else 0
            total_pr   = int(df_30[pr_cols_avail].sum().sum()) if pr_cols_avail else 0
            top_code   = ""
            if pr_cols_avail:
                tc = df_sum[pr_cols_avail].sum().idxmax()
                top_code = f"{tc.replace('PR_','')} – {PR_DESC.get(tc.replace('PR_',''),'')[:20]}"

            with m1:
                badge = "badge-red" if avg_dfr_30>limit else "badge-amber" if avg_dfr_30>limit*0.7 else "badge-green"
                st.markdown(f"""**DF Rate TB (30d)**
<span class="{badge}">{avg_dfr_30:.2f}%</span>""", unsafe_allow_html=True)
            with m2:
                badge = "badge-red" if days_over>3 else "badge-amber" if days_over>0 else "badge-green"
                st.markdown(f"""**Ngày vượt limit (30d)**
<span class="{badge}">{days_over} ngày</span>""", unsafe_allow_html=True)
            with m3:
                st.markdown(f"**Tổng PR RW (30d)**  \n`{total_pr:,} wafers`")
            with m4:
                st.markdown(f"**Top PR Code**  \n`{top_code}`")

            st.markdown("")

            # ── Control Chart + Daily Bar ─────────────────────────────────────
            st.markdown('<div class="sec-hdr">📉 DF RATE CONTROL CHART</div>', unsafe_allow_html=True)
            vc2, _ = st.columns([2, 10])
            view_q = vc2.radio("", ["Daily","Weekly","Monthly"],
                               horizontal=True, key="q_view", label_visibility="collapsed")
            st.plotly_chart(chart_dfrate(df_sum, view_q, wsd, days, limit),
                            use_container_width=True, key="q_dfrate")

            c1, c2 = st.columns([2, 1])
            with c1:
                st.plotly_chart(chart_e3157_prrw(df_sum, days),
                                use_container_width=True, key="q_e3157")
            with c2:
                st.plotly_chart(chart_dfrate_bar(df_sum, days, limit),
                                use_container_width=True, key="q_dfbar")

            # ── Pareto ────────────────────────────────────────────────────────
            st.markdown('<div class="sec-hdr">📊 PARETO ANALYSIS — PR CODE</div>', unsafe_allow_html=True)
            pc1, pc2, pc3 = st.columns([1, 1, 1])
            period_sel = pc1.selectbox("Kỳ phân tích:", ["All","MTD","Last 30d"], key="pareto_period")

            st.plotly_chart(chart_pr_pareto(df_sum, period_sel),
                            use_container_width=True, key="q_pareto")

            # ── PR Code Trend ─────────────────────────────────────────────────
            st.markdown('<div class="sec-hdr">📈 TOP PR CODES — TREND</div>', unsafe_allow_html=True)
            st.plotly_chart(chart_pr_trend(df_sum, days, top_n=6),
                            use_container_width=True, key="q_prtend")

            # ── PR Code Table ─────────────────────────────────────────────────
            with st.expander("📋 Bảng chi tiết PR Code theo ngày (30 ngày gần nhất)"):
                if pr_cols_avail:
                    disp2 = df_30[["Date"] + pr_cols_avail].copy()
                    disp2["Date"] = disp2["Date"].dt.strftime("%Y-%m-%d")
                    disp2.columns = ["Date"] + [c.replace("PR_","") for c in pr_cols_avail]
                    disp2 = disp2.sort_values("Date", ascending=False)
                    st.dataframe(disp2, use_container_width=True, height=300)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3: PRODUCTION — Input, Movement, Throughput
    # ══════════════════════════════════════════════════════════════════════════
    with tab_production:
        if df_sum.empty:
            st.info("Chưa có dữ liệu")
        else:
            st.markdown('<div class="sec-hdr">📥 INPUT & EPI SHIPMENT</div>', unsafe_allow_html=True)
            vc3, _ = st.columns([2, 10])
            view_p = vc3.radio("", ["Daily","Weekly","Monthly"],
                               horizontal=True, key="prod_view", label_visibility="collapsed")

            st.plotly_chart(chart_input(df_sum, view_p, wsd, days),
                            use_container_width=True, key="p_input")

            st.markdown('<div class="sec-hdr">🔄 MOVEMENT BY STEP</div>', unsafe_allow_html=True)
            st.plotly_chart(chart_movement(df_sum, view_p, wsd, days),
                            use_container_width=True, key="p_move")

            # E3157 detailed
            st.markdown('<div class="sec-hdr">📸 E3157 DEVELOP — KEY STEP</div>', unsafe_allow_html=True)
            st.plotly_chart(chart_e3157_prrw(df_sum, days),
                            use_container_width=True, key="p_e3")

            # Step comparison table
            with st.expander("📋 Movement by Step — Raw Data (30 ngày)"):
                mc = [f"Move_{s}" for s in STEPS if f"Move_{s}" in df_sum.columns]
                df_mv = _prep(df_sum, days)[["Date"] + mc].copy()
                df_mv["Date"] = df_mv["Date"].dt.strftime("%Y-%m-%d")
                df_mv.columns = ["Date"] + [c.replace("Move_","") for c in mc]
                st.dataframe(df_mv.sort_values("Date", ascending=False),
                             use_container_width=True, height=320)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4: WIP — Inventory Tracking
    # ══════════════════════════════════════════════════════════════════════════
    with tab_wip:
        if df_sum.empty:
            st.info("Chưa có dữ liệu")
        else:
            st.markdown('<div class="sec-hdr">📦 WIP DISTRIBUTION — HIỆN TẠI</div>', unsafe_allow_html=True)
            if today_row:
                st.plotly_chart(chart_wip_snapshot(today_row),
                                use_container_width=True, key="w_snap")
            else:
                st.warning("Chưa có WIP snapshot hôm nay")

            st.markdown('<div class="sec-hdr">🗓️ WIP HEATMAP — 21 NGÀY GẦN NHẤT</div>', unsafe_allow_html=True)
            st.plotly_chart(chart_wip_heatmap(df_sum, n=21),
                            use_container_width=True, key="w_heat")

            st.markdown('<div class="sec-hdr">📈 WIP TREND THEO THỜI GIAN</div>', unsafe_allow_html=True)
            vc4, _ = st.columns([2, 10])
            view_w = vc4.radio("", ["Daily","Weekly","Monthly"],
                               horizontal=True, key="wip_view", label_visibility="collapsed")
            st.plotly_chart(chart_wip_trend(df_sum, view_w, wsd, days),
                            use_container_width=True, key="w_trend")

            with st.expander("📋 WIP by Step — Raw Data (14 ngày)"):
                wc = [f"WIP_{s}" for s in STEPS if f"WIP_{s}" in df_sum.columns]
                df_wip = _prep(df_sum, 14)[["Date"] + wc].copy()
                df_wip["Date"] = df_wip["Date"].dt.strftime("%Y-%m-%d")
                df_wip.columns = ["Date"] + [c.replace("WIP_","") for c in wc]
                st.dataframe(df_wip.sort_values("Date", ascending=False),
                             use_container_width=True, height=300)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 5: LOT DETAIL
    # ══════════════════════════════════════════════════════════════════════════
    with tab_lot:
        st.markdown('<div class="sec-hdr">🔍 PR RW LOT LIST — CHI TIẾT</div>', unsafe_allow_html=True)
        if df_lot.empty:
            st.info("Chưa có dữ liệu Lot")
        else:
            # Filters
            c1, c2, c3 = st.columns(3)
            pg_opts  = sorted(df_lot["Product Group"].dropna().unique().tolist()) if "Product Group" in df_lot.columns else []
            code_opts = sorted(df_lot["Hold Code"].dropna().unique().tolist()) if "Hold Code" in df_lot.columns else []
            pg_filter   = c1.multiselect("Product Group", pg_opts,
                                          default=["IPSS"] if "IPSS" in pg_opts else pg_opts)
            code_filter = c2.multiselect("Hold Code", code_opts, default=[])
            rel_filter  = c3.selectbox("Trạng thái", ["Tất cả","Đã release","Chưa release"])

            df_f = df_lot.copy()
            if pg_filter:   df_f = df_f[df_f["Product Group"].isin(pg_filter)]
            if code_filter: df_f = df_f[df_f["Hold Code"].isin(code_filter)]
            if rel_filter == "Đã release":    df_f = df_f[df_f["Release time"].notna()]
            elif rel_filter == "Chưa release": df_f = df_f[df_f["Release time"].isna()]

            # Metrics
            m1, m2, m3, m4 = st.columns(4)
            nr = int(df_f["Release time"].isna().sum()) if "Release time" in df_f.columns else 0
            tq = int(df_f["Qty"].sum()) if "Qty" in df_f.columns else 0
            m1.metric("📋 Tổng Lots", f"{len(df_f):,}")
            m2.metric("🟡 Chưa Release", f"{nr:,}")
            m3.metric("⚪ Đã Release", f"{len(df_f)-nr:,}")
            m4.metric("📦 Tổng Qty", f"{tq:,} wafers")

            # Trend by day
            if "DAY" in df_f.columns and not df_f.empty:
                cnt = df_f.groupby("DAY").agg(
                    Lots=("Lot","count"),
                    Qty=("Qty","sum")
                ).reset_index()
                fig_lot = make_subplots(specs=[[{"secondary_y": True}]])
                fig_lot.add_bar(x=cnt["DAY"], y=cnt["Lots"], name="Số Lots",
                                marker_color="#EF9A9A", marker_line_width=0,
                                secondary_y=False)
                fig_lot.add_scatter(x=cnt["DAY"], y=cnt["Qty"], name="Qty (wafers)",
                                    mode="lines+markers",
                                    line=dict(color=C_RED, width=2),
                                    marker=dict(size=6), secondary_y=True)
                fig_lot.update_layout(
                    title=dict(text="<b>PR RW LOTS & QTY THEO NGÀY</b>",
                               font=dict(size=13, color=C_NAVY)),
                    height=280, plot_bgcolor="white", paper_bgcolor="white",
                    margin=dict(t=50, b=40, l=55, r=25),
                    font=dict(family="Arial", size=10),
                    legend=dict(orientation="h", y=1.12),
                    hovermode="x unified",
                )
                fig_lot.update_yaxes(title_text="Số Lots", secondary_y=False, gridcolor="#F0F0F0")
                fig_lot.update_yaxes(title_text="Qty", secondary_y=True, showgrid=False)
                st.plotly_chart(fig_lot, use_container_width=True, key="lot_trend")

            # Table
            show_cols = [c for c in ["YEAR","WEEK","DAY","Product Group","Step",
                                     "Lot","Qty","Hold Code","Hold date and time",
                                     "Release time","Hold reason"] if c in df_f.columns]
            st.dataframe(
                df_f[show_cols].sort_values("DAY", ascending=False).reset_index(drop=True)
                if "DAY" in df_f.columns else df_f[show_cols],
                use_container_width=True, height=400
            )
            st.caption("🟡 Lot chưa release có nền vàng trong file Excel")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 6: SETTINGS
    # ══════════════════════════════════════════════════════════════════════════
    with tab_cfg:
        st.markdown('<div class="sec-hdr">⚙️ CẤU HÌNH HỆ THỐNG</div>', unsafe_allow_html=True)

        with st.expander("📁 Thư mục RAW Data", expanded=True):
            st.info("Nhập đường dẫn Thư mục. App tự tìm file theo ngày báo cáo.")
            rf = cfg.get("raw_folders", {}); rpat = cfg.get("raw_patterns", {})
            c1, c2 = st.columns([3,1])
            f_hold = c1.text_input("📌 HOLD HISTORY folder", value=rf.get("hold_history",""))
            p_hold = c2.text_input("Pattern", value=rpat.get("hold_history","*HOLD*"), key="p1")
            f_wip  = c1.text_input("📦 WIP folder",          value=rf.get("wip",""))
            p_wip  = c2.text_input("Pattern", value=rpat.get("wip","*WIP*"),  key="p2")
            f_move = c1.text_input("🔄 MOVEMENT folder",     value=rf.get("move",""))
            p_move = c2.text_input("Pattern", value=rpat.get("move","*MOVE*"), key="p3")
            f_inp  = c1.text_input("📥 INPUT folder",        value=rf.get("input",""))
            p_inp  = c2.text_input("Pattern", value=rpat.get("input","*INPUT*"),key="p4")

        with st.expander("📋 Template & Output"):
            p_tmpl  = st.text_input("File template V03", value=cfg.get("template_file",""))
            out_dir = st.text_input("Folder output",     value=cfg.get("output_folder","reports"))
            prefix  = st.text_input("Prefix tên file",   value=cfg.get("report_prefix","IPSS_DAILY_REPORT_V03"))

        with st.expander("📅 Tuần & Biểu đồ"):
            week_opts = {"Thứ 2":0,"Thứ 3":1,"Thứ 4":2,"Thứ 5":3,"Thứ 6":4,"Thứ 7":5,"Chủ nhật":6}
            cur_lbl   = cfg.get("week_start_label","Thứ 6")
            week_lbl  = st.selectbox("Ngày bắt đầu tuần:",list(week_opts.keys()),
                                     index=list(week_opts.keys()).index(cur_lbl) if cur_lbl in week_opts else 4)
            chart_days_n = st.number_input("Số ngày Daily chart:", 7, 90, value=cfg.get("chart_days",30))
            df_limit_n   = st.number_input("DF Rate limit (%):",    1, 100, value=cfg.get("df_rate_limit",15))

        if st.button("💾 Lưu Cấu Hình", type="primary", use_container_width=True):
            cfg["raw_folders"]  = {"hold_history":f_hold,"wip":f_wip,"move":f_move,"input":f_inp}
            cfg["raw_patterns"] = {"hold_history":p_hold,"wip":p_wip,"move":p_move,"input":p_inp}
            cfg["template_file"]    = p_tmpl
            cfg["output_folder"]    = out_dir
            cfg["report_prefix"]    = prefix
            cfg["week_start_day"]   = week_opts[week_lbl]
            cfg["week_start_label"] = week_lbl
            cfg["chart_days"]       = int(chart_days_n)
            cfg["df_rate_limit"]    = int(df_limit_n)
            save_cfg(cfg)
            st.success("✅ Đã lưu!")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 7: EMAIL
    # ══════════════════════════════════════════════════════════════════════════
    with tab_email:
        st.markdown('<div class="sec-hdr">📧 EMAIL & AUTO SCHEDULE</div>', unsafe_allow_html=True)
        ec = cfg.get("email", {})

        with st.expander("📬 Người nhận", expanded=True):
            c1, _ = st.columns(2)
            e_sender = c1.text_input("Tên người gửi", value=ec.get("sender_name","IPSS Report System"))
            e_to = st.text_area("To (phân cách ;)", value="; ".join(ec.get("to",[])), height=60)
            e_cc = st.text_area("CC",               value="; ".join(ec.get("cc",[])), height=50)

        with st.expander("✉️ Nội dung"):
            st.caption("Biến: `{date}` `{input_ipss}` `{shipment}` `{pr_rw_rate}` `{total_move}` `{total_wip}` `{sender_name}`")
            e_subj = st.text_input("Subject", value=ec.get("subject_template","[IPSS Daily Report] {date}"))
            e_body = st.text_area("Body",     value=ec.get("body_template",""), height=200)

        with st.expander("⏰ Lịch gửi"):
            sched_t   = st.text_input("Giờ gửi (HH:MM)", value=ec.get("schedule_time","08:00"))
            auto_send = st.toggle("🔄 Auto-send hàng ngày", value=ec.get("auto_send",False))

        c_save, c_prev, c_send = st.columns(3)
        if c_save.button("💾 Lưu Email Config"):
            cfg["email"] = {
                "sender_name":e_sender,
                "to":[x.strip() for x in e_to.split(";") if x.strip()],
                "cc":[x.strip() for x in e_cc.split(";") if x.strip()],
                "subject_template":e_subj, "body_template":e_body,
                "schedule_time":sched_t, "auto_send":auto_send,
            }
            save_cfg(cfg); st.success("✅ Đã lưu!")

        if c_prev.button("👁️ Preview"):
            kpi = dict(st.session_state.get("last_row",{}))
            kpi["date"] = today_dt.strftime("%d/%m/%Y")
            p = preview_email(cfg.get("email",{}), kpi)
            st.markdown(f"**To:** {p['to']}  |  **CC:** {p['cc']}")
            st.code(p["body"])

        if c_send.button("📤 Gửi ngay", type="primary"):
            path = st.session_state.get("last_path","") or find_latest_report(cfg)
            kpi  = dict(st.session_state.get("last_row",{}))
            kpi["date"] = today_dt.strftime("%d/%m/%Y")
            ok, msg = send_via_outlook(cfg.get("email",{}), path, kpi)
            if ok: st.success(msg)
            else:  st.error(msg)

        logs = get_log()
        if logs:
            st.markdown("**📋 Activity Log:**")
            for e in logs[:15]:
                st.markdown(f"`{e['time']}` — {e['msg']}")


if __name__ == "__main__":
    main()
