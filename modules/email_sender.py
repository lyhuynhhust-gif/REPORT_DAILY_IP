"""Email sender via Outlook COM automation (Windows)."""
import os
from datetime import datetime


def _format(template: str, kpi: dict, cfg: dict) -> str:
    try:
        return template.format(
            date=kpi.get("date", datetime.now().strftime("%d/%m/%Y")),
            input_ipss=kpi.get("Input_IPSS", 0),
            input_pss=kpi.get("Input_PSS", 0),
            shipment=kpi.get("EPI_Shipment", 0),
            pr_rw_rate=f"{kpi.get('PR_RW_Rate', 0):.2f}",
            pr_rw_qty=kpi.get("PR_RW_Qty", 0),
            total_move=kpi.get("Total_Move", 0),
            total_wip=kpi.get("Total_WIP", 0),
            develop_move=kpi.get("Develop_Move", 0),
            active_hold=kpi.get("Active_Hold", 0),
            sender_name=cfg.get("sender_name", "IPSS Report System"),
        )
    except KeyError as e:
        return template  # return raw if unknown placeholder


def send_via_outlook(config: dict, attachment_path: str, kpi_data: dict) -> tuple:
    try:
        import win32com.client as win32
        outlook = win32.Dispatch("outlook.application")
        mail = outlook.CreateItem(0)

        to_list = config.get("to", [])
        cc_list = config.get("cc", [])
        if isinstance(to_list, str):
            to_list = [x.strip() for x in to_list.split(";") if x.strip()]
        if isinstance(cc_list, str):
            cc_list = [x.strip() for x in cc_list.split(";") if x.strip()]

        mail.To = "; ".join(to_list)
        if cc_list:
            mail.CC = "; ".join(cc_list)

        mail.Subject = _format(config.get("subject_template", "[IPSS Daily Report] {date}"), kpi_data, config)
        mail.Body    = _format(config.get("body_template", "Báo cáo ngày {date}"), kpi_data, config)

        if attachment_path and os.path.exists(attachment_path):
            mail.Attachments.Add(os.path.abspath(attachment_path))

        mail.Send()
        return True, "✅ Email đã gửi thành công qua Outlook."
    except ImportError:
        return False, "❌ pywin32 chưa được cài đặt. Chạy: pip install pywin32"
    except Exception as e:
        return False, f"❌ Lỗi gửi email: {str(e)}"


def preview_email(config: dict, kpi_data: dict) -> dict:
    return {
        "to":      config.get("to", []),
        "cc":      config.get("cc", []),
        "subject": _format(config.get("subject_template", "[IPSS Daily Report] {date}"), kpi_data, config),
        "body":    _format(config.get("body_template", ""), kpi_data, config),
    }
