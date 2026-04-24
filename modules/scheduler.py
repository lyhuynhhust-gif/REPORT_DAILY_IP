"""
Simple scheduler — runs in background thread.
On Windows, use Task Scheduler or the built-in schedule loop below.
"""
import schedule
import time
import threading
import json
import os
from datetime import datetime


_scheduler_thread = None
_stop_event = threading.Event()
_job_log = []


def _run_scheduler():
    while not _stop_event.is_set():
        schedule.run_pending()
        time.sleep(30)


def start_scheduler(job_func, schedule_time: str = "08:00"):
    """Start background scheduler thread."""
    global _scheduler_thread, _stop_event
    schedule.clear()
    _stop_event.clear()
    schedule.every().day.at(schedule_time).do(job_func)
    _scheduler_thread = threading.Thread(target=_run_scheduler, daemon=True)
    _scheduler_thread.start()
    return True


def stop_scheduler():
    global _stop_event
    _stop_event.set()
    schedule.clear()


def is_running() -> bool:
    return _scheduler_thread is not None and _scheduler_thread.is_alive()


def log_job(message: str):
    _job_log.append({"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "msg": message})
    if len(_job_log) > 100:
        _job_log.pop(0)


def get_log() -> list:
    return list(reversed(_job_log))
