from __future__ import annotations

import csv
import shutil
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request, send_from_directory

from google_shopping_crawler import (
    CrawlCancelled,
    FINAL_COLUMNS,
    Logger,
    crawl_google_shopping_reviews,
    now_utc_iso,
    save_reviews_csv,
)

APP_DIR = Path(__file__).resolve().parent
RUNS_DIR = APP_DIR / "runs"
RUNS_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_NAME = "structured_reviews.csv"
PREVIEW_ROW_LIMIT = 15

app = Flask(__name__, static_folder="static", static_url_path="/static")


def load_preview_rows(output_dir: Path) -> List[Dict[str, Any]]:
    csv_path = output_dir / DOWNLOAD_NAME
    if not csv_path.exists():
        return []
    try:
        rows: List[Dict[str, Any]] = []
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                if idx >= PREVIEW_ROW_LIMIT:
                    break
                rows.append({k: ("" if v is None else v) for k, v in row.items()})
        return rows
    except Exception:
        return []


@dataclass
class JobState:
    job_id: str
    status: str = "queued"
    created_at_utc: str = field(default_factory=now_utc_iso)
    started_at_utc: Optional[str] = None
    finished_at_utc: Optional[str] = None
    message: str = "대기 중"
    percent: float = 0.0
    output_dir: str = ""
    brand: str = ""
    product_name: str = ""
    start_date: str = ""
    end_date: str = ""
    row_count: int = 0
    logs: List[str] = field(default_factory=list)
    download_url: Optional[str] = None
    preview_rows: List[Dict[str, Any]] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    pause_event: threading.Event = field(default_factory=threading.Event, repr=False)
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)
    delete_requested: bool = False

    def __post_init__(self):
        self.pause_event.set()

    def append_log(self, message: str):
        with self.lock:
            ts = datetime.now().strftime("%H:%M:%S")
            self.logs.append(f"[{ts}] {message}")
            self.logs = self.logs[-500:]

    def set_status(self, status: str, message: Optional[str] = None):
        with self.lock:
            self.status = status
            if message:
                self.message = message
            if status == "running" and not self.started_at_utc:
                self.started_at_utc = now_utc_iso()
            if status in {"completed", "failed", "cancelled", "deleted"}:
                self.finished_at_utc = now_utc_iso()

    def request_pause(self):
        self.pause_event.clear()
        self.set_status("paused", "작업이 일시 중단되었습니다.")
        self.append_log("사용자 요청으로 일시 중단되었습니다.")

    def request_resume(self):
        self.pause_event.set()
        self.set_status("running", "작업이 재개되었습니다.")
        self.append_log("사용자 요청으로 작업을 재개합니다.")

    def request_verification(self, message: Optional[str] = None):
        final_message = message or "Google verification이 필요합니다. 브라우저에서 확인을 완료한 뒤 Resume을 누르세요."
        self.pause_event.clear()
        self.set_status("verification_required", final_message)
        self.append_log(final_message)

    def request_cancel(self, delete_requested: bool = False):
        self.delete_requested = bool(delete_requested)
        self.cancel_event.set()
        self.pause_event.set()
        self.set_status("cancelling", "작업 중단 요청을 처리 중입니다.")
        self.append_log("사용자 요청으로 작업 중단을 시작합니다.")

    def checkpoint(self):
        if self.cancel_event.is_set():
            raise CrawlCancelled("사용자가 작업을 중단했습니다.")
        while not self.pause_event.is_set():
            if self.status != "verification_required":
                self.set_status("paused", "작업이 일시 중단되었습니다.")
            if self.cancel_event.wait(0.25):
                raise CrawlCancelled("사용자가 작업을 중단했습니다.")
        with self.lock:
            if self.status in {"paused", "verification_required"}:
                self.status = "running"
                self.message = "작업이 재개되었습니다."

    def refresh_preview(self):
        if not self.output_dir:
            return
        output_dir = Path(self.output_dir)
        self.preview_rows = load_preview_rows(output_dir)
        csv_path = output_dir / DOWNLOAD_NAME
        if csv_path.exists():
            try:
                with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f)
                    self.row_count = sum(1 for _ in reader)
            except Exception:
                pass

    def to_dict(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "job_id": self.job_id,
                "status": self.status,
                "created_at_utc": self.created_at_utc,
                "started_at_utc": self.started_at_utc,
                "finished_at_utc": self.finished_at_utc,
                "message": self.message,
                "percent": self.percent,
                "output_dir": self.output_dir,
                "brand": self.brand,
                "product_name": self.product_name,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "row_count": self.row_count,
                "logs": list(self.logs),
                "download_url": self.download_url,
                "preview_rows": list(self.preview_rows),
                "summary": dict(self.summary),
                "error": self.error,
                "file_name": DOWNLOAD_NAME,
            }


class JobLogger(Logger):
    def __init__(self, path: Path, job: JobState):
        super().__init__(path)
        self.job = job

    def log(self, message: str):
        super().log(message)
        self.job.append_log(message)


JOBS: Dict[str, JobState] = {}
JOBS_LOCK = threading.Lock()


def safe_remove_output_dir(path_str: str):
    if not path_str:
        return
    try:
        shutil.rmtree(path_str, ignore_errors=True)
    except Exception:
        pass


def refresh_partial_output(job: JobState, output_dir: Path, rows: List[Dict[str, Any]]):
    save_reviews_csv(output_dir, rows)
    job.download_url = f"/api/download_csv/{job.job_id}"
    job.refresh_preview()
    job.summary = {
        "total_reviews_collected": len(rows),
        "structured_columns": FINAL_COLUMNS,
    }


def crawl_worker(job: JobState, payload: Dict[str, Any]):
    output_dir = RUNS_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{job.job_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    job.output_dir = str(output_dir)
    job.download_url = f"/api/download_csv/{job.job_id}"
    logger = JobLogger(output_dir / "crawl_log.txt", job)

    brand = str(payload.get("brand", "") or "").strip()
    product_name = str(payload.get("product_name", "") or "").strip()
    start_date = str(payload.get("start_date", "") or "").strip()
    end_date = str(payload.get("end_date", "") or "").strip()
    headful = bool(payload.get("headful", True))

    job.brand = brand
    job.product_name = product_name
    job.start_date = start_date
    job.end_date = end_date
    job.summary = {
        "total_reviews_collected": 0,
        "structured_columns": FINAL_COLUMNS,
    }
    job.set_status("running", "Google Shopping 페이지를 여는 중입니다.")
    job.percent = 10.0
    refresh_partial_output(job, output_dir, [])

    def partial_flush(rows: List[Dict[str, Any]]):
        job.percent = min(95.0, 35.0 + len(rows) * 0.2)
        job.message = f"리뷰 수집 중... 현재 {len(rows)}건"
        refresh_partial_output(job, output_dir, rows)

    try:
        rows = crawl_google_shopping_reviews(
            brand=brand,
            product_name=product_name,
            start_date_text=start_date,
            end_date_text=end_date,
            headless=not headful,
            output_dir=output_dir,
            logger=logger,
            control_hook=job.checkpoint,
            verification_hook=job.request_verification,
            partial_flush=partial_flush,
        )
        refresh_partial_output(job, output_dir, rows)
        job.percent = 100.0
        job.summary = {
            "total_reviews_collected": len(rows),
            "structured_columns": FINAL_COLUMNS,
        }
        job.set_status("completed", "명령에 의한 리뷰추출이 끝났다고 판단되어 크롤링을 종료했습니다.")
    except CrawlCancelled:
        job.refresh_preview()
        if job.delete_requested:
            safe_remove_output_dir(str(output_dir))
            job.download_url = None
            job.preview_rows = []
            job.summary = {}
            job.row_count = 0
            job.set_status("deleted", "작업과 진행 파일을 삭제했습니다.")
        else:
            job.set_status("cancelled", "작업이 중단되었습니다.")
    except Exception as e:
        job.error = str(e)
        job.append_log(f"오류: {e}")
        job.refresh_preview()
        job.set_status("failed", "작업이 중단되었습니다.")


@app.route("/")
def root():
    return send_from_directory(app.static_folder, "index.html")


@app.post("/api/start")
def api_start():
    payload = request.get_json(force=True, silent=True) or {}
    job_id = uuid.uuid4().hex[:10]
    job = JobState(job_id=job_id)
    with JOBS_LOCK:
        JOBS[job_id] = job

    def runner():
        crawl_worker(job, payload)

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    return jsonify({"job_id": job_id})


@app.get("/api/status/<job_id>")
def api_status(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "job_not_found"}), 404
    return jsonify(job.to_dict())


@app.get("/api/jobs")
def api_jobs():
    with JOBS_LOCK:
        items = [job.to_dict() for job in JOBS.values() if job.status != "deleted"]
    items.sort(key=lambda x: x.get("created_at_utc") or "", reverse=True)
    return jsonify({"jobs": items})


@app.post("/api/pause/<job_id>")
def api_pause(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "job_not_found"}), 404
    if job.status in {"completed", "failed", "cancelled", "deleted"}:
        return jsonify({"ok": False, "message": "이미 종료된 작업입니다."}), 400
    job.request_pause()
    return jsonify({"ok": True})


@app.post("/api/resume/<job_id>")
def api_resume(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "job_not_found"}), 404
    if job.status in {"completed", "failed", "cancelled", "deleted"}:
        return jsonify({"ok": False, "message": "이미 종료된 작업입니다."}), 400
    job.request_resume()
    return jsonify({"ok": True})


@app.post("/api/cancel/<job_id>")
def api_cancel(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "job_not_found"}), 404
    if job.status in {"completed", "failed", "cancelled", "deleted"}:
        return jsonify({"ok": False, "message": "이미 종료된 작업입니다."}), 400
    delete_requested = bool((request.get_json(silent=True) or {}).get("delete_requested", False))
    job.request_cancel(delete_requested=delete_requested)
    return jsonify({"ok": True})


@app.get("/api/download_csv/<job_id>")
def api_download_csv(job_id: str):
    job = JOBS.get(job_id)
    if not job or not job.output_dir:
        return jsonify({"error": "job_not_found"}), 404
    output_dir = Path(job.output_dir)
    csv_path = output_dir / DOWNLOAD_NAME
    if not csv_path.exists():
        return jsonify({"error": "csv_not_found"}), 404
    return send_from_directory(output_dir, DOWNLOAD_NAME, as_attachment=True, download_name=DOWNLOAD_NAME)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8765, debug=False)
