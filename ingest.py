"""media-ingest — watch cs-downloader downloads, transcribe, publish to Jellyfin.

Watches /downloads for finished media files (matched by their .json sidecar,
written by cs-downloader's worker). For each:
  1. settle-wait until the file is stable (NFS-safe)
  2. transcribe audio -> <video>.srt via faster-whisper (CPU, model env-tunable)
  3. compute the Jellyfin library target from the sidecar metadata
     (movie -> /jellyfin/movies/Title (Year)/ ; tv -> /jellyfin/tv/Show/Season NN/Show SxxEyy)
  4. move video + .srt + sidecar into the target (atomic-ish, .ingested marker)
  5. best-effort Jellyfin library scan (env JELLYFIN_URL/JELLYFIN_API_KEY; skipped if unset)
"""
from __future__ import annotations

import json
import os
import re
import shutil
import sys
import time
import urllib.request

import yaml  # noqa: F401  (keep deps minimal — not used yet)

WATCH_DIR = os.environ.get("INGEST_WATCH_DIR", "/downloads")
JELLYFIN_ROOT = os.environ.get("INGEST_JELLYFIN_ROOT", "/jellyfin")
MODEL = os.environ.get("INGEST_WHISPER_MODEL", "small.en")
DEVICE = os.environ.get("INGEST_WHISPER_DEVICE", "cpu")
POLL_SECONDS = int(os.environ.get("INGEST_POLL_SECONDS", "30"))
SETTLE_SECONDS = float(os.environ.get("INGEST_SETTLE_SECONDS", "6"))
JELLYFIN_URL = os.environ.get("JELLYFIN_URL", "")
JELLYFIN_API_KEY = os.environ.get("JELLYFIN_API_KEY", "")
# Coalescing floor between /Library/Refresh triggers — a busy publish queue
# must not cancel Jellyfin's in-flight scan on every sweep (scan cancel-loop).
MIN_REFRESH_INTERVAL = float(os.environ.get("INGEST_REFRESH_MIN_INTERVAL", "120"))

_last_refresh_at = 0.0  # time.monotonic() of the last successful trigger
_pending_refresh = False  # a publish wants a scan but one is running/too recent

VIDEO_EXT = {".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v", ".ts"}
WHISPER = None  # lazy import

# Belt-and-suspenders kids routing: even if the cs-downloader sidecar says
# kid_friendly=false (web UI checkbox default-off), a known children's show
# still lands in the Kids library. Substring match on the show title.
KIDS_TITLES = (
    "peppa pig", "paw patrol", "bluey", "spongebob", "daniel tiger",
    "arthur", "wild kratts", "dora the explorer", "sesame street",
    "cocomelon", "mickey mouse", "pj masks", "octonauts",
    "blaze and the monster machines", "doc mcstuffins", "bubble guppies",
    "curious george", "thomas & friends", "my little pony", "teletubbies",
    "blue's clues", "super wings", "pocoyo", "masha and the bear",
    "winnie the pooh", "little einsteins", "handy manny", "mickey mouse clubhouse",
    "peppa", "strawberry shortcake", "care bears", "the wiggles", "barney",
    "pinkfong", "bebefinn", "morphle", "tayo",
)


def is_kids_title(title: str) -> bool:
    t = (title or "").lower()
    return any(k in t for k in KIDS_TITLES)


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}", flush=True)


def stable(path: str, seconds: float) -> bool:
    s1 = os.path.getsize(path)
    time.sleep(min(seconds, 3))
    try:
        s2 = os.path.getsize(path)
    except OSError:
        return False
    return s1 == s2


def transcribe(video: str, srt: str) -> bool:
    global WHISPER
    try:
        if WHISPER is None:
            from faster_whisper import WhisperModel
            WHISPER = WhisperModel(MODEL, device=DEVICE, compute_type="int8")
        log(f"transcribing {os.path.basename(video)} ({MODEL})")
        t0 = time.time()
        segments, info = WHISPER.transcribe(video, language="en", vad_filter=True)
        lines = []
        for seg in segments:
            lines.append(f"{_ts(seg.start)} --> {_ts(seg.end)}\n{seg.text.strip()}\n")
        if not lines:
            log("transcribe produced no segments")
            return False
        with open(srt, "w", encoding="utf-8") as f:
            for i, block in enumerate(lines, 1):
                f.write(f"{i}\n{block}\n")
        log(f"wrote {os.path.basename(srt)} ({len(lines)} cues) in {time.time()-t0:.0f}s")
        return True
    except Exception as e:  # noqa: BLE001
        log(f"transcribe failed: {type(e).__name__}: {e}")
        return False


def _ts(sec: float) -> str:
    h = int(sec // 3600); m = int(sec % 3600 // 60); s = int(sec % 60)
    ms = int(round((sec - int(sec)) * 1000))
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def parse_year(title: str) -> int | None:
    m = re.search(r"\((\d{4})\)", title)
    return int(m.group(1)) if m else None


def target_for(sidecar: dict, base_name: str, ext: str) -> tuple[str, str]:
    """Return (dir, filename) under JELLYFIN_ROOT following Jellyfin conventions."""
    title = (sidecar.get("title") or base_name).strip()
    year = parse_year(title)
    clean_title = re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip()
    mtype = sidecar.get("media_type") or "movie"
    kid = bool(sidecar.get("kid_friendly")) or is_kids_title(clean_title)
    # /mnt/Media per-library layout (Reed 2026-08-05): INGEST_JELLYFIN_ROOT is
    # the Media root; libraries are per-folder, not per-type.
    movie_root = "Kids_Movies" if kid else "Adult_Movies"
    tv_root = "Kids_TV" if kid else "Adult_TV"

    if mtype == "tv":
        season = sidecar.get("season") or 1
        ep = sidecar.get("episode")
        # The sidecar title (from cs-downloader) often carries the episode
        # suffix, e.g. "Peppa Pig (2004) S01E01". Strip the (year) and any
        # trailing SxxEyy so we build ONE show dir, not a dir per episode.
        show_raw = re.sub(r"\s*\(\d{4}\)\s*", " ", clean_title)
        show_raw = re.sub(r"\s*S\d{2}E\d{2,}\s*$", "", show_raw, flags=re.I)
        show = re.sub(r"[^\w .'-]+", " ", show_raw)
        show = re.sub(r"\s{2,}", " ", show).strip()
        show_dir = f"{show} ({year})" if year else show
        season_dir = f"Season {int(season):02d}"
        if ep is not None:
            fname = f"{show} S{int(season):02d}E{int(ep):02d}{ext}"
        else:
            fname = f"{show}{ext}"
        base_dir = os.path.join(tv_root, show_dir, season_dir)
        return (base_dir, fname)
    # movie
    name = f"{clean_title} ({year})" if year else clean_title
    return (os.path.join(movie_root, name), f"{name}{ext}")


def scan_state() -> str | None:
    """State of Jellyfin's 'Scan Media Library' task (Idle/Running/Cancelling)."""
    if not (JELLYFIN_URL and JELLYFIN_API_KEY):
        return None
    try:
        req = urllib.request.Request(
            f"{JELLYFIN_URL}/ScheduledTasks",
            headers={"X-Emby-Token": JELLYFIN_API_KEY},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            tasks = json.load(r)
        for t in tasks or []:
            if t.get("Name") == "Scan Media Library":
                return t.get("State")
    except Exception as e:  # noqa: BLE001
        log(f"jellyfin task query skipped: {type(e).__name__}: {e}")
    return None


def scan_jellyfin() -> None:
    """Coalesced, scan-aware /Library/Refresh trigger.

    Every POST /Library/Refresh CANCELS the running scan task, so a busy
    publish queue that triggers per sweep keeps scans at ~0% forever (the
    cancel-loop, with EF Core SQLite connection errors on jellyfin.db as a
    side effect). Rules:
      - never fire more often than MIN_REFRESH_INTERVAL;
      - never fire while Scan Media Library is Running/Cancelling — defer.
    Deferred triggers are flushed by main() once the scan is Idle, so new
    publishes still get indexed with at most one interval of delay.
    """
    global _last_refresh_at, _pending_refresh
    if not (JELLYFIN_URL and JELLYFIN_API_KEY):
        return
    now = time.monotonic()
    if now - _last_refresh_at < MIN_REFRESH_INTERVAL:
        log("jellyfin scan deferred (min interval)")
        _pending_refresh = True
        return
    state = scan_state()
    if state in ("Running", "Cancelling"):
        log(f"jellyfin scan deferred (task {state})")
        _pending_refresh = True
        return
    try:
        req = urllib.request.Request(
            f"{JELLYFIN_URL}/Library/Refresh",
            headers={"X-Emby-Token": JELLYFIN_API_KEY},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            _last_refresh_at = now
            _pending_refresh = False
            log(f"jellyfin scan triggered: {r.status}")
    except Exception as e:  # noqa: BLE001
        log(f"jellyfin scan skipped: {type(e).__name__}: {e}")


def process(video: str) -> None:
    sidecar_path = video + ".json"
    try:
        with open(sidecar_path) as f:
            sidecar = json.load(f)
    except Exception:
        sidecar = {}
    log(f"claiming {os.path.basename(video)} (media_type={sidecar.get('media_type','?')})")

    srt = video + ".srt"
    if not os.path.exists(srt):
        if not transcribe(video, srt):
            log(f"transcribe failed for {os.path.basename(video)} — will retry next sweep")
            return  # leave the file in place for a retry

    ext = os.path.splitext(video)[1]
    base = os.path.splitext(os.path.basename(video))[0]
    rel_dir, fname = target_for(sidecar, base, ext)
    dest_dir = os.path.join(JELLYFIN_ROOT, rel_dir)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, fname)
    # avoid clobbering an existing file — keep both, suffix the new one
    if os.path.exists(dest):
        dest = os.path.join(dest_dir, f"{os.path.splitext(fname)[0]} (2){ext}")
    shutil.move(video, dest)
    if os.path.exists(srt):
        shutil.move(srt, dest + ".srt")
    if os.path.exists(sidecar_path):
        shutil.move(sidecar_path, dest + ".json")
    log(f"published -> {rel_dir}/{os.path.basename(dest)}")


def sweep() -> None:
    processed = 0
    try:
        entries = sorted(os.listdir(WATCH_DIR))
    except OSError as e:
        log(f"watch dir error: {e}")
        return
    for name in entries:
        full = os.path.join(WATCH_DIR, name)
        if not os.path.isfile(full):
            continue
        ext = os.path.splitext(name)[1].lower()
        if ext not in VIDEO_EXT:
            continue
        if name.endswith(".staging") or ".processing" in name:
            continue
        marker = full + ".json"
        if not os.path.exists(marker):
            continue  # not finished (no sidecar yet)
        if not stable(full, SETTLE_SECONDS):
            continue
        try:
            process(full)
            processed += 1
        except Exception as e:  # noqa: BLE001
            log(f"process failed {name}: {type(e).__name__}: {e}")
    # NFS realtime monitor is a no-op — trigger a scan after any publish.
    if processed:
        log(f"sweep published {processed} item(s), triggering Jellyfin scan")
        scan_jellyfin()


def main() -> None:
    os.makedirs(WATCH_DIR, exist_ok=True)
    os.makedirs(JELLYFIN_ROOT, exist_ok=True)
    log(f"media-ingest watching {WATCH_DIR} -> {JELLYFIN_ROOT} (model={MODEL})")
    while True:
        try:
            sweep()
            # Flush a deferred scan once the running scan is Idle and the
            # min-interval floor has elapsed (scan_jellyfin decides).
            if _pending_refresh:
                scan_jellyfin()
        except Exception as e:  # noqa: BLE001
            log(f"sweep error: {type(e).__name__}: {e}")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
