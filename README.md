# media-ingest

Watch cs-downloader downloads → whisper transcribe → .srt → move into the Jellyfin
library (Jellyfin naming) → best-effort library scan.

## Flow

1. cs-downloader worker finishes a download and writes `<file>.json` sidecar
   (media_type, title, season, episode, quality, provider).
2. This pod (ns cs-downloader) polls /downloads for video files with a sidecar,
   waits for the file to be stable, transcribes with faster-whisper (small.en,
   CPU) into `<file>.srt`, then moves video + .srt + sidecar into
   /jellyfin/{movies,tv} following Jellyfin conventions.
3. If JELLYFIN_URL + JELLYFIN_API_KEY are set, triggers a library refresh.

## Env

- INGEST_WATCH_DIR (default /downloads) — cs-downloader downloads PVC
- INGEST_JELLYFIN_ROOT (default /jellyfin) — static NFS PV of /mnt/raid_store/jellyfin
- INGEST_WHISPER_MODEL (default small.en), INGEST_WHISPER_DEVICE (cpu)
- INGEST_POLL_SECONDS, INGEST_SETTLE_SECONDS
- INGEST_TRANSCRIBE_TIMEOUT (default 3600) — kill a wedged transcribe subprocess
- INGEST_RETRY_BACKOFF (default 600) / INGEST_RETRY_BACKOFF_MAX (7200) —
  exponential per-file retry floor for failed transcribes (no more re-transcribe
  every sweep; a poisoned file backs off to 2h max)
- INGEST_REFRESH_MIN_INTERVAL (default 120) — floor between Jellyfin /Library/Refresh triggers; never cancels a running scan (cancel-loop fix)
- JELLYFIN_URL / JELLYFIN_API_KEY — optional scan trigger

Each transcribe runs in a **fresh subprocess** (`ingest.py --transcribe <video> <srt>`):
faster-whisper keeps model + decoded audio resident, so an in-process transcribe
peaks ~4Gi and the baseline never shrinks — two movies in one process blew the
6Gi pod limit and OOM-killed the container (68 restarts/35h, fixed in v1.8).
Subprocess-per-file reclaims memory on exit and a child crash only fails that
one file (retried after backoff), never the ingest loop.

## Layout

```
movies/Title (Year)/Title (Year).mkv      + .srt + .json sidecar
tv/tv_new/Show (Year)/Season 01/Show (Year) S01E01.mkv
tv/kids/Show (Year)/Season 01/...          (kid_friendly TV)
kids_movies/Title (Year)/Title (Year).mkv  (kid_friendly movies)
```

All publish targets are WATCHED Jellyfin library roots (Shows watches
`/mnt/new` + `/mnt/tv/tv_new` + `/mnt/tv/adults/tv_formatted`; Kids watches
`/mnt/tv/kids`; Kids Movies watches the sibling `/mnt/kids_movies`).
Do NOT publish to `/mnt/tv` root or `/mnt/movies/kids` — those are inside
other watched roots and either go invisible (Shows root is intentionally
unwatched) or land in the wrong library.
