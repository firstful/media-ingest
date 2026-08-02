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
- JELLYFIN_URL / JELLYFIN_API_KEY — optional scan trigger

## Layout

```
movies/Title (Year)/Title (Year).mkv      + .srt + .json sidecar
tv/Show (Year)/Season 01/Show (Year) S01E01.mkv
```
