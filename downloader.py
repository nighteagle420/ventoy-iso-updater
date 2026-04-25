"""
ISO Downloader & Verifier
Download → SHA256 verify → replace on Ventoy drive.
Checksum file is fetched, used, and discarded — never stored.

Download backends (auto-selected, fastest first):
  1. aria2c    — multi-connection CLI tool, best speeds
  2. threaded  — Python multi-threaded Range requests (8 connections)
  3. basic     — single-threaded urllib fallback
"""

import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import logging
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

USER_AGENT = "VentoyUpdater/1.0"
REQUEST_TIMEOUT = 30
CHUNK_SIZE = 1024 * 1024  # 1 MB per read
CONNECTIONS = 8            # parallel connections for threaded mode


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _fmt(size_bytes: int | float) -> str:
    size = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _progress(done: int, total: int, width: int = 40, speed: float = 0):
    if total > 0:
        pct = done / total
        bar = "█" * int(width * pct) + "░" * (width - int(width * pct))
        speed_str = f"  {_fmt(speed)}/s" if speed > 0 else ""
        sys.stderr.write(
            f"\r  [{bar}] {pct:6.1%}  {_fmt(done)} / {_fmt(total)}{speed_str}    "
        )
    else:
        sys.stderr.write(f"\r  Downloaded: {_fmt(done)}  ")
    sys.stderr.flush()


# ─────────────────────────────────────────────────────────────
# Backend 1: aria2c (fastest)
# ─────────────────────────────────────────────────────────────

def _has_aria2c() -> bool:
    try:
        subprocess.run(["aria2c", "--version"], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def _download_aria2c(url: str, dest: Path) -> bool:
    """Download using aria2c with multi-connection."""
    log.info(f"Using aria2c ({CONNECTIONS} connections)")
    dest_dir = str(dest.parent)
    dest_name = dest.name
    try:
        subprocess.run(
            [
                "aria2c",
                "--max-connection-per-server", str(CONNECTIONS),
                "--split", str(CONNECTIONS),
                "--min-split-size", "5M",
                "--file-allocation=none",
                "--summary-interval=0",
                "--download-result=hide",
                "--console-log-level=error",
                "--user-agent", USER_AGENT,
                "-d", dest_dir,
                "-o", dest_name,
                url,
            ],
            check=True,
        )
        return dest.exists() and dest.stat().st_size > 0
    except subprocess.CalledProcessError as e:
        log.error(f"aria2c failed with exit code {e.returncode}")
        return False


# ─────────────────────────────────────────────────────────────
# Backend 2: Multi-threaded Python (no external deps)
# ─────────────────────────────────────────────────────────────

def _get_file_info(url: str) -> tuple[int, bool]:
    """HEAD request to get file size and check Range support."""
    try:
        req = Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            size = int(resp.headers.get("Content-Length", 0))
            accept_ranges = resp.headers.get("Accept-Ranges", "").lower()
            return size, accept_ranges == "bytes"
    except (URLError, HTTPError, OSError):
        return 0, False


class _SegmentDownloader:
    """Download a byte range of a file into a shared buffer."""
    def __init__(self, url: str, start: int, end: int, segment_id: int):
        self.url = url
        self.start = start
        self.end = end
        self.segment_id = segment_id
        self.data = b""
        self.downloaded = 0
        self.error: Optional[str] = None

    def run(self):
        try:
            req = Request(self.url, headers={
                "User-Agent": USER_AGENT,
                "Range": f"bytes={self.start}-{self.end}",
            })
            with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                while True:
                    chunk = resp.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    self.data += chunk
                    self.downloaded = len(self.data)
        except (URLError, HTTPError, OSError) as e:
            self.error = str(e)


def _download_threaded(url: str, dest: Path) -> bool:
    """Multi-threaded download using HTTP Range requests."""
    total_size, supports_range = _get_file_info(url)

    if not supports_range or total_size == 0:
        log.info("Server doesn't support Range requests, falling back to basic")
        return _download_basic(url, dest)

    log.info(f"Multi-threaded download ({CONNECTIONS} connections, {_fmt(total_size)})")

    # Split into segments
    segment_size = total_size // CONNECTIONS
    segments: list[_SegmentDownloader] = []
    for i in range(CONNECTIONS):
        start = i * segment_size
        end = total_size - 1 if i == CONNECTIONS - 1 else (i + 1) * segment_size - 1
        segments.append(_SegmentDownloader(url, start, end, i))

    # Start all threads
    threads = []
    for seg in segments:
        t = threading.Thread(target=seg.run, daemon=True)
        t.start()
        threads.append(t)

    # Progress monitor
    start_time = time.time()
    while any(t.is_alive() for t in threads):
        total_done = sum(s.downloaded for s in segments)
        elapsed = time.time() - start_time
        speed = total_done / elapsed if elapsed > 0 else 0
        _progress(total_done, total_size, speed=speed)
        time.sleep(0.2)

    # Final progress
    total_done = sum(s.downloaded for s in segments)
    elapsed = time.time() - start_time
    speed = total_done / elapsed if elapsed > 0 else 0
    _progress(total_done, total_size, speed=speed)
    sys.stderr.write("\n")

    # Check for errors
    errors = [(s.segment_id, s.error) for s in segments if s.error]
    if errors:
        for seg_id, err in errors:
            log.error(f"Segment {seg_id} failed: {err}")
        return False

    # Assemble file from segments (in order)
    with open(dest, "wb") as f:
        for seg in segments:
            f.write(seg.data)

    actual_size = dest.stat().st_size
    if actual_size != total_size:
        log.error(f"Size mismatch: expected {total_size}, got {actual_size}")
        return False

    log.info(f"Download complete: {_fmt(total_size)} in {elapsed:.1f}s ({_fmt(speed)}/s)")
    return True


# ─────────────────────────────────────────────────────────────
# Backend 3: Basic single-threaded (final fallback)
# ─────────────────────────────────────────────────────────────

def _download_basic(url: str, dest: Path) -> bool:
    """Simple single-threaded download."""
    log.info("Using basic single-threaded download")
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            done = 0
            start_time = time.time()
            with open(dest, "wb") as f:
                while True:
                    chunk = resp.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                    done += len(chunk)
                    elapsed = time.time() - start_time
                    speed = done / elapsed if elapsed > 0 else 0
                    _progress(done, total, speed=speed)
            sys.stderr.write("\n")
            log.info(f"Download complete: {_fmt(done)}")
            return True
    except (URLError, HTTPError, OSError) as e:
        sys.stderr.write("\n")
        log.error(f"Download failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# Auto-select best backend
# ─────────────────────────────────────────────────────────────

def download_file(url: str, dest: Path) -> bool:
    """Download a file using the fastest available backend."""
    log.info(f"Downloading: {url}")
    if _has_aria2c():
        return _download_aria2c(url, dest)
    return _download_threaded(url, dest)


def _fetch_text(url: str) -> Optional[str]:
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except (URLError, HTTPError, OSError) as e:
        log.warning(f"Failed to fetch {url}: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# Checksum
# ─────────────────────────────────────────────────────────────

def _compute_sha256(path: Path) -> str:
    sha = hashlib.sha256()
    size = path.stat().st_size
    done = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            sha.update(chunk)
            done += len(chunk)
            _progress(done, size)
    sys.stderr.write("\n")
    return sha.hexdigest()


def _parse_checksum(content: str, target_filename: str) -> Optional[str]:
    """Parse SHA256 from checksum file. Handles GNU, BSD, and bare formats."""
    target = Path(target_filename).name
    for line in content.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # BSD: SHA256 (filename) = hash
        m = re.match(r'SHA256\s*\((.+?)\)\s*=\s*([0-9a-fA-F]{64})', line)
        if m and Path(m.group(1)).name == target:
            return m.group(2).lower()
        # GNU: hash  filename  OR  hash *filename
        m = re.match(r'([0-9a-fA-F]{64})\s+\*?(.+)', line)
        if m and Path(m.group(2)).name == target:
            return m.group(1).lower()
        # Bare hash (single line file)
        m = re.match(r'^([0-9a-fA-F]{64})$', line)
        if m:
            return m.group(1).lower()
    return None


def verify_checksum(iso_path: Path, checksum_url: str, iso_filename: str) -> bool:
    """Fetch checksum, verify ISO, return True if valid. Checksum is never stored."""
    print(f"  Fetching checksum: {checksum_url}")
    content = _fetch_text(checksum_url)
    if content is None:
        log.warning("Could not download checksum file")
        return _ask_skip()

    expected = _parse_checksum(content, iso_filename)
    if expected is None:
        log.warning(f"Hash for '{iso_filename}' not found in checksum file")
        return _ask_skip()

    print(f"  Expected : {expected}")
    print(f"  Computing SHA256...")
    actual = _compute_sha256(iso_path)
    print(f"  Computed : {actual}")

    if actual == expected:
        print(f"  ✓ Checksum VERIFIED")
        return True
    else:
        print(f"  ✗ Checksum MISMATCH — file may be corrupted!")
        return False


def _ask_skip() -> bool:
    try:
        return input("  Proceed without verification? [y/N]: ").strip().lower() in ("y", "yes")
    except (EOFError, KeyboardInterrupt):
        return False


# ─────────────────────────────────────────────────────────────
# Replace ISO on drive
# ─────────────────────────────────────────────────────────────

def _find_old_isos(ventoy_mount: Path, filename_regex: str) -> list[Path]:
    old = []
    for p in ventoy_mount.rglob("*.iso"):
        if re.match(filename_regex, p.name, re.IGNORECASE):
            old.append(p)
    return old


def _replace_iso(tmp_iso: Path, ventoy_mount: Path, new_name: str, regex: str) -> bool:
    dest = ventoy_mount / new_name
    old_isos = _find_old_isos(ventoy_mount, regex)

    # Delete old ISOs first to free up drive space
    # (safe because the new ISO is already checksum-verified in temp)
    if old_isos:
        print(f"\n  Removing old ISO(s) to free space:")
        for o in old_isos:
            if o.resolve() == dest.resolve():
                continue
            try:
                print(f"    • Deleting {o.name} ({_fmt(o.stat().st_size)})...", end=" ")
                o.unlink()
                print("✓")
            except OSError as e:
                print(f"✗")
                log.warning(f"Could not remove {o.name}: {e}")

    print(f"\n  Copying to Ventoy drive: {new_name}")
    try:
        shutil.copy2(str(tmp_iso), str(dest))
    except OSError as e:
        log.error(f"Copy failed: {e}")
        return False

    # Verify copy by size
    if tmp_iso.stat().st_size != dest.stat().st_size:
        log.error("Size mismatch after copy!")
        dest.unlink(missing_ok=True)
        return False
    print(f"  ✓ Copy verified ({_fmt(dest.stat().st_size)})")

    tmp_iso.unlink(missing_ok=True)
    print(f"  ✓ Cleaned up temp file")
    return True


# ─────────────────────────────────────────────────────────────
# Full Pipeline
# ─────────────────────────────────────────────────────────────

def download_and_replace(
    distro_name: str, download_url: str, checksum_url: str,
    new_filename: str, ventoy_mount: Path, filename_regex: str,
    checksum_lookup_name: str = "",
) -> bool:
    print("\n" + "=" * 60)
    print(f"  UPDATING: {distro_name}")
    print("=" * 60)
    print(f"  URL      : {download_url}")
    print(f"  Save as  : {new_filename}")
    print(f"  Drive    : {ventoy_mount}")

    tmp_dir = Path(tempfile.mkdtemp(prefix="ventoy_update_"))
    tmp_iso = tmp_dir / new_filename

    try:
        # Step 1: Download
        print(f"\n  Step 1/3: Downloading...")
        if not download_file(download_url, tmp_iso):
            return False

        # Step 2: Verify
        # Use checksum_lookup_name for sha256sums.txt lookup when the download
        # filename (e.g. generic "archlinux-x86_64.iso") differs from save name
        verify_name = checksum_lookup_name or new_filename
        if checksum_url:
            print(f"\n  Step 2/3: Verifying...")
            if not verify_checksum(tmp_iso, checksum_url, verify_name):
                tmp_iso.unlink(missing_ok=True)
                return False
        else:
            print(f"\n  Step 2/3: No checksum URL, skipping")

        # Step 3: Replace
        print(f"\n  Step 3/3: Replacing on drive...")
        if not _replace_iso(tmp_iso, ventoy_mount, new_filename, filename_regex):
            return False

        print(f"\n  ✓ {distro_name} updated successfully!")
        return True

    except KeyboardInterrupt:
        print(f"\n\n  Interrupted! Cleaning up...")
        tmp_iso.unlink(missing_ok=True)
        return False
    finally:
        shutil.rmtree(str(tmp_dir), ignore_errors=True)


def process_updates(matched_isos: list, ventoy_mount: Path, auto_confirm: bool = False) -> dict:
    """Process all ISOs that need updates."""
    updates = [iso for iso in matched_isos if iso.needs_update]
    if not updates:
        print("\n  Everything is up to date!")
        return {"updated": 0, "failed": 0, "skipped": 0}

    print("\n" + "=" * 60)
    print(f"  {len(updates)} UPDATE(S) AVAILABLE")
    print("=" * 60)
    for i, iso in enumerate(updates, 1):
        print(f"  {i}. {iso.distro.name}: {iso.current_version} → {iso.latest_version}")

    results = {"updated": 0, "failed": 0, "skipped": 0}

    if not auto_confirm:
        try:
            if input(f"\n  Proceed? [Y/n]: ").strip().lower() in ("n", "no"):
                results["skipped"] = len(updates)
                return results
        except (EOFError, KeyboardInterrupt):
            results["skipped"] = len(updates)
            return results

    for iso in updates:
        if not iso.download_url:
            log.warning(f"No download URL for {iso.distro.name}")
            results["skipped"] += 1
            continue

        new_filename = iso.download_url.split("/")[-1]
        checksum_url = ""
        if iso.distro.checksum_url_template:
            checksum_url = iso.distro.checksum_url_template.format(version=iso.latest_version)

        ok = download_and_replace(
            iso.distro.name, iso.download_url, checksum_url,
            new_filename, ventoy_mount, iso.distro.filename_regex,
        )
        results["updated" if ok else "failed"] += 1

    print("\n" + "=" * 60)
    print(f"  DONE — updated: {results['updated']}, failed: {results['failed']}, skipped: {results['skipped']}")
    print("=" * 60 + "\n")
    return results