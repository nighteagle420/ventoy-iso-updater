"""
Distro Registry & Version Checker
Add new distros by calling register() with a DistroInfo entry.
Currently supports: Arch Linux
"""

import re
import json
import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Strategy & Distro Definition
# ─────────────────────────────────────────────────────────────

class CheckStrategy(Enum):
    DIRECTORY_LISTING = "directory_listing"
    JSON_API = "json_api"
    GITHUB_RELEASE = "github_release"


@dataclass
class DistroInfo:
    id: str
    name: str
    filename_regex: str          # group(1) must capture the version
    strategy: CheckStrategy
    check_url: str               # URL to scrape/query for latest version
    download_url_template: str   # {version} gets replaced
    checksum_url_template: str = ""
    # What to save the file as on the Ventoy drive ({version} replaced).
    # If empty, the filename is extracted from the download URL.
    save_as_template: str = ""
    # Filename to look up in checksum file (if different from save_as).
    # Needed when download URL uses a generic name like "archlinux-x86_64.iso"
    # but we save as the dated name. If empty, uses the download URL filename.
    checksum_lookup_name: str = ""
    listing_version_regex: str = ""  # for DIRECTORY_LISTING strategy
    json_version_path: str = ""      # for JSON_API strategy
    github_repo: str = ""            # for GITHUB_RELEASE strategy
    github_asset_regex: str = ""
    # Max parallel connections for download. 0 = use global default (8).
    # Set to 1 for mirrors that don't support Range requests (e.g. SourceForge).
    connections: int = 0


# ─────────────────────────────────────────────────────────────
# Registry
# ─────────────────────────────────────────────────────────────

DISTRO_REGISTRY: dict[str, DistroInfo] = {}

def register(distro: DistroInfo):
    DISTRO_REGISTRY[distro.id] = distro


# ── Arch Linux ───────────────────────────────────────────────
# https://fastly.mirror.pkgbuild.com/iso/latest/ — Fastly CDN, fast globally.
# The generic "archlinux-x86_64.iso" is a redirect to the latest.
# We save it with the dated name for version tracking on the drive.
register(DistroInfo(
    id="archlinux",
    name="Arch Linux",
    filename_regex=r"archlinux-(\d{4}\.\d{2}\.\d{2})-x86_64\.iso",
    strategy=CheckStrategy.DIRECTORY_LISTING,
    check_url="https://fastly.mirror.pkgbuild.com/iso/latest/",
    download_url_template="https://fastly.mirror.pkgbuild.com/iso/latest/archlinux-x86_64.iso",
    checksum_url_template="https://fastly.mirror.pkgbuild.com/iso/latest/sha256sums.txt",
    save_as_template="archlinux-{version}-x86_64.iso",
    checksum_lookup_name="archlinux-x86_64.iso",
    listing_version_regex=r'href="archlinux-(\d{4}\.\d{2}\.\d{2})-x86_64\.iso"',
))


# ── CachyOS Desktop ─────────────────────────────────────────
# Version check: SourceForge directory listing (YYMMDD folders).
# Download: CachyOS CDN (supports Range → aria2c multi-connection works).
# Mirrors: iso.cachyos.org, cdn77.cachyos.org, us.cachyos.org
register(DistroInfo(
    id="cachyos-desktop",
    name="CachyOS Desktop",
    filename_regex=r"cachyos-desktop-linux-(\d{6})(?:_\d+)?\.iso",
    strategy=CheckStrategy.DIRECTORY_LISTING,
    check_url="https://sourceforge.net/projects/cachyos-arch/files/gui-installer/desktop/",
    download_url_template="https://iso.cachyos.org/desktop/{version}/cachyos-desktop-linux-{version}.iso",
    checksum_url_template="https://sourceforge.net/projects/cachyos-arch/files/gui-installer/desktop/{version}/cachyos-desktop-linux-{version}.iso.sha256/download",
    save_as_template="cachyos-desktop-linux-{version}.iso",
    listing_version_regex=r'href="/projects/cachyos-arch/files/gui-installer/desktop/(\d{6})/"',
))


# ── GParted Live ────────────────────────────────────────────
# SourceForge: /gparted-live-stable/ has versioned folders (X.Y.Z-N).
# SHA256 is embedded in the README.md — our parser extracts it line by line.
# User may have _N suffix ISOs (e.g. gparted-live-1.7.0-12-amd64_2.iso).
register(DistroInfo(
    id="gparted-live",
    name="GParted Live",
    filename_regex=r"gparted-live-(\d+\.\d+\.\d+-\d+)-amd64(?:_\d+)?\.iso",
    strategy=CheckStrategy.DIRECTORY_LISTING,
    check_url="https://sourceforge.net/projects/gparted/files/gparted-live-stable/",
    download_url_template="https://sourceforge.net/projects/gparted/files/gparted-live-stable/{version}/gparted-live-{version}-amd64.iso/download",
    checksum_url_template="https://sourceforge.net/projects/gparted/files/gparted-live-stable/{version}/gparted-live-{version}-README.md/download",
    save_as_template="gparted-live-{version}-amd64.iso",
    listing_version_regex=r'href="/projects/gparted/files/gparted-live-stable/(\d+\.\d+\.\d+-\d+)/"',
))


# ─────────────────────────────────────────────────────────────
# ISO Filename Matcher
# ─────────────────────────────────────────────────────────────

@dataclass
class MatchedISO:
    distro: DistroInfo
    current_version: str
    filename: str
    file_path: Path
    size_bytes: int
    latest_version: Optional[str] = None
    download_url: Optional[str] = None
    needs_update: Optional[bool] = None


def match_iso_to_distro(filename: str, file_path: Path, size_bytes: int) -> Optional[MatchedISO]:
    for distro_id, distro in DISTRO_REGISTRY.items():
        match = re.match(distro.filename_regex, filename, re.IGNORECASE)
        if match:
            return MatchedISO(
                distro=distro, current_version=match.group(1),
                filename=filename, file_path=file_path, size_bytes=size_bytes,
            )
    return None


def match_all_isos(iso_files: list) -> tuple[list[MatchedISO], list]:
    matched, unmatched = [], []
    for iso in iso_files:
        result = match_iso_to_distro(iso.name, iso.path, iso.size_bytes)
        (matched if result else unmatched).append(result or iso)
    return matched, unmatched


# ─────────────────────────────────────────────────────────────
# HTTP Helpers
# ─────────────────────────────────────────────────────────────

USER_AGENT = "VentoyUpdater/1.0"
REQUEST_TIMEOUT = 30

def fetch_url(url: str) -> Optional[str]:
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except (URLError, HTTPError, OSError) as e:
        log.warning(f"Failed to fetch {url}: {e}")
        return None

def fetch_json(url: str) -> Optional[dict | list]:
    body = fetch_url(url)
    if body is None:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError as e:
        log.warning(f"Invalid JSON from {url}: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# Version Comparison
# ─────────────────────────────────────────────────────────────

def parse_version_tuple(version_str: str) -> tuple:
    parts = re.split(r'[.\-]', version_str)
    result = []
    for part in parts:
        num_match = re.match(r'^(\d+)(.*)', part)
        if num_match:
            result.append(int(num_match.group(1)))
            if num_match.group(2):
                result.append(num_match.group(2))
        else:
            result.append(part)
    return tuple(result)

def is_newer(latest: str, current: str) -> bool:
    return parse_version_tuple(latest) > parse_version_tuple(current)


# ─────────────────────────────────────────────────────────────
# Version Checking Strategies
# ─────────────────────────────────────────────────────────────

def check_directory_listing(distro: DistroInfo) -> Optional[str]:
    html = fetch_url(distro.check_url)
    if html is None:
        return None
    versions = re.findall(distro.listing_version_regex, html)
    if not versions:
        log.warning(f"No versions found at {distro.check_url}")
        return None
    unique_versions = sorted(set(versions), key=parse_version_tuple)
    latest = unique_versions[-1]
    log.info(f"{distro.name}: latest = {latest}")
    return latest


def _resolve_json_path(data, path: str):
    for key in path.split('.'):
        if isinstance(data, list):
            data = data[int(key)]
        elif isinstance(data, dict):
            data = data[key]
        else:
            return None
    return str(data)


def check_json_api(distro: DistroInfo) -> Optional[str]:
    data = fetch_json(distro.check_url)
    if data is None:
        return None
    if distro.json_version_path:
        try:
            version = _resolve_json_path(data, distro.json_version_path)
            log.info(f"{distro.name}: JSON API version = {version}")
            return version
        except (KeyError, IndexError, TypeError) as e:
            log.warning(f"Failed to extract version: {e}")
    return None


def check_github_release(distro: DistroInfo) -> Optional[str]:
    data = fetch_json(distro.check_url)
    if data is None:
        return None
    tag = data.get("tag_name", "")
    version = re.sub(r'^(v|release[-_]?)', '', tag)
    log.info(f"{distro.name}: GitHub tag = {tag}, version = {version}")
    if distro.github_asset_regex:
        for asset in data.get("assets", []):
            if re.search(distro.github_asset_regex, asset.get("name", "")):
                distro.download_url_template = asset.get("browser_download_url", "")
                break
    return version


STRATEGY_HANDLERS = {
    CheckStrategy.DIRECTORY_LISTING: check_directory_listing,
    CheckStrategy.JSON_API: check_json_api,
    CheckStrategy.GITHUB_RELEASE: check_github_release,
}

def check_latest_version(distro: DistroInfo) -> Optional[str]:
    handler = STRATEGY_HANDLERS.get(distro.strategy)
    if handler is None:
        log.error(f"No handler for strategy: {distro.strategy}")
        return None
    return handler(distro)


# ─────────────────────────────────────────────────────────────
# Check All Matched ISOs
# ─────────────────────────────────────────────────────────────

def check_updates(matched_isos: list[MatchedISO], dry_run: bool = True) -> list[MatchedISO]:
    print("\n" + "=" * 60)
    print("  CHECKING FOR UPDATES")
    print("=" * 60 + "\n")

    for iso in matched_isos:
        print(f"  Checking {iso.distro.name}...", end=" ", flush=True)
        latest = check_latest_version(iso.distro)
        if latest is None:
            print("⚠ could not determine latest version")
            iso.needs_update = None
            continue
        iso.latest_version = latest
        if is_newer(latest, iso.current_version):
            iso.needs_update = True
            print(f"⬆ UPDATE: {iso.current_version} → {latest}")
            if "{version}" in iso.distro.download_url_template:
                iso.download_url = iso.distro.download_url_template.format(version=latest)
        else:
            iso.needs_update = False
            print(f"✓ up to date ({iso.current_version})")

    updates = [i for i in matched_isos if i.needs_update]
    up_to_date = [i for i in matched_isos if i.needs_update is False]
    failed = [i for i in matched_isos if i.needs_update is None]
    print(f"\n{'─' * 60}")
    print(f"  {len(up_to_date)} up to date, {len(updates)} update(s), {len(failed)} failed")
    if updates and dry_run:
        print("\n  Run with --update to download. Add --yes to skip prompts.\n")
    return matched_isos


# ─────────────────────────────────────────────────────────────
# Display Helpers
# ─────────────────────────────────────────────────────────────

def display_matches(matched: list[MatchedISO], unmatched: list) -> None:
    print("\n" + "=" * 60)
    print("  ISO IDENTIFICATION RESULTS")
    print("=" * 60)
    if matched:
        print(f"\n  ✓ Recognized ({len(matched)}):\n")
        max_name = max(len(m.distro.name) for m in matched)
        for m in matched:
            print(f"    {m.distro.name:<{max_name}}  v{m.current_version}")
            print(f"    {'':>{max_name}}  └─ {m.filename}")
    else:
        print("\n  No ISOs matched known distros.")
    if unmatched:
        print(f"\n  ? Unrecognized ({len(unmatched)}):\n")
        for u in unmatched:
            print(f"    • {u.name}")
        print(f"\n    Add entries in distro_checker.py using register()")
    print()


def list_supported_distros():
    print("\n  Supported Distros:\n")
    for did, d in sorted(DISTRO_REGISTRY.items()):
        print(f"    {d.name:<30} [{d.strategy.value}]")
        print(f"    {'':>30}  regex: {d.filename_regex}")
    print()