#!/usr/bin/env python3
"""
Ventoy ISO Updater — Interactive Menu
Detect drive → menu → check/download/update ISOs.
"""

import shutil
import sys
from pathlib import Path

from ventoy_detect import detect_ventoy_drives, VentoyDrive, check_ventoy, find_iso_files
from distro_checker import (
    DISTRO_REGISTRY, match_all_isos, check_latest_version, is_newer,
    MatchedISO, display_matches,
)
from downloader import download_and_replace


# ─────────────────────────────────────────────────────────────
# Drive Selection
# ─────────────────────────────────────────────────────────────

def pick_drive(drives: list[VentoyDrive]) -> VentoyDrive:
    if not drives:
        print("\n[!] No Ventoy drives detected.")
        print("    Plug in your Ventoy USB and try again.")
        manual = input("\n  Enter mount path manually (or q to quit): ").strip()
        if manual.lower() == "q":
            sys.exit(0)
        path = Path(manual)
        if not path.exists():
            print(f"[!] Path does not exist: {manual}")
            sys.exit(1)
        version = check_ventoy(path)
        if not version:
            print(f"[!] No Ventoy installation at {manual}")
            sys.exit(1)
        usage = shutil.disk_usage(str(path))
        drive = VentoyDrive(
            mount_point=path, device="manual", filesystem="unknown",
            total_size=usage.total, free_space=usage.free, ventoy_version=version,
        )
        drive.iso_files = find_iso_files(path)
        return drive

    if len(drives) == 1:
        return drives[0]

    print("\n  Multiple Ventoy drives found:\n")
    for i, d in enumerate(drives, 1):
        print(f"    {i}. {d.device} → {d.mount_point} ({len(d.iso_files)} ISOs)")
    while True:
        try:
            choice = int(input("\n  Select drive [1]: ") or "1")
            if 1 <= choice <= len(drives):
                return drives[choice - 1]
        except (ValueError, EOFError):
            pass


# ─────────────────────────────────────────────────────────────
# Find existing ISO for a distro on the drive
# ─────────────────────────────────────────────────────────────

def find_matched_iso(distro_id: str, drive: VentoyDrive) -> MatchedISO | None:
    """Check if a distro's ISO already exists on the drive."""
    matched, _ = match_all_isos(drive.iso_files)
    for m in matched:
        if m.distro.id == distro_id:
            return m
    return None


# ─────────────────────────────────────────────────────────────
# Handle a single distro
# ─────────────────────────────────────────────────────────────

def handle_distro(distro_id: str, drive: VentoyDrive):
    distro = DISTRO_REGISTRY[distro_id]
    existing = find_matched_iso(distro_id, drive)

    if existing:
        print(f"\n  Found on drive: {existing.filename}")
        print(f"  Current version: {existing.current_version}")
    else:
        print(f"\n  {distro.name} is NOT on your drive.")

    # Check latest version
    print(f"  Checking for latest version...", end=" ", flush=True)
    latest = check_latest_version(distro)

    if latest is None:
        print("⚠ could not determine latest version")
        return

    print(f"→ {latest}")

    # Build download URL and filename
    download_url = distro.download_url_template.format(version=latest)
    # save_as_template defines the filename on the Ventoy drive (dated for tracking)
    # Falls back to extracting from download URL if not set
    if distro.save_as_template:
        new_filename = distro.save_as_template.format(version=latest)
    else:
        new_filename = download_url.split("/")[-1]
    checksum_url = distro.checksum_url_template.format(version=latest) if distro.checksum_url_template else ""
    # For checksum lookup: use checksum_lookup_name if the download filename
    # differs from what's in sha256sums.txt (e.g. generic vs dated name)
    checksum_name = distro.checksum_lookup_name or new_filename

    if existing:
        # ISO exists — check if update needed
        if is_newer(latest, existing.current_version):
            print(f"\n  ⬆ Update available: {existing.current_version} → {latest}")
            confirm = input("  Download update? [Y/n]: ").strip().lower()
            if confirm in ("n", "no"):
                print("  Skipped.")
                return
        else:
            print(f"  ✓ Already up to date!")
            return
    else:
        # ISO not on drive — offer fresh download
        print(f"\n  Latest: {new_filename}")
        confirm = input("  Download to Ventoy drive? [Y/n]: ").strip().lower()
        if confirm in ("n", "no"):
            print("  Skipped.")
            return

    # Download → verify → place on drive
    download_and_replace(
        distro_name=distro.name,
        download_url=download_url,
        checksum_url=checksum_url,
        new_filename=new_filename,
        ventoy_mount=drive.mount_point,
        filename_regex=distro.filename_regex,
        checksum_lookup_name=checksum_name,
    )

    # Refresh drive's ISO list after changes
    drive.iso_files = find_iso_files(drive.mount_point)


# ─────────────────────────────────────────────────────────────
# Update all recognized ISOs on the drive
# ─────────────────────────────────────────────────────────────

def handle_update_all(drive: VentoyDrive):
    matched, _ = match_all_isos(drive.iso_files)

    if not matched:
        print("\n  No recognized ISOs on the drive to update.")
        return

    print(f"\n  Checking {len(matched)} recognized ISO(s)...\n")

    updates_available = []
    for m in matched:
        print(f"  {m.distro.name} (v{m.current_version})...", end=" ", flush=True)
        latest = check_latest_version(m.distro)
        if latest is None:
            print("⚠ check failed")
            continue
        if is_newer(latest, m.current_version):
            print(f"⬆ {latest}")
            m.latest_version = latest
            m.download_url = m.distro.download_url_template.format(version=latest)
            updates_available.append(m)
        else:
            print(f"✓ up to date")

    if not updates_available:
        print("\n  Everything is up to date!")
        return

    print(f"\n  {len(updates_available)} update(s) available:")
    for m in updates_available:
        print(f"    • {m.distro.name}: {m.current_version} → {m.latest_version}")

    confirm = input(f"\n  Download all updates? [Y/n]: ").strip().lower()
    if confirm in ("n", "no"):
        print("  Skipped.")
        return

    for m in updates_available:
        checksum_url = m.distro.checksum_url_template.format(version=m.latest_version) if m.distro.checksum_url_template else ""
        if m.distro.save_as_template:
            new_filename = m.distro.save_as_template.format(version=m.latest_version)
        else:
            new_filename = m.download_url.split("/")[-1]
        checksum_name = m.distro.checksum_lookup_name or new_filename
        download_and_replace(
            distro_name=m.distro.name,
            download_url=m.download_url,
            checksum_url=checksum_url,
            new_filename=new_filename,
            ventoy_mount=drive.mount_point,
            filename_regex=m.distro.filename_regex,
            checksum_lookup_name=checksum_name,
        )

    drive.iso_files = find_iso_files(drive.mount_point)
    print("\n  All done!")


# ─────────────────────────────────────────────────────────────
# Show drive status
# ─────────────────────────────────────────────────────────────

def show_drive_status(drive: VentoyDrive):
    matched, unmatched = match_all_isos(drive.iso_files)

    print(f"\n{'─' * 50}")
    print(f"  Drive: {drive.mount_point}  ({drive.free_space_human} free)")
    print(f"{'─' * 50}")

    if matched:
        print(f"\n  Tracked ISOs:")
        for m in matched:
            print(f"    ✓ {m.distro.name:<20} v{m.current_version}")
    if unmatched:
        print(f"\n  Other ISOs:")
        for u in unmatched:
            print(f"    · {u.name}")

    # Show which registered distros are missing
    matched_ids = {m.distro.id for m in matched}
    missing = [d for did, d in DISTRO_REGISTRY.items() if did not in matched_ids]
    if missing:
        print(f"\n  Not on drive:")
        for d in missing:
            print(f"    ✗ {d.name}")
    print()


# ─────────────────────────────────────────────────────────────
# Interactive Menu
# ─────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 50)
    print("  VENTOY ISO UPDATER")
    print("=" * 50)

    # Detect drive
    drives = detect_ventoy_drives()
    drive = pick_drive(drives)
    print(f"\n  ✓ Using: {drive.device} → {drive.mount_point}")
    print(f"    Ventoy: {drive.ventoy_version}  |  {len(drive.iso_files)} ISO(s)")

    # Main loop
    while True:
        distros = list(DISTRO_REGISTRY.items())

        print(f"\n{'─' * 50}")
        print("  What would you like to do?\n")

        # Numbered distro options
        for i, (did, distro) in enumerate(distros, 1):
            existing = find_matched_iso(did, drive)
            if existing:
                status = f"v{existing.current_version} on drive"
            else:
                status = "not on drive"
            print(f"    {i}) {distro.name:<20} [{status}]")

        # Extra options
        print()
        print(f"    u) Update all ISOs on drive")
        print(f"    s) Show drive status")
        print(f"    q) Quit")

        try:
            choice = input(f"\n  Select [1-{len(distros)}/u/s/q]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Bye!")
            break

        if choice == "q":
            print("  Bye!")
            break
        elif choice == "u":
            handle_update_all(drive)
        elif choice == "s":
            show_drive_status(drive)
        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(distros):
                handle_distro(distros[idx][0], drive)
            else:
                print("  Invalid choice.")
        else:
            print("  Invalid choice.")


if __name__ == "__main__":
    main()