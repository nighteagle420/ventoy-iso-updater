#!/usr/bin/env python3
"""
Ventoy Drive Detector & ISO Lister
Detects USB drives, verifies Ventoy installation, and lists all ISO files.
Cross-platform: Linux and Windows.
"""

import os
import sys
import json
import platform
import subprocess
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ISOFile:
    name: str
    path: Path
    size_bytes: int

    @property
    def size_human(self) -> str:
        size = self.size_bytes
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"


@dataclass
class VentoyDrive:
    mount_point: Path
    device: str
    filesystem: str
    total_size: int
    free_space: int
    ventoy_version: Optional[str] = None
    iso_files: list[ISOFile] = field(default_factory=list)

    @property
    def total_size_human(self) -> str:
        size = self.total_size
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"

    @property
    def free_space_human(self) -> str:
        size = self.free_space
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"


def detect_usb_partitions_linux() -> list[dict]:
    partitions = []
    try:
        result = subprocess.run(
            ["lsblk", "-J", "-o", "NAME,MOUNTPOINT,FSTYPE,SIZE,TRAN,HOTPLUG,TYPE,RM"],
            capture_output=True, text=True, check=True
        )
        data = json.loads(result.stdout)
        for device in data.get("blockdevices", []):
            is_usb = device.get("tran") == "usb" or (
                device.get("hotplug") and device.get("rm")
            )
            if not is_usb:
                continue
            children = device.get("children", [])
            if not children:
                if device.get("mountpoint"):
                    partitions.append({
                        "device": f"/dev/{device['name']}",
                        "mount_point": device["mountpoint"],
                        "filesystem": device.get("fstype", "unknown"),
                    })
            else:
                for part in children:
                    if part.get("mountpoint") and part.get("type") == "part":
                        partitions.append({
                            "device": f"/dev/{part['name']}",
                            "mount_point": part["mountpoint"],
                            "filesystem": part.get("fstype", "unknown"),
                        })
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[!] lsblk detection failed: {e}")
    return partitions


def detect_usb_partitions_windows() -> list[dict]:
    partitions = []
    try:
        ps_command = (
            "Get-WmiObject Win32_LogicalDisk | "
            "Where-Object { $_.DriveType -eq 2 } | "
            "Select-Object DeviceID, FileSystem, VolumeName | "
            "ConvertTo-Json -Compress"
        )
        result = subprocess.run(
            ["powershell", "-Command", ps_command],
            capture_output=True, text=True, check=True
        )
        output = result.stdout.strip()
        if not output:
            return partitions
        data = json.loads(output)
        if isinstance(data, dict):
            data = [data]
        for drive in data:
            partitions.append({
                "device": drive.get("DeviceID", "?"),
                "mount_point": drive.get("DeviceID", "?") + "\\",
                "filesystem": drive.get("FileSystem", "unknown"),
            })
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[!] Windows drive detection failed: {e}")
    return partitions


def detect_usb_partitions() -> list[dict]:
    system = platform.system()
    if system == "Linux":
        return detect_usb_partitions_linux()
    elif system == "Windows":
        return detect_usb_partitions_windows()
    else:
        print(f"[!] Unsupported platform: {system}")
        return []


def check_ventoy(mount_point: Path) -> Optional[str]:
    ventoy_dir = mount_point / "ventoy"
    if not ventoy_dir.is_dir():
        return None
    version = None
    ventoy_json = ventoy_dir / "ventoy.json"
    if ventoy_json.is_file():
        try:
            data = json.loads(ventoy_json.read_text(encoding="utf-8"))
            version = data.get("VENTOY_VERSION") or data.get("version")
        except (json.JSONDecodeError, OSError):
            pass
    if not version:
        for marker_file in ("ventoy_grub.cfg", "ventoy.cpio"):
            if (ventoy_dir / marker_file).exists():
                version = "detected"
                break
    return version or "detected"


def find_iso_files(mount_point: Path) -> list[ISOFile]:
    iso_files = []
    try:
        for iso_path in sorted(mount_point.rglob("*.iso")):
            try:
                iso_path.relative_to(mount_point / "ventoy")
                continue
            except ValueError:
                pass
            try:
                size = iso_path.stat().st_size
                iso_files.append(ISOFile(name=iso_path.name, path=iso_path, size_bytes=size))
            except OSError:
                continue
    except PermissionError:
        print(f"[!] Permission denied scanning {mount_point}")
    return iso_files


def detect_ventoy_drives() -> list[VentoyDrive]:
    print(f"[*] Platform: {platform.system()} {platform.release()}")
    print("[*] Scanning for USB drives...\n")
    usb_partitions = detect_usb_partitions()
    if not usb_partitions:
        print("[!] No USB drives detected.")
        print("    Make sure your USB drive is plugged in and mounted.")
        if platform.system() == "Linux":
            print("    Try: lsblk  (to see all block devices)")
            print("    Mount with: sudo mount /dev/sdX1 /mnt/usb")
        return []
    print(f"[*] Found {len(usb_partitions)} USB partition(s):\n")
    for p in usb_partitions:
        print(f"    {p['device']}  →  {p['mount_point']}  ({p['filesystem']})")
    print()
    ventoy_drives = []
    for partition in usb_partitions:
        mount = Path(partition["mount_point"])
        if not mount.exists():
            continue
        version = check_ventoy(mount)
        if version is None:
            print(f"[·] {partition['device']}: not a Ventoy drive, skipping.")
            continue
        try:
            usage = os.statvfs(str(mount)) if platform.system() != "Windows" else None
            if usage:
                total = usage.f_frsize * usage.f_blocks
                free = usage.f_frsize * usage.f_bavail
            else:
                import shutil
                disk_usage = shutil.disk_usage(str(mount))
                total, free = disk_usage.total, disk_usage.free
        except OSError:
            total, free = 0, 0
        drive = VentoyDrive(
            mount_point=mount, device=partition["device"],
            filesystem=partition["filesystem"], total_size=total,
            free_space=free, ventoy_version=version,
        )
        drive.iso_files = find_iso_files(mount)
        ventoy_drives.append(drive)
    return ventoy_drives


def display_results(drives: list[VentoyDrive]) -> None:
    if not drives:
        print("[!] No Ventoy drives found on any USB device.")
        return
    for drive in drives:
        print("=" * 60)
        print(f"  VENTOY DRIVE DETECTED")
        print("=" * 60)
        print(f"  Device      : {drive.device}")
        print(f"  Mount Point : {drive.mount_point}")
        print(f"  Filesystem  : {drive.filesystem}")
        print(f"  Ventoy      : {drive.ventoy_version}")
        print(f"  Total Size  : {drive.total_size_human}")
        print(f"  Free Space  : {drive.free_space_human}")
        print("-" * 60)
        if not drive.iso_files:
            print("  No ISO files found on this drive.")
        else:
            print(f"  ISO Files ({len(drive.iso_files)}):\n")
            col_width = min(max(len(iso.name) for iso in drive.iso_files), 50)
            for i, iso in enumerate(drive.iso_files, 1):
                name = iso.name[:47] + "..." if len(iso.name) > 50 else iso.name
                print(f"  {i:3d}. {name:<{col_width}}  {iso.size_human:>10}")
        print("=" * 60 + "\n")


def main():
    drives = detect_ventoy_drives()
    display_results(drives)
    return drives

if __name__ == "__main__":
    main()