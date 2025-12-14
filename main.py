import argparse
import asyncio
import json
import re
import random
import subprocess
import zipfile
import shutil
import threading
import sys
import os
import time
from datetime import datetime
from pathlib import Path

# Try importing dependencies
try:
    import httpx
    from PIL import Image
    from pydantic import BaseModel, Field, field_validator
    from tqdm.auto import tqdm 
    import ffmpeg
except ImportError as e:
    print(f"❌ Missing dependency: {e}")
    print("Run: pip install httpx pillow pydantic tqdm ffmpeg-python")
    sys.exit(1)

# ======================================================
# GLOBALS
# ======================================================

# DYNAMIC SEMAPHORE: Will be updated in main() based on worker count
DOWNLOAD_SEM = asyncio.Semaphore(50) 
MERGE_SEM = asyncio.Semaphore(5) 
IMG_SEM = asyncio.Semaphore(20)

BAR_LOCK = threading.Lock()
FILE_LOCK = threading.Lock() 
FAILED_LOG = "failed_memories.json"
FFMPEG_TIMEOUT = 300 

ERROR_COUNT = 0 

USE_GPU = True 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive"
}

# ======================================================
# DATA MODEL
# ======================================================

class Memory(BaseModel):
    date: datetime = Field(alias="Date")
    media_url: str | None = Field(default=None, alias="Media Download Url")
    download_link: str | None = Field(default=None, alias="Download Link")
    media_type: str | None = Field(default=None, alias="Media Type") 
    location: str = Field(default="", alias="Location")
    latitude: float | None = None
    longitude: float | None = None

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v):
        if not isinstance(v, str): return v
        v = v.strip()
        formats = ["%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S %Z"]
        for fmt in formats:
            try: return datetime.strptime(v, fmt)
            except ValueError: continue
        try: return datetime.fromisoformat(v)
        except ValueError: raise ValueError(f"Unknown date format: {v}")

    def model_post_init(self, _):
        if self.location:
            m = re.search(r"(-?\d+\.\d+),\s*(-?\d+\.\d+)", self.location)
            if m:
                self.latitude = float(m.group(1))
                self.longitude = float(m.group(2))

    @property
    def filename(self):
        return self.date.strftime("%Y-%m-%d_%H-%M-%S")

# ======================================================
# UTILS & VERIFICATION
# ======================================================

def is_img(p): return p.suffix.lower() in (".jpg", ".jpeg", ".png")

def clean_debris(outdir: Path):
    for item in outdir.glob("*_zip"):
        if item.is_dir():
            try: shutil.rmtree(item)
            except: pass
    for item in outdir.glob("*_TEMP"):
        if item.is_file():
            try: item.unlink()
            except: pass
    for item in outdir.glob("*.zip"):
        if item.is_file():
            try: item.unlink()
            except: pass

def log_failure(mem: Memory, error_msg: str):
    entry = mem.model_dump(by_alias=True)
    entry["Date"] = mem.date.strftime("%Y-%m-%d %H:%M:%S UTC")
    entry["_error"] = str(error_msg)

    with FILE_LOCK:
        try:
            if os.path.exists(FAILED_LOG):
                with open(FAILED_LOG, "r", encoding="utf-8") as f: data = json.load(f)
            else: data = []
        except: data = []
        
        found = False
        for d in data:
            if d.get("Media Download Url") == entry["Media Download Url"]:
                d["_error"] = str(error_msg) 
                found = True
                break
        if not found: data.append(entry)
            
        with open(FAILED_LOG, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

def safe_write(path, data, retries=3):
    for i in range(retries):
        try:
            path.write_bytes(data)
            return
        except PermissionError: time.sleep(1.0)
    raise Exception(f"Failed to write {path.name} (Locked)")

def wait_for_file_access(path, retries=3):
    for i in range(retries):
        try:
            if not path.exists(): return False
            path.rename(path)
            return True
        except PermissionError: time.sleep(1.0)
        except Exception: return True
    return False

def verify_file_integrity(path):
    if not path.exists() or path.stat().st_size == 0:
        raise Exception("File empty or missing")
    
    if is_img(path):
        try:
            with Image.open(path) as img: img.verify()
            return True
        except: raise Exception("Corrupt Image Header")
    else:
        try:
            ffmpeg.probe(str(path), cmd='ffprobe')
            return True
        except ffmpeg.Error:
            raise Exception("Corrupt Video Container")
        except:
            if path.stat().st_size < 1024: raise Exception("Video too small")
            return True

# ======================================================
# MERGE & EXIF
# ======================================================

def set_exif_data(path, mem: Memory):
    if not path.exists(): return
    if not wait_for_file_access(path): return

    try:
        mod_time = mem.date.timestamp()
        os.utime(path, (mod_time, mod_time))
    except: pass

    ts = mem.date.strftime("%Y:%m:%d %H:%M:%S")
    cmd = ["exiftool", "-overwrite_original", "-q", "-ignoreMinorErrors"]
    
    if is_img(path):
        cmd.extend([f"-DateTimeOriginal={ts}", f"-CreateDate={ts}", f"-ModifyDate={ts}"])
    else:
        cmd.extend([f"-CreateDate={ts}", f"-ModifyDate={ts}", f"-TrackCreateDate={ts}", f"-MediaCreateDate={ts}",
                    f"-Keys:CreationDate={ts}", f"-QuickTime:CreateDate={ts}"])

    if mem.latitude is not None:
        lat_ref = 'N' if mem.latitude >= 0 else 'S'
        lon_ref = 'E' if mem.longitude >= 0 else 'W'
        cmd.extend([
            f"-GPSLatitude={mem.latitude}", f"-GPSLatitudeRef={lat_ref}",
            f"-GPSLongitude={mem.longitude}", f"-GPSLongitudeRef={lon_ref}",
            f"-GPSCoordinates={mem.latitude}, {mem.longitude}, 0",
            f"-Keys:GPSCoordinates={mem.latitude} {mem.longitude}"
        ])
    
    cmd.append(str(path))
    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=30)
    except: pass 

def sync_unzip(zip_path: Path, target_dir: Path):
    main, overlay = None, None
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.startswith("__MACOSX/"): continue
            if "-main." in name: main = Path(z.extract(name, target_dir))
            elif "-overlay." in name: overlay = Path(z.extract(name, target_dir))
    return main, overlay

def sync_merge_videos(main, overlay, out):
    is_ovr_img = is_img(overlay)
    input_args = {"hwaccel": "cuda"} if USE_GPU else {}
    output_args = {"vcodec": "h264_nvenc" if USE_GPU else "libx264", "vsync": "vfr", "max_muxing_queue_size": 9999}
    if USE_GPU: output_args["preset"] = "p1" 

    input_main = ffmpeg.input(str(main), **input_args)
    if is_ovr_img: input_overlay = ffmpeg.input(str(overlay), loop=1)
    else: input_overlay = ffmpeg.input(str(overlay), **input_args)

    try:
        v_main = input_main.video
        v_ovr = input_overlay.video
        v_main = v_main.filter('scale', w='trunc(iw/2)*2', h='trunc(ih/2)*2').filter('format', 'yuv420p')
        joined = ffmpeg.filter_multi_output([v_ovr, v_main], 'scale2ref')
        final_video = joined[1].overlay(joined[0], shortest=1)
        stream = ffmpeg.output(final_video, str(out), **output_args)
        cmd = ffmpeg.compile(stream, overwrite_output=True)
        subprocess.run(cmd, check=True, timeout=FFMPEG_TIMEOUT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return
    except: pass 

    try:
        final_video = input_main.overlay(input_overlay, shortest=1)
        stream = ffmpeg.output(final_video, str(out), **output_args)
        cmd = ffmpeg.compile(stream, overwrite_output=True)
        subprocess.run(cmd, check=True, timeout=FFMPEG_TIMEOUT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return
    except Exception as e:
        raise Exception(f"Merge Failed/Timeout: {e}")

def sync_merge_images(main, overlay, out):
    verify_file_integrity(main)
    verify_file_integrity(overlay)
    base = Image.open(main).convert("RGBA")
    top = Image.open(overlay).convert("RGBA")
    if top.size != base.size:
        top = top.resize(base.size, Image.Resampling.LANCZOS)
    base.paste(top, (0, 0), top)
    base.convert("RGB").save(out, "JPEG")

# ======================================================
# CORE PROCESS
# ======================================================

async def fetch_binary(client, url):
    try:
        r = await client.get(url, timeout=45)
        return r.content if r.status_code == 200 else None
    except: return None

async def process_memory(client, mem, outdir, add_exif, specific_bar, total_bar, force_backup_link=False):
    if not mem.media_url and not mem.download_link: return
    name = mem.filename
    temp_path = outdir / f"{name}_TEMP"
    extract_dir = outdir / f"{name}_zip"
    
    # === ZERO-TRUST EXISTING FILE CHECK ===
    possible_files = [
        outdir / f"{name}_MERGED.mp4", outdir / f"{name}_MERGED.jpg",
        outdir / f"{name}_MAIN.mp4", outdir / f"{name}_MAIN.jpg", outdir / f"{name}_MAIN.png"
    ]
    
    file_valid = False
    for f in possible_files:
        if f.exists():
            if f.stat().st_size == 0:
                try: f.unlink()
                except: pass
                continue 
            try:
                verify_file_integrity(f) 
            except:
                try: f.unlink()
                except: pass
                continue 
            try:
                target_ts = mem.date.timestamp()
                if abs(f.stat().st_mtime - target_ts) > 86400:
                     os.utime(f, (target_ts, target_ts))
            except: pass
            file_valid = True
            break
    
    if file_valid:
        if specific_bar: specific_bar.update(1)
        if total_bar: total_bar.update(1)
        return

    # Retry Loop
    for attempt in range(1, 4):
        created_files = []
        try:
            async with DOWNLOAD_SEM:
                links_to_try = []
                if force_backup_link:
                    if mem.download_link: links_to_try.append(mem.download_link)
                    if mem.media_url: links_to_try.append(mem.media_url)
                else:
                    if mem.media_url: links_to_try.append(mem.media_url)
                    if mem.download_link: links_to_try.append(mem.download_link)

                data = None
                for link in links_to_try:
                    data = await fetch_binary(client, link)
                    if data: break
                
                if not data: raise Exception("Download Failed (All Links Dead)")
                await asyncio.to_thread(safe_write, temp_path, data)

            try: is_zip = zipfile.is_zipfile(temp_path)
            except: is_zip = False

            if is_zip:
                zip_path = outdir / f"{name}.zip"
                if not zip_path.exists(): temp_path.rename(zip_path)
                extract_dir.mkdir(exist_ok=True)
                main, overlay = await asyncio.to_thread(sync_unzip, zip_path, extract_dir)
                if not main: raise Exception("Zip Empty")
                
                final_main = outdir / f"{name}_MAIN{main.suffix}"
                await asyncio.to_thread(safe_write, final_main, main.read_bytes())
                created_files.append(final_main)
                if add_exif: await asyncio.to_thread(set_exif_data, final_main, mem)

                merged_out = outdir / f"{name}_MERGED{main.suffix}"
                if is_img(main) and is_img(overlay):
                    async with IMG_SEM: await asyncio.to_thread(sync_merge_images, final_main, overlay, merged_out)
                else:
                    async with MERGE_SEM: await asyncio.to_thread(sync_merge_videos, final_main, overlay, merged_out)
                
                created_files.append(merged_out)
                if add_exif: await asyncio.to_thread(set_exif_data, merged_out, mem)
                await asyncio.to_thread(zip_path.unlink, missing_ok=True)
            else:
                ext = ".mp4" if mem.media_type == "Video" else ".jpg"
                final_path = outdir / f"{name}_MAIN{ext}"
                if not final_path.exists(): temp_path.rename(final_path)
                created_files.append(final_path)
                await asyncio.to_thread(verify_file_integrity, final_path)
                if add_exif: await asyncio.to_thread(set_exif_data, final_path, mem)

            if extract_dir.exists(): await asyncio.to_thread(shutil.rmtree, extract_dir, ignore_errors=True)
            break 

        except Exception as e:
            for f in created_files:
                try: 
                    if f.exists(): f.unlink()
                except: pass
            
            if attempt == 3:
                log_failure(mem, str(e))
                with BAR_LOCK:
                    global ERROR_COUNT
                    ERROR_COUNT += 1
                    if total_bar: total_bar.set_postfix(errors=ERROR_COUNT)

            if attempt < 3: await asyncio.sleep(attempt * 2)
        finally:
             if extract_dir.exists(): await asyncio.to_thread(shutil.rmtree, extract_dir, ignore_errors=True)

    if specific_bar: specific_bar.update(1)
    if total_bar: total_bar.update(1)

# ======================================================
# RUNNERS & AUDIT
# ======================================================

async def run_batch(memories, outdir, add_exif, desc, n_workers, force_backup_link=False):
    global ERROR_COUNT
    ERROR_COUNT = 0
    
    img_queue = asyncio.Queue()
    vid_queue = asyncio.Queue()
    img_count = sum(1 for m in memories if m.media_type == "Image")
    vid_count = len(memories) - img_count

    for m in memories:
        if m.media_type == "Image": img_queue.put_nowait(m)
        else: vid_queue.put_nowait(m)

    print(f"\n🔹 {desc} ({len(memories)} items) | Threads: {n_workers}")
    pbar_total = tqdm(total=len(memories), position=0, desc="TOTAL", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{postfix}]")
    pbar_img = tqdm(total=img_count, position=1, desc="📸 IMG ", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}")
    pbar_vid = tqdm(total=vid_count, position=2, desc="🎥 VID ", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}")
    
    pbar_total.set_postfix(errors=0)

    async def worker(q, bar):
        async with httpx.AsyncClient(headers=HEADERS, timeout=60, follow_redirects=True) as client:
            while not q.empty():
                try:
                    mem = await q.get()
                    await process_memory(client, mem, outdir, add_exif, bar, pbar_total, force_backup_link)
                finally: q.task_done()

    tasks = []
    # DYNAMIC WORKERS: Uses 'n_workers' for video, and n_workers*2 for images
    for _ in range(n_workers): tasks.append(asyncio.create_task(worker(vid_queue, pbar_vid)))
    for _ in range(n_workers * 2): tasks.append(asyncio.create_task(worker(img_queue, pbar_img)))

    await asyncio.gather(img_queue.join(), vid_queue.join())
    for t in tasks: t.cancel()
    pbar_vid.close(); pbar_img.close(); pbar_total.close()

def scan_for_issues(all_memories, outdir, delete_bad=True, fix_metadata=False, deep_scan=False):
    missing_or_bad = []
    metadata_fixed = 0

    for mem in tqdm(all_memories, desc="🔎 Checking Files", unit="file"):
        base_name = mem.filename
        candidates = [f"{base_name}_MERGED.mp4", f"{base_name}_MERGED.jpg",
                      f"{base_name}_MAIN.mp4", f"{base_name}_MAIN.jpg", f"{base_name}_MAIN.png"]
        
        found_valid = False
        found_0kb = False
        bad_file_path = None
        best_file = None

        for c in candidates:
            fpath = outdir / c
            if fpath.exists():
                if fpath.stat().st_size > 0:
                    if deep_scan:
                        try:
                            verify_file_integrity(fpath)
                            found_valid = True
                            best_file = fpath
                            break
                        except:
                            found_0kb = True 
                            bad_file_path = fpath
                    else:
                        found_valid = True
                        best_file = fpath
                        break
                else:
                    found_0kb = True
                    bad_file_path = fpath
        
        if found_valid and fix_metadata:
            try:
                actual_ts = best_file.stat().st_mtime
                target_ts = mem.date.timestamp()
                diff = abs(actual_ts - target_ts)
                if diff > 86400: 
                    os.utime(best_file, (target_ts, target_ts))
                    metadata_fixed += 1
            except: pass

        if not found_valid:
            if found_0kb and delete_bad and bad_file_path:
                try: bad_file_path.unlink()
                except: pass
            missing_or_bad.append(mem)
            
    return missing_or_bad, metadata_fixed

async def main():
    global USE_GPU
    global DOWNLOAD_SEM
    
    ap = argparse.ArgumentParser()
    ap.add_argument("json_file", nargs="?", default="json/memories_history.json")
    ap.add_argument("-o", "--output", default="./downloads")
    ap.add_argument("--no-exif", action="store_true")
    ap.add_argument("-w", "--workers", type=int, default=10) 
    ap.add_argument("--gpu", action="store_true", default=True) 
    args = ap.parse_args()

    USE_GPU = args.gpu
    workers_val = args.workers
    
    # UNLOCKED: Update Semaphore to match worker count if user forces high numbers
    if workers_val > 50:
        DOWNLOAD_SEM = asyncio.Semaphore(workers_val + 20)
    
    outdir = Path(args.output)
    outdir.mkdir(parents=True, exist_ok=True)
    clean_debris(outdir)
    if USE_GPU: print("🚀 GPU MODE: Active")

    try:
        raw = json.load(open(args.json_file, "r", encoding="utf-8"))
        all_memories = [Memory(**m) for m in raw["Saved Media"]]
    except: print("❌ JSON Error"); return

    await run_batch(all_memories, outdir, not args.no_exif, "Phase 1: Main Run", workers_val)

    print("\n" + "="*50)
    print("🚑 STARTING SELF-HEALING DIAGNOSTICS")
    print("="*50)

    print("\n🔎 Audit Run #1 (Deep Verification)...")
    bad_memories, _ = await asyncio.to_thread(scan_for_issues, all_memories, outdir, True, False, True)
    
    if bad_memories:
        print(f"⚠️ Found {len(bad_memories)} issues. Retrying...")
        await run_batch(bad_memories, outdir, not args.no_exif, "Auto-Repair Run #1", workers_val, force_backup_link=False)
    else:
        print("✅ Clean.")

    print("\n🔎 Audit Run #2 (Backup Link Strategy)...")
    bad_memories_2, _ = await asyncio.to_thread(scan_for_issues, all_memories, outdir, True, False, True)
    
    if bad_memories_2:
        print(f"⚠️ {len(bad_memories_2)} items still broken. ENGAGING BACKUP LINKS...")
        await run_batch(bad_memories_2, outdir, not args.no_exif, "Auto-Repair Run #2", workers_val, force_backup_link=True)
    else:
        print("✅ Clean.")

    print("\n" + "="*60)
    print("📊 FINAL FORENSIC REPORT")
    print("="*60)
    
    print("⏳ Final Metadata Enforcement...")
    final_issues, fixed_count = await asyncio.to_thread(scan_for_issues, all_memories, outdir, False, True, True)
    
    total = len(all_memories)
    success = total - len(final_issues)
    
    print(f"   Total Memories Target : {total}")
    print(f"   Successfully Saved    : {success}")
    print(f"   Failed / Missing      : {len(final_issues)}")
    if fixed_count > 0:
        print(f"   Metadata Fixed        : {fixed_count}")
    print("-" * 40)
    
    if len(final_issues) == 0:
        print("\n✅ PERFECT SCORE: All files verified.")
        if os.path.exists(FAILED_LOG): os.remove(FAILED_LOG)
    else:
        print(f"\n❌ FINAL FAILURE: {len(final_issues)} files could not be recovered.")
        error_map = {}
        if os.path.exists(FAILED_LOG):
            try:
                log_data = json.load(open(FAILED_LOG, "r", encoding="utf-8"))
                for item in log_data: error_map[item["Media Download Url"]] = item.get("_error", "Unknown")
            except: pass

        with open("missing_report.txt", "w", encoding="utf-8") as f:
            for m in final_issues: 
                reason = error_map.get(m.media_url, "Unknown")
                f.write(f"❌ {m.filename} -> {reason}\n")
        print("📄 Full report saved to 'missing_report.txt'")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())