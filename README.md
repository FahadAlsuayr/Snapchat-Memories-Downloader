# (TUSMD) The Ultimate Snapchat Memory Downloader

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Mac%20%7C%20Linux-lightgrey)
![Status](https://img.shields.io/badge/Status-Maintained-success)

A high-performance, asynchronous CLI tool to bulk download, merge, and organize your entire Snapchat Memories history. 

This tool is designed for speed and data integrity. It solves the common problem of "lost dates" by automatically recovering file creation timestamps and GPS data, ensuring your memories appear in the correct order in your photo gallery.

## ✨ Key Features

* **⚡ Ultra-Fast Bulk Download:** Uses `asyncio` to maximize bandwidth by downloading files in parallel.
* **📍 Metadata Preservation:** Automatically fixes "Date Created" and embeds GPS/EXIF data so photos sort correctly in your gallery.
* **🏎️ GPU Acceleration:** Detects NVIDIA GPUs to speed up video overlay merging (NVENC), falling back to CPU if unavailable.
* **🛡️ Self-Healing & Diagnostics:** Automatically detects 0KB or corrupt files and attempts to re-download them using backup links.
* **🔧 Smart Modes:** Choose between archiving everything, optimizing for space, or keeping raw originals.

---

## ⚙️ Prerequisites

* **Python 3.10+**
* **FFmpeg** (Required for video processing)
    * *Windows:* `winget install ffmpeg`
    * *Mac:* `brew install ffmpeg`
    * *Linux:* `sudo apt install ffmpeg`
 
## 📥 Installation

1.  **Clone the repository**
    ```bash
    git clone https://github.com/FahadAlsuayr/TUSMD_The-Ultimate-Snapchat-Memories-Downloader.git
    cd tusmd_the-ultimate-snapchat-memory-downloader
    ```

2.  **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

---

## 🚀 Usage

### 1. Get Your Data
1.  Log in to [Snapchat Accounts](https://accounts.snapchat.com/).
2.  Click **"My Data"**.
3.  **Important:** Select **"Export JSON Files"** and **"Memories"**.
4.  Submit the request and wait for the email.

### 2. Setup Directory
Download the ZIP file from the email and **extract the entire contents** into this project's folder. Your folder should look like this:

```text
/tusmd_the-ultimate-snapchat-memory-downloader
├── main.py
├── json/               <-- Extracted from ZIP
│   └── memories_history.json
├── html/               <-- Extracted from ZIP (Optional)
```

### 3. Run the Script
```bash
python main.py
```

### 4. Select Mode
The script will ask you how you want to handle overlays (text/stickers on snaps):

| Mode | Description | Best For |
| :--- | :--- | :--- |
| **[1] Keep Both** | Saves the original raw file AND the merged video. | Archiving (Max Safety) |
| **[2] Optimized** | Saves the merged video. If no overlay exists, keeps the original. | General Use (Max Space Efficiency) |
| **[3] Raw Only** | Ignores all overlays. Saves only original files. | Speed |

---

## 🔧 CLI Options

You can skip the manual setup by using command-line arguments:

```bash
python main.py [-h] [-o OUTPUT] [-w WORKERS] [--no-exif] [--gpu] [json_file]
```

| Argument | Description | Default |
| :--- | :--- | :--- |
| `json_file` | Path to your memories JSON file. | `json/memories_history.json` |
| `-o`, `--output` | Custom directory to save downloads. | `./downloads` |
| `-w`, `--workers` | Number of concurrent download threads. | `Auto-detect` |
| `--gpu` | Force usage of NVIDIA GPU for video merging. | `False` |
| `--no-exif` | Disable metadata timestamp fixing. | `False` |

---

## ❓ Troubleshooting

* **"All download links failed"**:
    The download links in `memories_history.json` expire after a few days. If you see this error, you must request a **fresh data export** from Snapchat.

* **"FFmpeg not found"**:
    Ensure FFmpeg is installed and added to your system's PATH environment variable.

* **Stuck at 100%**:
    The script performs a final diagnostic scan to ensure no files are corrupt. This might take a moment for large collections.

## 📝 Disclaimer
This tool is an independent open-source project and is not affiliated with, endorsed by, or connected to Snapchat Inc. Use this tool responsibly for your own personal data archiving.
