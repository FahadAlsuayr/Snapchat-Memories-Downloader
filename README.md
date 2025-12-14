# Snapchat Memory Downloader

A CLI tool to batch download, merge, and organize Snapchat Memories. Features asynchronous concurrent downloading, metadata correction, and hardware-agnostic video processing.

## Capabilities

* **Concurrent Downloading:** Uses `asyncio` to download multiple files simultaneously.
* **Auto-Correction:** Detects and repairs incomplete downloads or corrupt files.
* **Metadata Reconstruction:** Sets file creation dates and EXIF data to match the original memory timestamp.
* **Hardware Agnostic:** Works on standard CPUs via software encoding or NVIDIA GPUs via NVENC (optional).

## Prerequisites

* **Python 3.10** or higher.
* **FFmpeg** (Must be installed and accessible in the system PATH).
    * Windows: `winget install ffmpeg`
    * Mac: `brew install ffmpeg`
    * Linux: `sudo apt install ffmpeg`

## Installation

1.  Clone the repository:
    ```bash
    git clone [https://github.com/FahadAlsuaer/snapchat-memory-downloader.get](https://github.com/FahadAlsuaer/snapchat-memory-downloader.git)
    cd snapchat-memory-downloader
    ```

2.  Install python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### 1. Export Data
Request your data from [Snapchat Accounts](https://accounts.snapchat.com). Download the ZIP, extract it, and locate the `memories_history.json` file.

### 2. Directory Setup
Create a `json` folder in the project directory and place your file inside:
```text
/snapchat-memory-downloader
    ├── main.py
    ├── json/
    │   └── memories_history.json
```

### 3. Run
```bash
python main.py
```

## Configuration

The script attempts to auto-detect optimal settings for your hardware. You can manually override these using the flags below.

### Worker Threads (`-w`)
Controls the number of simultaneous active downloads.

| Hardware Tier | Recommended Setting | Command |
| :--- | :--- | :--- |
| **Basic / Laptop** | 4 - 8 Workers | `python main.py -w 5` |
| **Mid-Range Desktop** | 10 - 20 Workers | `python main.py -w 15` |
| **High-End / Server** | 30+ Workers | `python main.py -w 50` |

*Note: Setting this too high on a weak CPU may cause system instability or network timeouts.*

### Hardware Acceleration (`--gpu`)
By default, the script uses the CPU for video merging to ensure maximum compatibility. If you have an NVIDIA GPU, you can enable hardware acceleration for faster processing.

```bash
python main.py --gpu
```

### Other Options
| Flag | Description |
| :--- | :--- |
| `-o [path]` | Specify a custom output directory (Default: `./downloads`). |
| `--no-exif` | Disable metadata timestamp modification. |

## Error Handling
If downloads fail, the script generates a `failed_memories.json` log and a `missing_report.txt` summary. The script includes a self-healing phase that will automatically attempt to re-download missing files using backup links found in the JSON data.

## Disclaimer
This software is provided for archiving purposes only. Users are responsible for their own data retention and security. It is not affiliated with or endorsed by Snapchat Inc.