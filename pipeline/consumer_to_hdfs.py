"""
consumer_to_hdfs.py — Consumer Kafka → HDFS (JSON) + Local JSON
==================================================================
Anggota : Akbar
Topics  : pangan-api, pangan-rss
Group   : pangan-consumer-group

Alur data:
  Kafka → Consumer (real-time) → local JSON (dashboard langsung baca)
                               → HDFS (JSON format, diberi nama timestamp)
"""

import json, os, threading, time, sys
from datetime import datetime, timezone, timedelta
from collections import deque
from kafka import KafkaConsumer
from tenacity import retry, wait_exponential, stop_after_attempt
import subprocess

# ─── Konfigurasi ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")  # lebih stabil di Windows (hindari resolusi IPv6 localhost)
GROUP_ID         = "pangan-consumer-group"
HDFS_BATCH_SIZE  = 8      # Flush ke HDFS setiap 8 pesan
MAX_LOCAL_EVENTS = 200    # Simpan lebih banyak event untuk analisis periodik (UI tetap ambil 50 terakhir)

# Otomasi analisis (update spark_results.json + upload ke HDFS /data/pangan/hasil)
ANALYSIS_TRIGGER_EVERY_N_API_MESSAGES = 100
ANALYSIS_MIN_INTERVAL_SECONDS = 20

# Path HDFS
HDFS_PATH_API = "/data/pangan/api"
HDFS_PATH_RSS = "/data/pangan/rss"

# Path lokal (untuk dashboard)
LOCAL_DIR      = os.path.join(os.path.dirname(__file__), "..", "dashboard", "data")
LOCAL_API_FILE = os.path.join(LOCAL_DIR, "live_api.json")
LOCAL_RSS_FILE = os.path.join(LOCAL_DIR, "live_rss.json")

# Buffer in-memory untuk HDFS batch
buffer_api: list = []
buffer_rss: list = []
buffer_lock       = threading.Lock()

# Deque untuk local JSON (dashboard)
local_api = deque(maxlen=MAX_LOCAL_EVENTS)
local_rss = deque(maxlen=20)

TZ_WIB = timezone(timedelta(hours=7))


def _normalize_komoditas(name: str) -> str:
    if not name:
        return ""
    key = str(name).strip().lower()
    key = " ".join(key.split())
    if key in {"beras", "beras medium"}:
        return "Beras"
    return str(name).strip()


# ─── Otomasi Analisis ───────────────────────────────────────────────────────

_analysis_lock = threading.Lock()
_analysis_running = False
_api_messages_since_analysis = 0
_last_analysis_at = 0.0


# ─── Backoff Upload HDFS (hindari spam saat docker/HDFS bermasalah) ─────────

_hdfs_api_failures = 0
_hdfs_api_next_try_at = 0.0
_hdfs_rss_failures = 0
_hdfs_rss_next_try_at = 0.0


def _analysis_script_path() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "spark", "analysis.py"))


def _run_analysis_job():
    global _analysis_running, _last_analysis_at
    script = _analysis_script_path()
    try:
        print(f"  [ANALYSIS] Menjalankan analisis: {script}")
        subprocess.run([sys.executable, script], check=False)
    except Exception as e:
        print(f"  [ANALYSIS] Gagal menjalankan analisis: {e}")
    finally:
        with _analysis_lock:
            _analysis_running = False
            _last_analysis_at = time.time()


def _maybe_trigger_analysis():
    global _analysis_running, _api_messages_since_analysis
    with _analysis_lock:
        _api_messages_since_analysis += 1
        if _analysis_running:
            return
        if _api_messages_since_analysis < ANALYSIS_TRIGGER_EVERY_N_API_MESSAGES:
            return
        if (time.time() - _last_analysis_at) < ANALYSIS_MIN_INTERVAL_SECONDS:
            return
        _analysis_running = True
        _api_messages_since_analysis = 0

    t = threading.Thread(target=_run_analysis_job, name="analysis-runner", daemon=True)
    t.start()


# ─── Fungsi HDFS ─────────────────────────────────────────────────────────────

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def simpan_ke_hdfs(data: list, hdfs_path: str, label: str):
    """
    Simpan batch data ke HDFS dalam format JSON bernama timestamp.
    Menggunakan subprocess (docker exec) — Opsi A dari hint soal.
    Nama file: YYYY-MM-DD_HH-MM-SS.json
    """
    if not data:
        return

    timestamp_str = datetime.now(TZ_WIB).strftime("%Y-%m-%d_%H-%M-%S")
    filename  = f"{timestamp_str}.json"

    try:
        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file_path = os.path.join(tmp_dir, filename)

            # Simpan ke file lokal sementara sebagai JSON lines
            with open(tmp_file_path, "w", encoding="utf-8") as f:
                for record in data:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")

            hdfs_full_path = f"{hdfs_path}/{filename}"

            # Pastikan direktori HDFS ada
            subprocess.run(
                ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_path],
                check=True, capture_output=True
            )

            # Copy file ke dalam container namenode
            subprocess.run(
                ["docker", "cp", tmp_file_path, f"namenode:/tmp/{filename}"],
                check=True, capture_output=True
            )
            # Upload dari container ke HDFS
            subprocess.run(
                ["docker", "exec", "namenode",
                 "hdfs", "dfs", "-put", "-f", f"/tmp/{filename}", hdfs_full_path],
                check=True, capture_output=True
            )
            print(f"  [HDFS] [{label}] → {hdfs_full_path} ({len(data)} records) - JSON")
    except subprocess.CalledProcessError as e:
        stderr = ""
        try:
            stderr = (e.stderr or b"").decode("utf-8", errors="replace").strip()
        except Exception:
            stderr = ""
        if stderr:
            print(f"  [HDFS] Gagal simpan ke HDFS (stderr): {stderr}")
        print(f"  [HDFS] Gagal simpan ke HDFS: {e}")
        raise
    except Exception as e:
        print(f"  [HDFS] Gagal simpan ke HDFS: {e}")
        raise


def update_local_file(filepath: str, data: deque):
    """Update file JSON lokal untuk dashboard (real-time)."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(list(data), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  [LOCAL] Gagal update {os.path.basename(filepath)}: {e}")


# ─── Thread Consumer API ──────────────────────────────────────────────────────

def consume_api():
    """Thread consumer untuk topic pangan-api."""
    global _hdfs_api_failures, _hdfs_api_next_try_at
    msg_count = 0
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                "pangan-api",
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                api_version=(3, 9, 0),
                request_timeout_ms=30000,
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000,
                reconnect_backoff_ms=500,
                reconnect_backoff_max_ms=5000,
                client_id="pangan-consumer-api",
            )
            print("  [OK] Consumer API aktif (topic: pangan-api)")

            for msg in consumer:
                data = msg.value

                # Normalisasi nama komoditas agar konsisten di seluruh pipeline
                if isinstance(data, dict) and data.get("komoditas"):
                    data["komoditas"] = _normalize_komoditas(data.get("komoditas"))

                # 1. Update local JSON LANGSUNG (real-time untuk dashboard)
                with buffer_lock:
                    local_api.append(data)
                    buffer_api.append(data)
                    msg_count += 1
                    current_count = len(buffer_api)

                update_local_file(LOCAL_API_FILE, local_api)

                # Log ringkas
                kom = data.get("komoditas", "?")
                harga = data.get("harga", 0)
                pct = data.get("perubahan_persen", 0)
                arrow = "+" if pct > 0 else ("-" if pct < 0 else "=")
                print(
                    f"  [API] #{msg_count} {kom:<15} Rp {harga:>9,.0f} ({arrow}{abs(pct):.2f}%) "
                    f"| local={len(local_api)}/{MAX_LOCAL_EVENTS} | hdfs_batch={current_count}/{HDFS_BATCH_SIZE}"
                )

                # 1b. Otomasi analisis tiap N pesan API
                _maybe_trigger_analysis()

                # 2. Flush ke HDFS setelah batch penuh
                if current_count >= HDFS_BATCH_SIZE:
                    now = time.time()
                    if now < _hdfs_api_next_try_at:
                        continue
                    # Jangan hilangkan data kalau upload gagal: hapus dari buffer hanya setelah sukses.
                    with buffer_lock:
                        batch = buffer_api[:HDFS_BATCH_SIZE]
                    try:
                        simpan_ke_hdfs(batch, HDFS_PATH_API, "API")
                        with buffer_lock:
                            del buffer_api[:HDFS_BATCH_SIZE]
                        _hdfs_api_failures = 0
                        _hdfs_api_next_try_at = 0.0
                    except Exception as e:
                        _hdfs_api_failures += 1
                        delay = min(60, 2 ** min(_hdfs_api_failures, 5))
                        _hdfs_api_next_try_at = time.time() + delay
                        print(
                            f"  [ERROR] Gagal simpan HDFS setelah retries (batch tetap disimpan). "
                            f"Retry lagi dalam {delay}s: {e}"
                        )
        except Exception as e:
            # Kafka-python di Windows kadang melempar ValueError: Invalid file descriptor: -1
            level = "WARN" if "Invalid file descriptor" in str(e) else "CRITICAL"
            print(f"  [{level}] Error di consumer API: {e} (reconnect dalam 3 detik)")
            time.sleep(3)
        finally:
            try:
                if consumer is not None:
                    consumer.close()
            except Exception:
                pass


# ─── Thread Consumer RSS ──────────────────────────────────────────────────────

def consume_rss():
    """Thread consumer untuk topic pangan-rss."""
    global _hdfs_rss_failures, _hdfs_rss_next_try_at
    rss_count = 0
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                "pangan-rss",
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                api_version=(3, 9, 0),
                request_timeout_ms=30000,
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000,
                reconnect_backoff_ms=500,
                reconnect_backoff_max_ms=5000,
                client_id="pangan-consumer-rss",
            )
            print("  [OK] Consumer RSS aktif (topic: pangan-rss)")

            for msg in consumer:
                data = msg.value
                rss_count += 1

                with buffer_lock:
                    local_rss.append(data)
                    buffer_rss.append(data)
                    current_rss = len(buffer_rss)

                update_local_file(LOCAL_RSS_FILE, local_rss)
                print(f"  [RSS] #{rss_count} {data.get('title','?')[:60]}...")

                if current_rss >= 5:
                    now = time.time()
                    if now < _hdfs_rss_next_try_at:
                        continue
                    with buffer_lock:
                        batch = buffer_rss[:5]
                    try:
                        simpan_ke_hdfs(batch, HDFS_PATH_RSS, "RSS")
                        with buffer_lock:
                            del buffer_rss[:5]
                        _hdfs_rss_failures = 0
                        _hdfs_rss_next_try_at = 0.0
                    except Exception as e:
                        _hdfs_rss_failures += 1
                        delay = min(60, 2 ** min(_hdfs_rss_failures, 5))
                        _hdfs_rss_next_try_at = time.time() + delay
                        print(
                            f"  [ERROR] Gagal simpan HDFS setelah retries (batch tetap disimpan). "
                            f"Retry lagi dalam {delay}s: {e}"
                        )
        except Exception as e:
            level = "WARN" if "Invalid file descriptor" in str(e) else "CRITICAL"
            print(f"  [{level}] Error di consumer RSS: {e} (reconnect dalam 3 detik)")
            time.sleep(3)
        finally:
            try:
                if consumer is not None:
                    consumer.close()
            except Exception:
                pass


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  HargaPangan Consumer — Kafka → HDFS (JSON) + Local JSON")
    print(f"  Group      : {GROUP_ID}")
    print(f"  Local JSON : {LOCAL_API_FILE}")
    print(f"  HDFS batch : setiap {HDFS_BATCH_SIZE} pesan")
    print(f"  Mode       : auto_offset_reset=earliest")
    print("=" * 60)

    os.makedirs(LOCAL_DIR, exist_ok=True)

    # Threads
    t_api  = threading.Thread(target=consume_api,  name="consumer-api",  daemon=True)
    t_rss  = threading.Thread(target=consume_rss,  name="consumer-rss",  daemon=True)

    t_api.start()
    t_rss.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n  Consumer dihentikan.")


if __name__ == "__main__":
    main()
