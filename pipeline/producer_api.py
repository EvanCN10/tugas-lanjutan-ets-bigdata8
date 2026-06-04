# [Anggota2]: Producer Simulator Harga Komoditas Pangan

import json
import time
import random
import math
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
try:
    import schedule
except ModuleNotFoundError as e:
    raise SystemExit(
        "Dependency 'schedule' belum ter-install di Python environment yang aktif. "
        "Jalankan: pip install -r requirements.txt"
    ) from e
from tenacity import retry, wait_exponential, stop_after_attempt

# ============================================================
# KONFIGURASI KOMODITAS
# Harga baseline berdasarkan data BULOG / BPS terkini
# ============================================================
KOMODITAS_CONFIG = {
    "Beras":     {"harga_dasar": 13500, "std": 200,  "unit": "kg"},
    "Jagung":           {"harga_dasar": 6500,  "std": 150,  "unit": "kg"},
    "Kedelai":          {"harga_dasar": 14000, "std": 300,  "unit": "kg"},
    "Gula Pasir":       {"harga_dasar": 16500, "std": 250,  "unit": "kg"},
    "Minyak Goreng":    {"harga_dasar": 19000, "std": 400,  "unit": "liter"},
    "Cabai Merah":      {"harga_dasar": 45000, "std": 3000, "unit": "kg"},
    "Bawang Merah":     {"harga_dasar": 38000, "std": 2500, "unit": "kg"},
    "Telur Ayam":       {"harga_dasar": 29000, "std": 500,  "unit": "kg"},
}

# State harga saat ini (untuk random walk)
current_prices = {k: v["harga_dasar"] for k, v in KOMODITAS_CONFIG.items()}
prev_prices = dict(current_prices)

def simulate_price_movement(komoditas):
    """
    Simulasi pergerakan harga menggunakan Random Walk dengan Mean Reversion.
    Harga bergerak acak tapi selalu balik ke baseline (tidak lari ke infinity).
    """
    config = KOMODITAS_CONFIG[komoditas]
    harga_dasar = config["harga_dasar"]
    std = config["std"]
    
    # Random walk component
    random_change = random.gauss(0, std * 0.1)
    
    # Mean reversion: tarik balik ke harga dasar
    current = current_prices[komoditas]
    reversion = (harga_dasar - current) * 0.05
    
    # Update harga
    new_price = current + random_change + reversion
    
    # Pastikan tidak negatif dan tidak terlalu jauh dari baseline
    min_price = harga_dasar * 0.7
    max_price = harga_dasar * 1.5
    new_price = max(min_price, min(max_price, new_price))
    
    return round(new_price, 0)

def calculate_change_pct(old_price, new_price):
    """Hitung persentase perubahan harga"""
    if old_price == 0:
        return 0.0
    return round((new_price - old_price) / old_price * 100, 4)

def build_event(komoditas, harga_baru):
    """Build JSON event untuk dikirim ke Kafka"""
    config = KOMODITAS_CONFIG[komoditas]
    harga_lama = prev_prices[komoditas]
    
    return {
        "komoditas": komoditas,
        "harga": harga_baru,
        "harga_sebelumnya": harga_lama,
        "perubahan_persen": calculate_change_pct(harga_lama, harga_baru),
        "harga_baseline": config["harga_dasar"],
        "unit": config["unit"],
        "timestamp": datetime.utcnow().isoformat(),
        "sumber": "simulator_bulog_baseline",
        "kota": "Nasional"
    }

iteration = 0

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def send_to_kafka(producer, topic, key, event):
    producer.send(
        topic=topic,
        key=key,
        value=event
    )

def job(producer):
    global iteration
    iteration += 1
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Iteration #{iteration} (Scheduler)")
    
    for komoditas in KOMODITAS_CONFIG.keys():
        # Hitung harga baru
        harga_baru = simulate_price_movement(komoditas)
        
        # Build event
        event = build_event(komoditas, harga_baru)
        
        # Update state
        prev_prices[komoditas] = current_prices[komoditas]
        current_prices[komoditas] = harga_baru
        
        # Kirim ke Kafka dengan retry logic
        try:
            send_to_kafka(producer, 'pangan-api', komoditas, event)
            # Log
            change = event['perubahan_persen']
            arrow = "▲" if change > 0 else "▼" if change < 0 else "—"
            print(f"  {arrow} {komoditas:20s}: Rp {harga_baru:>8,.0f} ({change:+.2f}%)")
        except Exception as e:
            print(f"  ❌ Gagal kirim {komoditas} ke Kafka setelah retries: {e}")
    
    try:
        producer.flush()
        print(f"✅ {len(KOMODITAS_CONFIG)} events sent to pangan-api")
    except Exception as e:
        print(f"❌ Error during flush: {e}")


def main():
    print("=" * 60)
    print("🚀 Producer HargaPangan started")
    print(f"📦 Memantau {len(KOMODITAS_CONFIG)} komoditas")
    print(f"⏱️  Scheduler Interval: 30 detik")
    print("=" * 60)
    
    try:
        import os
        bootstrap_srv = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_srv],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8'),
            acks='all',
            retries=5
        )
    except KafkaError as e:
        print(f"Gagal inisialisasi Kafka Producer: {e}")
        return
        
    # Run immediately first
    job(producer)
    
    # Schedule every 30 seconds
    schedule.every(30).seconds.do(job, producer)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n✋ Dihentikan.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
