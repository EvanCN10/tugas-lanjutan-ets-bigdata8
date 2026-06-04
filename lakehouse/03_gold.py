"""
03_gold.py - Agregasi & Enhanced Analysis ke Gold Layer Delta tables.
====================================================================
Kelompok 8 - Big Data dan Data Lakehouse

Script ini melakukan:
1. Membaca data dari Silver Delta tables (API dan RSS).
2. Membentuk dan menyimpan 4 Gold Delta tables:
   - `gold/pangan_volatility` (Repro): Volatilitas harga (Max, Min, Avg, Volatility %)
   - `gold/pangan_trend` (Repro): Rata-rata harga per periode menit/jam.
   - `gold/pangan_alert` (Enhanced): Deteksi fluktuasi harga (> 5% kenaikan atau < -5% penurunan dibanding observasi sebelumnya) menggunakan Window Functions.
   - `gold/pangan_news_correlation` (Enhanced): Cross-source join untuk menganalisis frekuensi penyebutan komoditas di berita RSS vs perubahan harga di API.
3. Mengekspor hasil agregasi ke `dashboard/data/spark_results.json` untuk fallback kompatibilitas dashboard lama.
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Driver/Worker Python version alignment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_JVM_STARTUP_TIMEOUT'] = '600'
os.environ['SPARK_AUTH_SOCKET_TIMEOUT'] = '600'

# Configure HADOOP_HOME for Windows to use local mock winutils
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
hadoop_home = os.path.abspath(os.path.join(BASE_DIR, ".hadoop"))
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession, Window
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, lag, when, avg, max, min, count, date_format, to_timestamp, explode, concat_ws, lower, array, array_compact, lit

import socket

SILVER_API_PATH = os.path.join(BASE_DIR, "lakehouse_data", "silver", "pangan_api")
SILVER_RSS_PATH = os.path.join(BASE_DIR, "lakehouse_data", "silver", "pangan_rss")

GOLD_VOLATILITY_PATH = os.path.join(BASE_DIR, "lakehouse_data", "gold", "pangan_volatility")
GOLD_TREND_PATH = os.path.join(BASE_DIR, "lakehouse_data", "gold", "pangan_trend")
GOLD_ALERT_PATH = os.path.join(BASE_DIR, "lakehouse_data", "gold", "pangan_alert")
GOLD_NEWS_PATH = os.path.join(BASE_DIR, "lakehouse_data", "gold", "pangan_news_correlation")

# Convert local paths to valid URIs for Spark
SILVER_API_URI = "file:///" + SILVER_API_PATH.replace("\\", "/")
SILVER_RSS_URI = "file:///" + SILVER_RSS_PATH.replace("\\", "/")

GOLD_VOLATILITY_URI = "file:///" + GOLD_VOLATILITY_PATH.replace("\\", "/")
GOLD_TREND_URI = "file:///" + GOLD_TREND_PATH.replace("\\", "/")
GOLD_ALERT_URI = "file:///" + GOLD_ALERT_PATH.replace("\\", "/")
GOLD_NEWS_URI = "file:///" + GOLD_NEWS_PATH.replace("\\", "/")

SPARK_RESULTS_JSON = os.path.join(BASE_DIR, "dashboard", "data", "spark_results.json")

HDFS_HOST = "localhost"
HDFS_PORT = 8020

def is_hdfs_reachable(host, port):
    """Mengecek apakah port RPC HDFS terbuka untuk koneksi."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3.0)
        s.connect((host, port))
        s.close()
        return True
    except Exception:
        return False

# Keywords Map untuk korelasi berita
KORELASI_NEWS_KEYWORDS = {
    "Beras": ["beras"],
    "Jagung": ["jagung"],
    "Kedelai": ["kedelai"],
    "Gula Pasir": ["gula", "gula pasir"],
    "Minyak Goreng": ["minyak goreng", "minyak"],
    "Cabai Merah": ["cabai", "cabai merah"],
    "Bawang Merah": ["bawang merah", "bawang"],
    "Telur Ayam": ["telur", "telur ayam"],
}

# UDF removed to prevent Python 3.14 cloudpickle recursion stack overflow.
# Commodity matching is now implemented using pure Spark SQL native functions (array_compact, array, and contains).

def build_spark_session(use_hdfs=False):
    builder = SparkSession.builder \
        .appName("Gold-Aggregation-Pangan") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g")
    
    if use_hdfs:
        hdfs_uri = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"
        builder.config("spark.hadoop.fs.defaultFS", hdfs_uri)
        print(f"[CONFIG] Menggunakan HDFS sebagai Default FS: {hdfs_uri}")
        
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark._jvm.org.apache.logging.log4j.core.config.Configurator.setLevel("org.apache.spark.util.ShutdownHookManager", spark._jvm.org.apache.logging.log4j.Level.OFF)
    except Exception:
        pass
    return spark

def build_gold_layer():
    global HDFS_HOST
    print("=" * 70)
    print("        Gold Layer: Aggregating Silver -> Gold Delta Tables")
    print("=" * 70)

    # 1. Deteksi ketersediaan HDFS
    hdfs_active = is_hdfs_reachable(HDFS_HOST, HDFS_PORT)
    if not hdfs_active:
        hdfs_active = is_hdfs_reachable("namenode", 8020)
        if hdfs_active:
            HDFS_HOST = "namenode"

    print(f"[STATUS] HDFS Konektivitas: {'AKTIF' if hdfs_active else 'NON-AKTIF / TIDAK TERJANGKAU'}")

    spark = build_spark_session(use_hdfs=hdfs_active)

    # Pastikan data Silver API ada
    silver_api = None
    if hdfs_active:
        try:
            hdfs_silver_api_uri = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/lakehouse/silver/pangan_api"
            print(f"[HDFS] Membaca Silver API dari HDFS: {hdfs_silver_api_uri}")
            silver_api = spark.read.format("delta").load(hdfs_silver_api_uri)
        except Exception as e:
            print(f"[WARN] Gagal membaca Silver API dari HDFS: {e}. Fallback ke lokal...")

    if silver_api is None:
        if not os.path.exists(SILVER_API_PATH):
            print(f"[ERROR] Silver API path tidak ditemukan: {SILVER_API_PATH}. Jalankan 02_silver.py dulu.")
            spark.stop()
            return
        silver_api = spark.read.format("delta").load(SILVER_API_URI)

    print(f"[READ] Memuat {silver_api.count()} baris data dari Silver API.")

    # 1. Gold Volatility (Repro)
    print("\n--- 1. Generating Gold Volatility Table ---")
    gold_volatility = silver_api.groupBy("komoditas").agg(
        max("harga").alias("harga_max"),
        min("harga").alias("harga_min"),
        avg("harga").alias("harga_avg"),
        count("harga").alias("jumlah_data")
    ).withColumn("volatilitas_pct", 
                ((col("harga_max") - col("harga_min")) / col("harga_min")) * 100)
    
    # Rounding untuk keindahan data
    gold_volatility = gold_volatility.select(
        col("komoditas"),
        col("harga_max").cast("double"),
        col("harga_min").cast("double"),
        col("harga_avg").cast("double"),
        col("jumlah_data").cast("int"),
        col("volatilitas_pct").cast("double")
    )
    print(f"[WRITE] Menyimpan ke Gold Volatility Delta: {GOLD_VOLATILITY_URI}")
    gold_volatility.write.format("delta").mode("overwrite").save(GOLD_VOLATILITY_URI)
    
    if hdfs_active:
        try:
            hdfs_gold_volatility_uri = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/lakehouse/gold/pangan_volatility"
            print(f"[WRITE HDFS] Menyimpan ke Gold Volatility Delta HDFS: {hdfs_gold_volatility_uri}")
            gold_volatility.write.format("delta").mode("overwrite").save(hdfs_gold_volatility_uri)
        except Exception as e:
            print(f"[ERROR HDFS] Gagal menyimpan Gold Volatility ke HDFS: {e}")

    gold_volatility.show()

    # 2. Gold Trend per Periode (Repro)
    print("\n--- 2. Generating Gold Trend Table ---")
    # Format timestamp ke yyyy-MM-dd HH:mm (tren per menit sesuai ETS)
    gold_trend = silver_api \
        .withColumn("periode", date_format("timestamp", "yyyy-MM-dd HH:mm")) \
        .groupBy("komoditas", "periode") \
        .agg(avg("harga").alias("harga_rata")) \
        .orderBy("periode", "komoditas")
    
    print(f"[WRITE] Menyimpan ke Gold Trend Delta: {GOLD_TREND_URI}")
    gold_trend.write.format("delta").mode("overwrite").save(GOLD_TREND_URI)
    
    if hdfs_active:
        try:
            hdfs_gold_trend_uri = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/lakehouse/gold/pangan_trend"
            print(f"[WRITE HDFS] Menyimpan ke Gold Trend Delta HDFS: {hdfs_gold_trend_uri}")
            gold_trend.write.format("delta").mode("overwrite").save(hdfs_gold_trend_uri)
        except Exception as e:
            print(f"[ERROR HDFS] Gagal menyimpan Gold Trend ke HDFS: {e}")

    gold_trend.show(5)

    # 3. Gold Alert (Enhanced - Window Function)
    print("\n--- 3. Generating Gold Alert (Price Alert) Table ---")
    window_spec = Window.partitionBy("komoditas").orderBy("timestamp")
    
    # Deteksi fluktuasi harga > 5% dibanding observasi sebelumnya
    gold_alert = silver_api \
        .withColumn("prev_harga", lag("harga", 1).over(window_spec)) \
        .withColumn("pct_change", ((col("harga") - col("prev_harga")) / col("prev_harga")) * 100) \
        .withColumn("alert", when(col("pct_change") > 5, "⚠️ NAIK SIGNIFIKAN")
                              .when(col("pct_change") < -5, "📉 TURUN SIGNIFIKAN")
                              .otherwise("Normal")) \
        .filter(col("alert") != "Normal") \
        .select("komoditas", "harga", "prev_harga", "pct_change", "alert", date_format("timestamp", "yyyy-MM-dd HH:mm:ss").alias("timestamp"))
    
    print(f"[WRITE] Menyimpan ke Gold Alert Delta: {GOLD_ALERT_URI}")
    gold_alert.write.format("delta").mode("overwrite").save(GOLD_ALERT_URI)
    
    if hdfs_active:
        try:
            hdfs_gold_alert_uri = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/lakehouse/gold/pangan_alert"
            print(f"[WRITE HDFS] Menyimpan ke Gold Alert Delta HDFS: {hdfs_gold_alert_uri}")
            gold_alert.write.format("delta").mode("overwrite").save(hdfs_gold_alert_uri)
        except Exception as e:
            print(f"[ERROR HDFS] Gagal menyimpan Gold Alert ke HDFS: {e}")

    print(f"[ALERT] {gold_alert.count()} fluktuasi signifikan terdeteksi.")
    gold_alert.show(5)

    # 4. Gold News Correlation (Enhanced - Cross-Source Join)
    print("\n--- 4. Generating Gold News Correlation Table ---")
    silver_rss = None
    if hdfs_active:
        try:
            hdfs_silver_rss_uri = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/lakehouse/silver/pangan_rss"
            print(f"[HDFS] Membaca Silver RSS dari HDFS: {hdfs_silver_rss_uri}")
            silver_rss = spark.read.format("delta").load(hdfs_silver_rss_uri)
        except Exception as e:
            print(f"[WARN] Gagal membaca Silver RSS dari HDFS: {e}. Fallback ke lokal...")
            
    if silver_rss is None and os.path.exists(SILVER_RSS_PATH):
        silver_rss = spark.read.format("delta").load(SILVER_RSS_URI)

    if silver_rss is not None:
        # Ekstrak penyebutan berita per komoditas secara native (JVM-only execution)
        from functools import reduce
        text_col = lower(concat_ws(" ", col("title"), col("summary")))
        match_exprs = []
        for kom, keys in KORELASI_NEWS_KEYWORDS.items():
            key_cond = reduce(lambda a, b: a | b, [text_col.contains(k) for k in keys])
            match_exprs.append(when(key_cond, lit(kom)).otherwise(lit(None).cast("string")))
        
        matched_array = array_compact(array(*match_exprs))
        exploded_news = silver_rss.withColumn("komoditas", explode(matched_array))
        
        news_count = exploded_news.groupBy("komoditas").count().withColumnRenamed("count", "frekuensi_berita")
        
        # Ambil rata-rata perubahan persen dari API
        api_chg = silver_api.groupBy("komoditas").agg(avg("perubahan_persen").alias("avg_perubahan_persen"))
        
        # Join data news dan api
        gold_news_correlation = news_count.join(api_chg, "komoditas", "outer").fillna(0)
        gold_news_correlation = gold_news_correlation.select(
            col("komoditas"),
            col("frekuensi_berita").cast("int"),
            col("avg_perubahan_persen").cast("double")
        )
    else:
        print("[WARN] Silver RSS tidak ditemukan. Menggunakan tabel kosong untuk korelasi berita.")
        # Fallback empty df using pure JVM Spark SQL (completely Python 3.14 safe)
        gold_news_correlation = spark.sql(
            "SELECT CAST(NULL AS STRING) as komoditas, CAST(NULL AS INT) as frekuensi_berita, CAST(NULL AS DOUBLE) as avg_perubahan_persen"
        ).filter("1 = 0")

    print(f"[WRITE] Menyimpan ke Gold News Delta: {GOLD_NEWS_URI}")
    gold_news_correlation.write.format("delta").mode("overwrite").save(GOLD_NEWS_URI)
    
    if hdfs_active:
        try:
            hdfs_gold_news_uri = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/lakehouse/gold/pangan_news_correlation"
            print(f"[WRITE HDFS] Menyimpan ke Gold News Delta HDFS: {hdfs_gold_news_uri}")
            gold_news_correlation.write.format("delta").mode("overwrite").save(hdfs_gold_news_uri)
        except Exception as e:
            print(f"[ERROR HDFS] Gagal menyimpan Gold News ke HDFS: {e}")

    gold_news_correlation.show()

    # --- 5. EKSPOR HASIL KE spark_results.json UNTUK DASHBOARD ---
    print("\n--- 5. Exporting Results to Dashboard JSON ---")
    
    # Load MLlib results dari JSON yang sudah ada agar tidak hilang
    existing_mllib = []
    mllib_gen_at = datetime.now().isoformat()
    if os.path.exists(SPARK_RESULTS_JSON):
        try:
            with open(SPARK_RESULTS_JSON, "r", encoding="utf-8") as f:
                blob = json.load(f)
                existing_mllib = blob.get("prediksi_mlllib", [])
                mllib_gen_at = blob.get("mllib_generated_at", mllib_gen_at)
        except Exception:
            pass

    # Convert Spark DataFrames to Python lists/dicts
    vol_list = [row.asDict() for row in gold_volatility.collect()]
    trend_list = [row.asDict() for row in gold_trend.collect()]
    news_list = [row.asDict() for row in gold_news_correlation.collect()]
    alert_list = [row.asDict() for row in gold_alert.collect()]

    # Format JSON
    now_iso = datetime.now().isoformat()
    output_data = {
        "generated_at": now_iso,
        "volatilitas": vol_list,
        "tren_harga": trend_list,
        "korelasi_berita": news_list,
        "alert_harga": alert_list,
        "prediksi_mlllib": existing_mllib,
        "mllib_generated_at": mllib_gen_at
    }

    os.makedirs(os.path.dirname(SPARK_RESULTS_JSON), exist_ok=True)
    with open(SPARK_RESULTS_JSON, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"[SUCCESS] JSON file berhasil diekspor ke: {SPARK_RESULTS_JSON}")

    spark.stop()
    print("\n" + "=" * 70)
    print("              Gold Layer Aggregation Selesai")
    print("=" * 70)

if __name__ == "__main__":
    build_gold_layer()
