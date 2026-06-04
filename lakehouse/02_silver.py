"""
02_silver.py - Cleaning & Transformation dari Bronze ke Silver Layer + Time Travel Demo.
========================================================================================
Kelompok 8 - Big Data dan Data Lakehouse

Script ini melakukan:
1. Membaca data dari Bronze Delta tables (API dan RSS).
2. Melakukan 3+ data cleaning & transformasi:
   - Deduplikasi data berdasarkan kolom unik.
   - Type casting: cast string timestamp -> TimestampType, harga -> DoubleType.
   - Filter null atau data tidak valid (harga <= 0).
   - Ekstraksi kolom: jam (hour) dan tanggal (date).
   - Standarisasi nilai komoditas (Title case).
3. Menyimpan hasil ke Silver Delta tables:
   - `./lakehouse_data/silver/pangan_api`
   - `./lakehouse_data/silver/pangan_rss`
4. Demonstrasi TIME TRAVEL Delta Lake:
   - Menampilkan history tabel.
   - Melakukan update nilai data secara atomik.
   - Membandingkan data versi sekarang dengan versi lama (versionAsOf 0).
"""

import os
import sys
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

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_timestamp, hour, to_date, when, lit, upper

BRONZE_API_PATH = os.path.join(BASE_DIR, "lakehouse_data", "bronze", "pangan_api")
BRONZE_RSS_PATH = os.path.join(BASE_DIR, "lakehouse_data", "bronze", "pangan_rss")
SILVER_API_PATH = os.path.join(BASE_DIR, "lakehouse_data", "silver", "pangan_api")
SILVER_RSS_PATH = os.path.join(BASE_DIR, "lakehouse_data", "silver", "pangan_rss")

# Convert local paths to valid URIs for Spark
BRONZE_API_URI = "file:///" + BRONZE_API_PATH.replace("\\", "/")
BRONZE_RSS_URI = "file:///" + BRONZE_RSS_PATH.replace("\\", "/")
SILVER_API_URI = "file:///" + SILVER_API_PATH.replace("\\", "/")
SILVER_RSS_URI = "file:///" + SILVER_RSS_PATH.replace("\\", "/")

def build_spark_session():
    builder = SparkSession.builder \
        .appName("Silver-Transformation-Pangan") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g")
    
    # Deteksi jika defaultFS diset ke HDFS di script ingestion sebelumnya
    # Supaya pembacaan file lokal aman, kita tidak memaksa defaultFS HDFS di session lokal ini jika hanya memproses Delta lokal.
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark._jvm.org.apache.logging.log4j.core.config.Configurator.setLevel("org.apache.spark.util.ShutdownHookManager", spark._jvm.org.apache.logging.log4j.Level.OFF)
    except Exception:
        pass
    return spark

def clean_and_transform():
    print("=" * 70)
    print("        Silver Layer: Transforming Bronze -> Silver")
    print("=" * 70)

    spark = build_spark_session()

    # --- 1. PROSES API SILVER ---
    print("\n--- Transforming API Data ---")
    if not os.path.exists(BRONZE_API_PATH):
        print(f"[ERROR] Bronze API path tidak ditemukan: {BRONZE_API_PATH}. Jalankan 01_bronze.py dulu.")
        spark.stop()
        return

    bronze_api = spark.read.format("delta").load(BRONZE_API_URI)
    initial_api_count = bronze_api.count()
    print(f"[READ] Berhasil membaca {initial_api_count} baris dari Bronze API.")

    # Transformasi Cleaning
    silver_api = bronze_api \
        .dropDuplicates(["timestamp", "komoditas", "harga"]) \
        .filter(col("harga").isNotNull() & (col("harga") > 0)) \
        .withColumn("harga", col("harga").cast("double")) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("jam", hour(col("timestamp"))) \
        .withColumn("tanggal", to_date(col("timestamp"))) \
        .withColumn("komoditas", when(col("komoditas") == "Beras Medium", "Beras")
                                 .when(col("komoditas") == "beras", "Beras")
                                 .otherwise(col("komoditas")))

    final_api_count = silver_api.count()
    print(f"[TRANSFORM] Cleaning selesai. Baris berkurang dari {initial_api_count} -> {final_api_count} (Selisih: {initial_api_count - final_api_count} baris duplikat/invalid dibersihkan).")
    
    print(f"[WRITE] Menyimpan ke Silver API Delta: {SILVER_API_URI}")
    silver_api.write.format("delta").mode("overwrite").save(SILVER_API_URI)
    silver_api.show(5)

    # --- 2. PROSES RSS SILVER ---
    print("\n--- Transforming RSS Data ---")
    if os.path.exists(BRONZE_RSS_PATH):
        bronze_rss = spark.read.format("delta").load(BRONZE_RSS_URI)
        initial_rss_count = bronze_rss.count()
        print(f"[READ] Berhasil membaca {initial_rss_count} baris dari Bronze RSS.")

        # Transformasi RSS
        silver_rss = bronze_rss \
            .dropDuplicates(["title", "published"]) \
            .filter(col("title").isNotNull() & (col("title") != "")) \
            .withColumn("published_ts", to_timestamp(col("published"))) \
            .withColumn("source", upper(col("source")))

        final_rss_count = silver_rss.count()
        print(f"[TRANSFORM] RSS Cleaning selesai: {initial_rss_count} -> {final_rss_count} baris.")
        
        print(f"[WRITE] Menyimpan ke Silver RSS Delta: {SILVER_RSS_URI}")
        silver_rss.write.format("delta").mode("overwrite").save(SILVER_RSS_URI)
        silver_rss.show(5)
    else:
        print("[WARN] Bronze RSS path tidak ditemukan, skip cleaning RSS.")

    # --- 3. DEMONSTRASI TIME TRAVEL DELTA LAKE ---
    print("\n" + "=" * 50)
    print("      DEMONSTRASI TIME TRAVEL DELTA LAKE")
    print("=" * 50)
    
    deltaTable = DeltaTable.forPath(spark, SILVER_API_URI)

    # A. Lihat history tabel
    print("\n[TIME TRAVEL 1] Menampilkan History Operasi Tabel:")
    deltaTable.history().select("version", "timestamp", "operation", "userName").show(truncate=False)

    # B. Lakukan update nilai (simulasi perubahan data)
    print("[TIME TRAVEL 2] Melakukan Koreksi Data (Simulasi penambahan Rp 100 untuk semua harga > 0)")
    deltaTable.update(
    condition="harga > 0",
    set={"harga": col("harga") + 100}
    )

    # C. Bandingkan versi sekarang dengan versi 0
    print("\n[TIME TRAVEL 3] Membandingkan data SEKARANG vs data VERSI 0 (Sebelum Update)")
    
    print("\n--- Data SEKARANG (Setelah Update) ---")
    spark.read.format("delta").load(SILVER_API_URI) \
        .select("komoditas", "harga", "timestamp") \
        .orderBy("komoditas") \
        .show(10)

    print("--- Data VERSI 0 (Sebelum Update) ---")
    spark.read.format("delta").option("versionAsOf", 0).load(SILVER_API_URI) \
        .select("komoditas", "harga", "timestamp") \
        .orderBy("komoditas") \
        .show(10)

    spark.stop()
    print("\n" + "=" * 70)
    print("              Silver Layer & Time Travel Selesai")
    print("=" * 70)

if __name__ == "__main__":
    clean_and_transform()
