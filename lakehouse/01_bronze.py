"""
01_bronze.py - Ingest data dari HDFS (atau Local JSON) ke Bronze Delta Lake Layer.
=================================================================================
Kelompok 8 - Big Data dan Data Lakehouse

Script ini bertugas untuk:
1. Membaca data mentah dari HDFS (/data/pangan/api/ dan /data/pangan/rss/).
2. Jika HDFS tidak aktif/tidak dapat diakses, melakukan fallback dengan membaca file JSON lokal.
3. Menambahkan kolom metadata:
   - `_ingested_at` (waktu ingest saat ini)
   - `_source` (sumber data: 'api' atau 'rss')
4. Menyimpan data dalam format Delta Lake (Bronze Layer) di:
   - `./lakehouse_data/bronze/pangan_api`
   - `./lakehouse_data/bronze/pangan_rss`
"""

import os
import sys
import socket
from datetime import datetime
from pathlib import Path

# Driver/Worker Python version alignment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_JVM_STARTUP_TIMEOUT'] = '600'
os.environ['SPARK_AUTH_SOCKET_TIMEOUT'] = '600'

# Configure HADOOP_HOME for Windows to use local mock winutils
hadoop_home = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".hadoop"))
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp, lit

# --- Configuration & Paths ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DASHBOARD_DATA_DIR = os.path.join(BASE_DIR, "dashboard", "data")
LOCAL_API_PATH = os.path.join(DASHBOARD_DATA_DIR, "live_api.json")
LOCAL_RSS_PATH = os.path.join(DASHBOARD_DATA_DIR, "live_rss.json")

BRONZE_API_TARGET = os.path.join(BASE_DIR, "lakehouse_data", "bronze", "pangan_api")
BRONZE_RSS_TARGET = os.path.join(BASE_DIR, "lakehouse_data", "bronze", "pangan_rss")

HDFS_HOST = "localhost"  # Default host jika dijalankan dari Windows host ke docker port mapping
HDFS_PORT = 8020
HDFS_API_URI = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/data/pangan/api/"
HDFS_RSS_URI = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/data/pangan/rss/"

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

def build_spark_session(use_hdfs=False):
    """Inisialisasi SparkSession dengan Delta Lake."""
    builder = SparkSession.builder \
        .appName("Bronze-Ingestion-Pangan") \
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

def ingest_layer():
    global HDFS_HOST, HDFS_API_URI, HDFS_RSS_URI
    print("=" * 70)
    print("        Bronze Layer Ingestion: Raw Data -> Delta Lake Bronze")
    print("=" * 70)

    # 1. Deteksi ketersediaan HDFS
    hdfs_active = is_hdfs_reachable(HDFS_HOST, HDFS_PORT)
    # Coba juga deteksi hostname docker internal "namenode" jika dijalankan di container Spark
    if not hdfs_active:
        hdfs_active = is_hdfs_reachable("namenode", 8020)
        if hdfs_active:
            HDFS_HOST = "namenode"
            HDFS_API_URI = f"hdfs://namenode:8020/data/pangan/api/"
            HDFS_RSS_URI = f"hdfs://namenode:8020/data/pangan/rss/"

    print(f"[STATUS] HDFS Konektivitas: {'AKTIF' if hdfs_active else 'NON-AKTIF / TIDAK TERJANGKAU'}")
    
    # Inisialisasi Spark
    spark = build_spark_session(use_hdfs=hdfs_active)
    
    # 2. Ingest API Data
    print("\n--- Ingesting API Data ---")
    api_df = None
    if hdfs_active:
        try:
            print(f"[HDFS] Membaca JSON lines dari: {HDFS_API_URI}")
            api_df = spark.read.json(HDFS_API_URI)
            print(f"[HDFS] Berhasil memuat {api_df.count()} baris data dari HDFS.")
        except Exception as e:
            print(f"[WARN] Gagal membaca dari HDFS: {e}. Melakukan fallback ke lokal...")
            hdfs_active = False

    if not hdfs_active or api_df is None:
        if os.path.exists(LOCAL_API_PATH):
            local_api_uri = "file:///" + LOCAL_API_PATH.replace("\\", "/")
            print(f"[LOKAL] Membaca JSON array dari file lokal: {local_api_uri}")
            api_df = spark.read.option("multiLine", True).json(local_api_uri)
            print(f"[LOKAL] Berhasil memuat {api_df.count()} baris data dari file lokal.")
        else:
            print(f"[ERROR] File lokal {LOCAL_API_PATH} tidak ditemukan. Ingest API dibatalkan.")

    if api_df is not None:
        # Tambah metadata kolom
        bronze_api_df = api_df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit("api"))
        
        # Simpan ke Delta format (Bronze)
        bronze_api_target_uri = "file:///" + BRONZE_API_TARGET.replace("\\", "/")
        print(f"[WRITE] Menyimpan ke Bronze Delta: {bronze_api_target_uri}")
        bronze_api_df.write.format("delta").mode("append").save(bronze_api_target_uri)
        print(f"[SUCCESS] Ingest API selesai. Data tersimpan di {BRONZE_API_TARGET}")
        
        # Simpan ke HDFS jika aktif
        if hdfs_active:
            try:
                hdfs_api_target_uri = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/lakehouse/bronze/pangan_api"
                print(f"[WRITE HDFS] Menyimpan ke Bronze Delta HDFS: {hdfs_api_target_uri}")
                bronze_api_df.write.format("delta").mode("append").save(hdfs_api_target_uri)
                print(f"[SUCCESS HDFS] Data API tersimpan di HDFS: {hdfs_api_target_uri}")
            except Exception as e:
                print(f"[ERROR HDFS] Gagal menyimpan API ke HDFS: {e}")
        
        bronze_api_df.show(5, truncate=False)
    
    # 3. Ingest RSS Data
    print("\n--- Ingesting RSS Data ---")
    rss_df = None
    if hdfs_active:
        try:
            print(f"[HDFS] Membaca JSON lines dari: {HDFS_RSS_URI}")
            rss_df = spark.read.json(HDFS_RSS_URI)
            print(f"[HDFS] Berhasil memuat {rss_df.count()} baris data RSS dari HDFS.")
        except Exception as e:
            print(f"[WARN] Gagal membaca RSS dari HDFS: {e}. Melakukan fallback ke lokal...")
            hdfs_active = False

    if not hdfs_active or rss_df is None:
        if os.path.exists(LOCAL_RSS_PATH):
            local_rss_uri = "file:///" + LOCAL_RSS_PATH.replace("\\", "/")
            print(f"[LOKAL] Membaca JSON array dari file lokal: {local_rss_uri}")
            rss_df = spark.read.option("multiLine", True).json(local_rss_uri)
            print(f"[LOKAL] Berhasil memuat {rss_df.count()} baris data RSS dari file lokal.")
        else:
            print(f"[ERROR] File lokal {LOCAL_RSS_PATH} tidak ditemukan. Ingest RSS dibatalkan.")

    if rss_df is not None:
        # Tambah metadata kolom
        bronze_rss_df = rss_df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit("rss"))
        
        # Simpan ke Delta format (Bronze)
        bronze_rss_target_uri = "file:///" + BRONZE_RSS_TARGET.replace("\\", "/")
        print(f"[WRITE] Menyimpan ke Bronze Delta: {bronze_rss_target_uri}")
        bronze_rss_df.write.format("delta").mode("append").save(bronze_rss_target_uri)
        print(f"[SUCCESS] Ingest RSS selesai. Data tersimpan di {BRONZE_RSS_TARGET}")
        
        # Simpan ke HDFS jika aktif
        if hdfs_active:
            try:
                hdfs_rss_target_uri = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/lakehouse/bronze/pangan_rss"
                print(f"[WRITE HDFS] Menyimpan ke Bronze Delta HDFS: {hdfs_rss_target_uri}")
                bronze_rss_df.write.format("delta").mode("append").save(hdfs_rss_target_uri)
                print(f"[SUCCESS HDFS] Data RSS tersimpan di HDFS: {hdfs_rss_target_uri}")
            except Exception as e:
                print(f"[ERROR HDFS] Gagal menyimpan RSS ke HDFS: {e}")
        
        bronze_rss_df.show(5, truncate=False)

    spark.stop()
    print("\n" + "=" * 70)
    print("              Bronze Layer Ingestion Selesai")
    print("=" * 70)

if __name__ == "__main__":
    ingest_layer()
