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

SILVER_API_PATH = os.path.join(BASE_DIR, "lakehouse_data", "silver", "pangan_api")
SILVER_RSS_PATH = os.path.join(BASE_DIR, "lakehouse_data", "silver", "pangan_rss")

GOLD_VOLATILITY_PATH = os.path.join(BASE_DIR, "lakehouse_data", "gold", "pangan_volatility")
GOLD_TREND_PATH = os.path.join(BASE_DIR, "lakehouse_data", "gold", "pangan_trend")
GOLD_ALERT_PATH = os.path.join(BASE_DIR, "lakehouse_data", "gold", "pangan_alert")
GOLD_NEWS_PATH = os.path.join(BASE_DIR, "lakehouse_data", "gold", "pangan_news_correlation")

# Convert local paths to valid URIs for Spark
SILVER_API_URI = Path(SILVER_API_PATH).as_uri()
SILVER_RSS_URI = Path(SILVER_RSS_PATH).as_uri()

GOLD_VOLATILITY_URI = Path(GOLD_VOLATILITY_PATH).as_uri()
GOLD_TREND_URI = Path(GOLD_TREND_PATH).as_uri()
GOLD_ALERT_URI = Path(GOLD_ALERT_PATH).as_uri()
GOLD_NEWS_URI = Path(GOLD_NEWS_PATH).as_uri()

SPARK_RESULTS_JSON = os.path.join(BASE_DIR, "dashboard", "data", "spark_results.json")

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

def build_spark_session():
    builder = SparkSession.builder \
        .appName("Gold-Aggregation-Pangan") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g")
    
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def build_gold_layer():
    print("=" * 70)
    print("        Gold Layer: Aggregating Silver -> Gold Delta Tables")
    print("=" * 70)

    spark = build_spark_session()

    # Pastikan data Silver API ada
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
        .select("komoditas", "harga", "prev_harga", "pct_change", "alert", "timestamp")
    
    print(f"[WRITE] Menyimpan ke Gold Alert Delta: {GOLD_ALERT_URI}")
    gold_alert.write.format("delta").mode("overwrite").save(GOLD_ALERT_URI)
    print(f"[ALERT] {gold_alert.count()} fluktuasi signifikan terdeteksi.")
    gold_alert.show(5)

    # 4. Gold News Correlation (Enhanced - Cross-Source Join)
    print("\n--- 4. Generating Gold News Correlation Table ---")
    if os.path.exists(SILVER_RSS_PATH):
        silver_rss = spark.read.format("delta").load(SILVER_RSS_URI)
        
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
    gold_news_correlation.show()

    # --- 5. EKSPOR HASIL KE spark_results.json UNTUK DASHBOARD FALLBACK ---
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

    # Format JSON
    now_iso = datetime.now().isoformat()
    output_data = {
        "generated_at": now_iso,
        "volatilitas": vol_list,
        "tren_harga": trend_list,
        "korelasi_berita": news_list,
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
