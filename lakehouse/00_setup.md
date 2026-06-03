# 00 Setup: Menjalankan Spark + Delta Lake (Medallion Architecture)

Dokumen ini berisi panduan untuk mengonfigurasi environment agar dapat menjalankan PySpark dengan ekstensi Delta Lake untuk memproses pipeline data Medallion (Bronze, Silver, Gold).

## 1. Prasyarat Sistem

Pastikan library python yang diperlukan sudah terinstal:

```bash
pip install -r requirements.txt
```

Atau instal secara manual:

```bash
pip install pyspark==3.5.1 delta-spark==3.1.0 deltalake>=0.15.0
```

> [!NOTE]
> PySpark versi `3.5.x` membutuhkan Delta Lake versi `3.1.0` atau `3.2.0` agar kompatibel secara penuh. Library `deltalake` adalah pustaka berbasis Rust yang cepat untuk membaca Delta tables secara langsung di Flask tanpa memerlukan JVM.

## 2. Struktur Konfigurasi SparkSession dengan Delta Lake

Untuk menjalankan skrip dengan Delta Lake, kita harus menyertakan konfigurasi Spark SQL extensions dan catalog class Delta.

Berikut adalah contoh inisialisasi di Python:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Medallion-Pangan") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "4")

# Mengonfigurasi SparkSession dengan package Delta Lake Maven
spark = configure_spark_with_delta_pip(
    builder, 
    extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
).getOrCreate()
```

## 3. Konfigurasi HDFS (Jika HDFS Aktif)

Jika cluster Hadoop HDFS berjalan (`docker compose -f docker-compose-hadoop.yml up -d`), tambahkan default file system ke konfigurasi:

```python
builder.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
```

## 4. Cara Menjalankan Pipeline secara Berurutan

Jalankan skrip-skrip berikut secara berurutan untuk memproses data dari mentah hingga agregat:

1. **Bronze Ingestion**: Mengambil data mentah dari HDFS/Lokal ke Bronze Delta Table.
   ```bash
   python lakehouse/01_bronze.py
   ```
2. **Silver Cleaning**: Membersihkan data, membuang duplikat/invalid, casting tipe data, dan demo Time Travel.
   ```bash
   python lakehouse/02_silver.py
   ```
3. **Gold Aggregation**: Membuat ringkasan analitik, fluktuasi harga (Window function), dan join berita (RSS + API) untuk dikonsumsi oleh Flask dashboard.
   ```bash
   python lakehouse/03_gold.py
   ```
