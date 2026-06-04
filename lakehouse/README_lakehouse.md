# README: Dokumentasi Pipeline Data Lakehouse (Medallion & Delta Lake)

Dokumentasi ini menjelaskan peningkatan arsitektur data pipeline **HargaPangan** dari model HDFS JSON mentah lama (ETS) menjadi arsitektur **Data Lakehouse** modern menggunakan **Medallion Architecture** dan **Delta Lake**.

---

## 1. Diagram Arsitektur: Sebelum vs Sesudah

### Arsitektur Lama (ETS - JSON Mentah)

```mermaid
graph LR
    subgraph Pengumpulan
        API[API Real-time] --> Kafka[Apache Kafka]
        RSS[RSS Feed] --> Kafka
    end

    subgraph Penyimpanan
        Kafka --> Consumer[HDFS Consumer]
        Consumer --> HDFS[HDFS /data/pangan/ api & rss JSON]
    end

    subgraph Analisis & Visualisasi
        HDFS --> Spark[Spark analysis.py]
        Spark --> SparkResults[spark_results.json]
        SparkResults --> Flask[Flask Dashboard]
    end
```

### Arsitektur Baru (Data Lakehouse - Medallion Architecture + Delta Lake)

```mermaid
graph TD
    subgraph Data Sources
        API[API Real-time]
        RSS[RSS Feed]
    end

    subgraph Ingestion & Streaming
        API --> Kafka[Apache Kafka]
        RSS --> Kafka
        Kafka --> HDFS[HDFS /data/pangan/ api & rss JSON]
    end

    subgraph Lakehouse Layer (Delta Lake)
        HDFS -->|"01_bronze.py"| Bronze["🥉 Bronze Layer (Raw Delta Table) <br/> pangan_api & pangan_rss <br/> + _ingested_at, _source"]
        
        Bronze -->|"02_silver.py (Clean, Cast, Deduplicate)"| Silver["🥈 Silver Layer (Cleaned Delta Table) <br/> pangan_api & pangan_rss <br/> Tipe data benar, tanpa duplikat/null"]
        
        Silver -->|"03_gold.py (Aggregate, Window, Join)"| Gold["🥇 Gold Layer (Aggregated Delta Table) <br/> pangan_volatility, pangan_trend, pangan_alert, pangan_news"]
    end

    subgraph Dashboard BI
        Gold -->|"deltalake Python / spark_results.json fallback"| Flask[Flask Dashboard Backend]
        Flask --> UI[Web UI Monitor]
    end
```

---

## 2. Struktur Folder & Skrip

Struktur repositori yang ditambahkan untuk tugas ini adalah sebagai berikut:

```text
lakehouse/
├── 00_setup.md             ← Panduan instalasi dan persiapan environment
├── 01_bronze.py            ← Ingestion dari HDFS/Lokal ke Bronze Delta Table
├── 02_silver.py            ← Cleaning dan standarisasi ke Silver Delta Table & Demo Time Travel
├── 03_gold.py              ← Agregasi analitik & Enhanced logic ke Gold Delta Table
└── README_lakehouse.md     ← Dokumentasi lengkap arsitektur (File ini)
```

---

## 3. Detail Implementasi Lapisan Medallion

### A. Bronze Layer (`01_bronze.py`)

Mengambil file JSON mentah dari HDFS `/data/pangan/api` dan `/data/pangan/rss/` (atau fallback file JSON lokal jika HDFS tidak aktif) lalu menyimpannya apa adanya ke format Delta Lake di `./lakehouse_data/bronze/pangan_api` dan `./lakehouse_data/bronze/pangan_rss`.

- **Kolom Metadata Tambahan**:
  1. `_ingested_at` (Timestamp): Waktu masuknya data ke Danau Data (Data Lake).
  2. `_source` (String): Asal data (`"api"` atau `"rss"`).

### B. Silver Layer (`02_silver.py`)

Membaca data dari Bronze, lalu melakukan transformasi pembersihan data (QC) sebelum disimpan ke `./lakehouse_data/silver/pangan_api` dan `./lakehouse_data/silver/pangan_rss`.

**Transformasi yang dilakukan:**

1. **Deduplikasi (Hapus Duplikat)**: Menggunakan `.dropDuplicates(["timestamp", "komoditas", "harga"])` pada data API dan `["title", "published"]` pada data RSS untuk menjamin integritas data (tidak ada data tercatat ganda pada waktu yang sama).
2. **Type Casting (Penyelarasan Tipe Data)**: Mengubah harga menjadi tipe `double` dan string timestamp menjadi `TimestampType` agar dapat diproses secara matematis dan temporal oleh Spark.
3. **Filter Data Invalid & Null**: Membuang baris yang memiliki harga `null` atau `harga <= 0` untuk menghindari skewing saat visualisasi tren.
4. **Ekstraksi Temporal**: Menambahkan kolom `jam` (`hour(col("timestamp"))`) dan `tanggal` (`to_date(col("timestamp"))`) untuk mempermudah analisis waktu.
5. **Standarisasi Nilai**: Mengubah penulisan nama komoditas yang tidak konsisten (misal: "Beras Medium" -> "Beras").

### C. Gold Layer (`03_gold.py`)

Lapisan akhir yang siap dikueri oleh pengguna bisnis atau Dashboard. Kami membuat 4 tabel Gold dalam format Delta:

1. `gold/pangan_volatility` (Repro): Agregasi nilai Max, Min, Rata-rata, serta Indeks Volatilitas Harga per komoditas.
2. `gold/pangan_trend` (Repro): Rata-rata harga per periode menit/jam untuk line chart tren.
3. `gold/pangan_alert` (Enhanced): Early warning system mendeteksi fluktuasi harga > 5% dibanding observasi sebelumnya menggunakan **Window Functions** (`lag`).
4. `gold/pangan_news_correlation` (Enhanced): **Cross-source join** antara Silver API dan Silver RSS untuk mengaitkan jumlah artikel berita tentang suatu komoditas dengan rata-rata persentase fluktuasi harganya.

---

## 4. Transformasi pada Silver Layer

Silver Layer merupakan tahap pemrosesan data setelah data mentah berhasil disimpan pada Bronze Layer. Tujuan utama layer ini adalah meningkatkan kualitas data sehingga siap digunakan untuk analisis pada Gold Layer.

### 1. Menghapus Data Duplikat (Deduplication)

#### Implementasi

```python
.dropDuplicates(["timestamp", "komoditas", "harga"])
```

#### Mengapa Penting?

Pada sistem streaming atau proses ingestion berulang, data yang sama dapat masuk lebih dari satu kali. Jika data duplikat tidak dihapus, maka:

- Rata-rata harga menjadi tidak akurat.
- Perhitungan tren menjadi bias.
- Visualisasi dashboard menjadi menyesatkan.

#### Contoh

Sebelum:

| Timestamp | Komoditas | Harga |
|------------|------------|--------|
| 10:00 | Beras | 14000 |
| 10:00 | Beras | 14000 |

Sesudah:

| Timestamp | Komoditas | Harga |
|------------|------------|--------|
| 10:00 | Beras | 14000 |

---

### 2. Validasi dan Filtering Data

#### Implementasi

```python
.filter(
    col("harga").isNotNull() &
    (col("harga") > 0)
)
```

#### Mengapa Penting?

Data harga yang kosong atau bernilai negatif tidak memiliki makna bisnis.

Jika tidak dibersihkan:

- Analisis harga rata-rata menjadi salah.
- Perhitungan volatilitas menjadi tidak valid.
- Dashboard dapat menampilkan nilai anomali.

#### Data yang Dihapus

- Harga kosong (`NULL`)
- Harga = 0
- Harga negatif

---

### 3. Konversi Tipe Data (Casting)

#### Implementasi

```python
.withColumn(
    "harga",
    col("harga").cast("double")
)
```

```python
.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)
```

#### Mengapa Penting?

Data dari API dan RSS biasanya diterima dalam bentuk string.

Agar Spark dapat melakukan:

- Perhitungan statistik
- Sorting waktu
- Window Function
- Agregasi

maka data harus dikonversi ke tipe yang sesuai.

#### Contoh

Sebelum:

```text
harga = "14000"
timestamp = "2026-06-01 10:00:00"
```

Sesudah:

```text
harga = 14000.0
timestamp = Timestamp
```

---

### 4. Penambahan Kolom Analitik

#### Implementasi

```python
.withColumn("jam", hour(col("timestamp")))
.withColumn("tanggal", to_date(col("timestamp")))
```

#### Mengapa Penting?

Kolom tambahan mempermudah proses analisis pada Gold Layer.

Contohnya:

- Analisis harga per hari
- Analisis harga per jam
- Pembuatan dashboard time-series

#### Contoh

Timestamp:

```text
2026-06-01 15:45:22
```

Menjadi:

| Jam | Tanggal |
|------|----------|
| 15 | 2026-06-01 |

---

### 5. Standarisasi Nama Komoditas

#### Implementasi

```python
.when(col("komoditas") == "beras", "Beras")
```

#### Mengapa Penting?

Sumber data yang berbeda sering menggunakan format penulisan berbeda.

Contoh:

```text
beras
BERAS
Beras
beras medium
```

Jika tidak diseragamkan:

- Spark menganggapnya sebagai komoditas berbeda.
- Hasil agregasi menjadi tidak akurat.

Setelah standarisasi:

```text
Beras
```

digunakan secara konsisten pada seluruh pipeline.

---

# Perbandingan Analisis Gold Layer dengan Analisis Spark ETS Sebelumnya

## Analisis Spark ETS Sebelumnya

Analisis yang dilakukan pada ETS sebelumnya umumnya terbatas pada:

- Statistik dasar
- Rata-rata harga
- Jumlah data
- Visualisasi sederhana
- Query Spark biasa

Contoh:

```python
df.groupBy("komoditas").avg("harga")
```

Hasil:

| Komoditas | Rata-rata Harga |
|------------|----------------|
| Beras | 14500 |

---

## 5. Analisis pada Gold Layer

Gold Layer menghasilkan insight yang lebih kaya dan siap digunakan oleh dashboard maupun pengambilan keputusan.

### 1. Volatilitas Harga

Mengukur seberapa besar fluktuasi harga suatu komoditas.

```python
(max_harga - min_harga) / min_harga
```

Manfaat:

- Identifikasi komoditas tidak stabil.
- Monitoring risiko kenaikan harga.

---

### 2. Trend Harga

Menganalisis perubahan harga berdasarkan waktu.

Manfaat:

- Mengetahui arah pergerakan harga.
- Dasar pembuatan grafik time-series.

---

### 3. Alert Harga

Menggunakan Window Function.

```python
lag("harga")
```

Manfaat:

- Deteksi kenaikan harga secara otomatis.
- Monitoring kondisi pasar secara real-time.

Contoh:

| Harga Lama | Harga Baru |
|------------|------------|
| 10000 | 12000 |

Kenaikan:

```text
20%
```

Alert:

```text
NAIK SIGNIFIKAN
```

---

### 4. Korelasi Berita dan Harga

Menggabungkan:

- Data harga
- Data RSS berita

Manfaat:

- Mengetahui pengaruh berita terhadap harga pasar.
- Insight yang tidak tersedia pada analisis Spark biasa.

---

### Ringkasan Perbandingan

| Fitur | Spark ETS Lama | Gold Layer |
|---------|---------|---------|
| Statistik Dasar | ✓ | ✓ |
| Rata-rata Harga | ✓ | ✓ |
| Trend Harga | ✗ | ✓ |
| Volatilitas | ✗ | ✓ |
| Alert Otomatis | ✗ | ✓ |
| Korelasi Berita | ✗ | ✓ |
| Dashboard Ready | Sebagian | ✓ |
| Business Insight | Rendah | Tinggi |

---

## 6. Demonstrasi Time Travel Delta Lake

Time Travel memungkinkan kita mengkueri snapshot data masa lalu menggunakan riwayat transaction log Delta Lake (`_delta_log`).

Di dalam `02_silver.py`, kami mendemonstrasikan ini dengan:

1. Menampilkan riwayat transaksi menggunakan `deltaTable.history()`.
2. Mengubah (update) harga Jagung yang di bawah Rp 6000 menjadi Rp 6000.
3. Membaca data saat ini (setelah ter-update) dan membandingkannya dengan data versi pertama sebelum ter-update menggunakan opsi `.option("versionAsOf", 0)`.

---

## 7. Refleksi: Keuntungan Nyata Delta Lake vs Flat HDFS/CSV

Dengan menerapkan Delta Lake sebagai format tabel, kami memperoleh keuntungan krusial dibanding menyimpan langsung di HDFS (berupa file JSON/CSV biasa):

1. **Jaminan Transaksi ACID**: Delta Lake menjamin operasi write, update, dan delete bersifat atomik melalui transaction log (`_delta_log`). Tidak ada data corrupt atau partial write akibat pipeline crash di tengah jalan.
2. **Schema Enforcement & Evolution**: Delta Lake memproteksi data dari "bad data" dengan memvalidasi skema secara otomatis. Jika skema berubah secara legal, kita dapat menggunakan opsi `mergeSchema` untuk melakukan evolusi skema tanpa merusak pipeline lama.
3. **Audit Trail & Time Travel**: Setiap modifikasi dicatat dalam log. Kami dapat melihat history audit (siapa, kapan, dan operasi apa yang dilakukan) dan melakukan rollback atau memanggil ulang data di masa lampau (`versionAsOf`). Hal ini krusial untuk melatih ulang model Machine Learning secara reprodusibel.
4. **Performa Query Lebih Cepat**: Delta Lake menyimpan file dalam format Parquet (columnar) terkompresi lengkap dengan file metadata, statistik min/max, dan partisi. Ini memungkinkan query engine melakukan skip file (data pruning) sehingga query analitik jauh lebih cepat dibandingkan memindai seluruh direktori JSON mentah.
