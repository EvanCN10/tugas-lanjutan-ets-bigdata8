"""
producer_rss.py — Producer RSS Feed Berita Ekonomi Pangan
==========================================================
Anggota : Akbar
Topic   : pangan-rss
Update  : polling setiap 5 menit dengan scheduler
"""

import json, time, hashlib
import html
import re
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
import feedparser
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
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "pangan-rss"

RSS_FEEDS = [
    # Feed kategori Ekonomi (lebih stabil + sering memuat isu pangan)
    {"url": "https://www.cnnindonesia.com/ekonomi/rss", "source": "cnnindonesia.com"},
    {"url": "https://www.antaranews.com/rss/ekonomi.xml", "source": "antaranews.com"},

    # Backup (feed campur) — akan difilter ketat berdasarkan keyword + section
    {"url": "https://rss.bisnis.com/", "source": "bisnis.com"},
]

# Keyword relevansi: sengaja dibuat cukup "ketat" agar tidak kebanjiran berita non-pangan.
PANGAN_KEYWORDS = [
    "pangan",
    "sembako",
    "beras",
    "padi",
    "gabah",
    "bulog",
    "gula",
    "minyak goreng",
    "cabai",
    "cabe",
    "bawang",
    "telur",
    "ayam",
    "daging",
    "sapi",
    "ikan",
    "kedelai",
    "jagung",
    "pupuk",
]

_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")

sent_urls: set = set()


def url_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _normalize_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(str(s))
    s = _TAG_RE.sub(" ", s)
    s = s.lower()
    s = _SPACE_RE.sub(" ", s).strip()
    return s


def _link_allowed(link: str, source: str) -> bool:
    """Basic guard supaya feed ekonomi yang tercampur tidak membawa artikel lintas kanal."""
    try:
        u = urlparse(link)
        host = (u.netloc or "").lower()
        path = (u.path or "").lower()
    except Exception:
        return False

    if source == "cnnindonesia.com":
        return host.endswith("cnnindonesia.com")

    if source == "antaranews.com":
        # Feed ekonomi ANTARA kadang menyelipkan link otomotif.antaranews.com
        if not host.endswith("antaranews.com"):
            return False
        if host.startswith("otomotif."):
            return False
        return True

    if source == "bisnis.com":
        if not host.endswith("bisnis.com"):
            return False
        # Hindari kanal yang sering tidak relevan (bola/otomotif/dll)
        if host.startswith((
            "bola.", "sport.", "otomotif.", "tekno.", "travel.", "lifestyle.",
        )):
            return False
        if any(seg in path for seg in ("/bola/", "/otomotif/", "/sport/", "/tekno/", "/travel/", "/lifestyle/")):
            return False
        return True

    return True


def is_relevant(title: str, summary: str, link: str, source: str) -> bool:
    if not _link_allowed(link, source):
        return False
    teks = _normalize_text(title) + " " + _normalize_text(summary)
    return any(kw in teks for kw in PANGAN_KEYWORDS)


def parse_published(entry) -> str:
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            import calendar
            ts = calendar.timegm(entry.published_parsed)
            return datetime.utcfromtimestamp(ts).isoformat()
    except Exception:
        pass
    return datetime.now().isoformat()


@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def fetch_feed(producer, feed_url: str, source: str) -> int:
    try:
        feed = feedparser.parse(feed_url)
    except Exception as e:
        print(f"  ⚠️  Gagal fetch {source}: {e}")
        raise e

    count = 0
    tz_wib = timezone(timedelta(hours=7))

    for entry in feed.entries:
        link = getattr(entry, "link", "")
        if not link or link in sent_urls:
            continue

        title   = getattr(entry, "title", "")
        summary = getattr(entry, "summary", "")

        if not is_relevant(title, summary, link, source):
            continue

        payload = {
            "title":     title,
            "link":      link,
            "summary":   summary[:500],
            "published": parse_published(entry),
            "source":    source,
            "timestamp": datetime.now(tz_wib).isoformat(),
        }

        try:
            producer.send(TOPIC, key=url_hash(link), value=payload)
            sent_urls.add(link)
            count += 1
            print(f"    📰 [{source}] {title[:70]}...")
        except KafkaError as e:
            print(f"    ❌ Gagal kirim: {e}")

    return count


def job(producer):
    total = 0
    print(f"\n[Scheduler — {datetime.now().strftime('%H:%M:%S')}]")
    for feed in RSS_FEEDS:
        print(f"  🔗 Fetching {feed['source']}...")
        try:
            n = fetch_feed(producer, feed["url"], feed["source"])
            total += n
            print(f"     → {n} artikel baru dikirim")
        except Exception as e:
            print(f"     → Gagal setelah retries: {e}")
    producer.flush()
    print(f"  ↑ Total: {total} artikel | Cache: {len(sent_urls)} URL")


def main():
    print("=" * 55)
    print("  📡 HargaPangan Producer — RSS Berita Ekonomi")
    print(f"  Topic: {TOPIC} | Scheduler: 5 menit")
    print("=" * 55)

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=5,
        )
    except KafkaError as e:
        print(f"Gagal inisialisasi Kafka Producer: {e}")
        return

    # Run immediately first
    job(producer)

    # Schedule every 5 minutes
    schedule.every(5).minutes.do(job, producer)

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
