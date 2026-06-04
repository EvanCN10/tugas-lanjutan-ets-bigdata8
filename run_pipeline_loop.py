import time
import subprocess
import sys

def run_pipeline():
    print("\n" + "=" * 50)
    print("🚀 Running Medallion Processing Ingestion & Processing Cycle")
    print("=" * 50)
    
    print("\n--- Running Stage 1: Bronze Ingestion ---")
    subprocess.run([sys.executable, "lakehouse/01_bronze.py"])
    
    print("\n--- Running Stage 2: Silver Cleaning & Transformation ---")
    subprocess.run([sys.executable, "lakehouse/02_silver.py"])
    
    print("\n--- Running Stage 3: Gold Aggregation ---")
    subprocess.run([sys.executable, "lakehouse/03_gold.py"])
    
    print("\n" + "=" * 50)
    print("✅ Cycle Complete. Sleeping for 60 seconds...")
    print("=" * 50)

if __name__ == "__main__":
    while True:
        try:
            run_pipeline()
        except Exception as e:
            print(f"❌ Error during pipeline loop: {e}")
        time.sleep(60)
