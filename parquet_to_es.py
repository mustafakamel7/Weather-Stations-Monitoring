import os
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch("http://localhost:9200")
PARQUET_DIR = os.path.expanduser("~/weather-project/central-station/data/parquet")
INDEX_NAME = "weather_history"

# Recursively converts hidden NumPy types into standard Python JSON types
def clean_dict(d):
    clean = {}
    for k, v in d.items():
        if isinstance(v, dict):
            clean[k] = clean_dict(v)
        elif pd.isna(v):  # Handle nulls/NaNs
            clean[k] = None
        elif hasattr(v, "item"): # Safely converts numpy int64/float64 to native python
            clean[k] = v.item()
        else:
            clean[k] = v
    return clean

def generate_actions():
    count = 0
    for root, dirs, files in os.walk(PARQUET_DIR):
        for file in files:
            if file.endswith(".parquet"):
                filepath = os.path.join(root, file)
                print(f"Reading: {filepath}")
                
                df = pd.read_parquet(filepath)
                
                for _, row in df.iterrows():
                    count += 1
                    doc = clean_dict(row.to_dict())
                    
                    # Create a deterministic ID (e.g., "6_1530")
                    unique_id = f"{doc['station_id']}_{doc['s_no']}"
                    
                    yield {
                        "_index": INDEX_NAME,
                        "_id": unique_id,  # Tell ES to use this specific ID!
                        "_source": doc
                    }
                    
    if count == 0:
        print("❌ WARNING: Zero Parquet files were found in the directory.")

print("Starting bulk index into ElasticSearch...")
try:
    success, _ = helpers.bulk(es, generate_actions())
    print(f"🚀 SUCCESS! {success} records loaded into ElasticSearch!")
except Exception as e:
    print("\n" + "="*50)
    print("❌ ELASTICSEARCH REJECTED THE DATA")
    print("="*50)
    print(str(e))
    print("="*50)