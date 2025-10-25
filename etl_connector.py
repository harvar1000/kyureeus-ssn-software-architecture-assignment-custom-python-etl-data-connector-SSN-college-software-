# etl_connector.py
import os
import time
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

# ---------------------------------------------------------------------
# Load environment variables from .env file
# ---------------------------------------------------------------------
load_dotenv()
GREYNOISE_API_KEY = os.getenv("GREYNOISE_API_KEY")  # optional for community
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "ssn_greynoise")
INTEGRATION_NAME = os.getenv("INTEGRATION_NAME", "ssn-greynoise-connector")

# ---------------------------------------------------------------------
# GreyNoise API setup
# ---------------------------------------------------------------------
BASE_URL = "https://api.greynoise.io"
HEADERS = {
    "Accept": "application/json",
    "User-Agent": f"{INTEGRATION_NAME}"
}
if GREYNOISE_API_KEY:
    HEADERS["key"] = GREYNOISE_API_KEY

# ---------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------
# MongoDB setup
# ---------------------------------------------------------------------
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

def insert_doc(collection_name, doc):
    """Insert a document into MongoDB with an ingestion timestamp"""
    doc["ingested_at"] = datetime.utcnow()
    result = db[collection_name].insert_one(doc)
    logging.info("Inserted _id=%s into %s", result.inserted_id, collection_name)
    return result.inserted_id


# ---------------------------------------------------------------------
# 1️⃣ Community endpoint
# ---------------------------------------------------------------------
def fetch_community_ip(ip):
    """GET /v3/community/{ip} - Community endpoint"""
    url = f"{BASE_URL}/v3/community/{ip}"
    print(f"\n🔍 Fetching community data for {ip} ...")
    resp = requests.get(url, headers=HEADERS, timeout=15)
    print("Status Code:", resp.status_code)
    try:
        print("Response JSON:", resp.json())
    except Exception:
        print("Response text:", resp.text)

    if resp.status_code == 200:
        data = resp.json()
        payload = {"query_ip": ip, "source": "community", "response": data}
        insert_doc("greynoise_community_raw", payload)
        return data
    else:
        logging.error("Community lookup failed for %s: %s - %s", ip, resp.status_code, resp.text)
        return {"error": resp.status_code, "text": resp.text}


# ---------------------------------------------------------------------
# 2️⃣ IP lookup endpoint
# ---------------------------------------------------------------------
def fetch_ip_lookup(ip, quick=False):
    """GET /v3/ip/{ip} - Full IP lookup (requires API key)"""
    params = {}
    if quick:
        params["quick"] = "true"
    url = f"{BASE_URL}/v3/ip/{ip}"
    print(f"\n🔍 Fetching IP lookup for {ip} (quick={quick}) ...")
    resp = requests.get(url, headers=HEADERS, params=params, timeout=20)
    print("Status Code:", resp.status_code)
    try:
        print("Response JSON:", resp.json())
    except Exception:
        print("Response text:", resp.text)

    if resp.status_code == 200:
        data = resp.json()
        payload = {"query_ip": ip, "quick": quick, "source": "ip_lookup", "response": data}
        insert_doc("greynoise_ip_raw", payload)
        return data
    else:
        logging.error("IP lookup failed for %s: %s - %s", ip, resp.status_code, resp.text)
        return {"error": resp.status_code, "text": resp.text}


# ---------------------------------------------------------------------
# 3️⃣ GNQL metadata endpoint
# ---------------------------------------------------------------------
def fetch_gnql_metadata(query, size=1000):
    """GET /v3/gnql/metadata - GNQL query (metadata only)"""
    url = f"{BASE_URL}/v3/gnql/metadata"
    params = {"query": query, "size": size}
    all_data = []
    attempt = 0

    while True:
        attempt += 1
        print(f"\n🔄 Fetching GNQL metadata page {attempt} ...")
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        print("Status Code:", resp.status_code)

        if resp.status_code == 200:
            rj = resp.json()
            data = rj.get("data", [])
            all_data.extend(data)
            request_meta = rj.get("request_metadata", {})
            complete = request_meta.get("complete", True)
            scroll = request_meta.get("scroll", "")

            # Insert page into MongoDB
            page_payload = {
                "query": query,
                "page": attempt,
                "response_data_count": len(data),
                "response": rj,
            }
            insert_doc("greynoise_gnql_metadata_raw", page_payload)

            if complete or not scroll:
                break
            params["scroll"] = scroll
            time.sleep(0.5)
        else:
            logging.error("GNQL metadata query failed: %s - %s", resp.status_code, resp.text)
            return {"error": resp.status_code, "text": resp.text}

    print(f"✅ Total items fetched: {len(all_data)}")
    return {"query": query, "count": len(all_data), "data": all_data}


# ---------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="GreyNoise ETL connector (SSN assignment)")
    sub = parser.add_subparsers(dest="cmd")

    c1 = sub.add_parser("community", help="lookup community ip")
    c1.add_argument("ip")

    c2 = sub.add_parser("ip", help="lookup ip context")
    c2.add_argument("ip")
    c2.add_argument("--quick", action="store_true")

    c3 = sub.add_parser("gnql", help="run gnql metadata query")
    c3.add_argument("query")
    c3.add_argument("--size", type=int, default=1000)

    args = parser.parse_args()

    if args.cmd == "community":
        print(fetch_community_ip(args.ip))
    elif args.cmd == "ip":
        print(fetch_ip_lookup(args.ip, quick=args.quick))
    elif args.cmd == "gnql":
        print(fetch_gnql_metadata(args.query, size=args.size))
    else:
        parser.print_help()
