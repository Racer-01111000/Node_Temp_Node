import requests
import xml.etree.ElementTree as ET
import json
from pathlib import Path

OUT = Path.home() / "Node_Temp/training/corpus/arxiv_feed.jsonl"

ARXIV_URL = "http://export.arxiv.org/api/query?search_query=all:quantum+computing&start=0&max_results=20"

def fetch():
    r = requests.get(ARXIV_URL, timeout=10)
    return r.text

def parse(xml_data):
    root = ET.fromstring(xml_data)
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    entries = []
    for entry in root.findall("atom:entry", ns):
        title = entry.find("atom:title", ns).text.strip()
        summary = entry.find("atom:summary", ns).text.strip()

        entries.append({
            "topic": "arxiv_quantum",
            "type": "paper",
            "content": f"{title}: {summary}",
            "state": "UNVERIFIED"
        })

    return entries

def save(entries):
    with open(OUT, "a") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")

if __name__ == "__main__":
    xml_data = fetch()
    entries = parse(xml_data)
    save(entries)
    print(f"Ingested {len(entries)} real papers.")
