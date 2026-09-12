import os
import requests
import urllib.parse
from bs4 import BeautifulSoup

# ==========================================
# 1. Configuration & Path Settings
# ==========================================

CATALOG = "rearc"
SCHEMA = "default"
VOLUME = "rearc_landing_zone"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
BLS_TARGET_DIR = os.path.join(VOLUME_PATH, "bls_productivity")
POP_TARGET_FILE = os.path.join(VOLUME_PATH, "us_population", "population_data.json")

BLS_INDEX_URL = "https://download.bls.gov/pub/time.series/pr/"
#POPULATION_API_URL = "https://datausa.io"
POPULATION_API_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"

# Use browser signatures paired with candidate details to satisfy compliance
CUSTOM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 (Contact: Hariharasudhan; hariharannitt@gmail.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9"
}

os.makedirs(BLS_TARGET_DIR, exist_ok=True)
os.makedirs(os.path.dirname(POP_TARGET_FILE), exist_ok=True)


# ==========================================
# 2. Corrected Ingestion Logic
# ==========================================
def ingest_bls_time_series(source_url, target_directory):
    print(f"[*] Crawling BLS remote directory listing: {source_url}")
    
    response = requests.get(source_url, headers=CUSTOM_HEADERS)
    if response.status_code != 200:
        raise Exception(f"Failed to access BLS index page. HTTP Status Code: {response.status_code}")
        
    soup = BeautifulSoup(response.text, 'html.parser')
    discovered_files = []
    
    # Base URL parsing tool to build clean targets
    base_netloc = urllib.parse.urlparse(source_url).netloc # download.bls.gov
    
    for link in soup.find_all('a'):
        href = link.get('href')
        if not href:
            continue
            
        # Ignore parent navigation paths
        if href == "../" or href == "/pub/time.series/" or "Parent Directory" in link.text:
            continue
            
        # Resolve any partial path relative to our root domain
        full_url = urllib.parse.urljoin(f"https://{base_netloc}", href)
        
        # Verify the target belongs inside our designated assignment directory
        if "/pub/time.series/pr/" in full_url:
            discovered_files.append(full_url)
            
    if not discovered_files:
        print("[!] Error: Still failed to parse files. Printing response content summary for debug:")
        print(response.text[:300])
        return

    print(f"[+] Successfully extracted {len(discovered_files)} structural targets from index.")
    
    # Process files sequentially for idempotency
    for file_url in discovered_files:
        filename = os.path.basename(urllib.parse.urlparse(file_url).path)
        destination_path = os.path.join(target_directory, filename)
        
        try:
            head_response = requests.head(file_url, headers=CUSTOM_HEADERS, timeout=10)
            remote_size = int(head_response.headers.get('Content-Length', 0))
            
            # IDEMPOTENCY CHECK: Bypass download if local file exists and matches size precisely
            if os.path.exists(destination_path) and os.path.getsize(destination_path) == remote_size and remote_size > 0:
                print(f"[~] Skipping match (unchanged): {filename}")
                continue
                
            print(f"[->] Syncing asset: {filename} ({remote_size / 1024:.2f} KB)...")
            
            with requests.get(file_url, headers=CUSTOM_HEADERS, stream=True, timeout=15) as file_stream:
                if file_stream.status_code == 200:
                    with open(destination_path, 'wb') as local_file:
                        for chunk in file_stream.iter_content(chunk_size=8192):
                            local_file.write(chunk)
                else:
                    print(f"[!] Target extraction failed for {filename}. Code: {file_stream.status_code}")
        except Exception as e:
            print(f"[!] Processing error on asset {filename}: {str(e)}")


def ingest_population_json(api_url, output_filepath):
    print(f"[*] Ingesting population data...")
    response = requests.get(api_url, headers=CUSTOM_HEADERS, timeout=15)
    if response.status_code == 200:
        with open(output_filepath, 'w', encoding='utf-8') as json_file:
            json_file.write(response.text)
        print(f"[+] Population snapshot written successfully: {output_filepath}")
    else:
        print(f"[!] Population API error. Code: {response.status_code}")


# ==========================================
# 3. Execution Pipeline Entrypoint
# ==========================================
if __name__ == "__main__":
    print("[===] Launching Corrected Step 1 Pipeline [===]")
    ingest_bls_time_series(BLS_INDEX_URL, BLS_TARGET_DIR)
    ingest_population_json(POPULATION_API_URL, POP_TARGET_FILE)
    print("[===] Step 1 Complete [===]")
