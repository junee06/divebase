import os, json, time, requests
from tqdm import tqdm

RETRIES = 3
DELAY = 1
BATCH_SIZE = 10000
API_URL = "https://mdl-pi.vercel.app/id/"

with open("drama_ids.json") as f:
    drama_list = json.load(f)

total = len(drama_list)
failed = []

os.makedirs("data", exist_ok=True)

for i in tqdm(range(0, total, BATCH_SIZE), desc="Batch Progress"):
    batch = drama_list[i:i + BATCH_SIZE]
    results = []

    for item in tqdm(batch, desc=f"Scraping {i+1} to {i+len(batch)}"):
        slug = item["url"].split("/")[-1]
        url = API_URL + slug

        for attempt in range(RETRIES):
            try:
                res = requests.get(url, timeout=10)
                if res.status_code == 200:
                    data = res.json()
                    results.append(data)
                    break
                else:
                    raise Exception(f"Status {res.status_code}")
            except Exception as e:
                if attempt < RETRIES - 1:
                    time.sleep(DELAY)
                else:
                    failed.append(slug)
                    print(f"Failed: {slug} - {e}")

    filename = f"data/mdl_batch_{i//BATCH_SIZE + 1}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

if failed:
    with open("failed_slugs.log", "w") as f:
        f.write("\n".join(failed))
    print(f"{len(failed)} slugs failed and logged.")
