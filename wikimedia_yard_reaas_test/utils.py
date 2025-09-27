import os
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt

# Base URL and output folder
base_url = "https://dumps.wikimedia.org/other/pageviews/2025/2025-01/"
output_dir = "data/raw/pageviews/2025-01"
os.makedirs(output_dir, exist_ok=True)

# Step 1: Scrape the index page to get all file names
resp = requests.get(base_url)
soup = BeautifulSoup(resp.text, "html.parser")

files = [
    a["href"]
    for a in soup.find_all("a")
    if a["href"].startswith("pageviews-2025") and a["href"].endswith(".gz")
]

print(f"Found {len(files)} files to download")

# Step 2: Download files if not already present
for f in files:
    url = base_url + f
    out_path = os.path.join(output_dir, f)
    if not os.path.exists(out_path):
        print(f"Downloading {f}...")
        r = requests.get(url, stream=True)
        with open(out_path, "wb") as f_out:
            for chunk in r.iter_content(chunk_size=8192):
                f_out.write(chunk)
    else:
        print(f"Already downloaded {f}")

# Step 3: Compute total size of all downloaded files
sizes = []
names = []

for f in files:
    out_path = os.path.join(output_dir, f)
    size = os.path.getsize(out_path)  # bytes
    sizes.append(size)
    names.append(f)

total_bytes = sum(sizes)
print(f"\nTotal bytes downloaded: {total_bytes:,}")

# Step 4: Plot
plt.figure(figsize=(12, 6))
plt.plot(range(len(sizes)), sizes, marker="o", label="File size (bytes)")
plt.axhline(y=sum(sizes), color="r", linestyle="--", label="Total size")
plt.xlabel("File index")
plt.ylabel("Size (bytes)")
plt.title("Downloaded Wikimedia Pageviews (January 2025)")
plt.legend()
plt.tight_layout()
plt.show()
