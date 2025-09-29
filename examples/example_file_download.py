from wikimedia_yard_reaas_test.utils import download_pageviews_files
import os

# Base URL and output folder
base_url = "https://dumps.wikimedia.org/other/pageviews/2025/2025-01/"
output_dir = "data/raw/pageviews/2025-01"

download_pageviews_files(base_url=base_url, output_dir=output_dir)
