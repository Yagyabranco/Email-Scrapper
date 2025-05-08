import json
import re
import time
import os
import pandas as pd
import random
import logging
import urllib.parse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

# === CONFIGURATION ===
MIN_DELAY = 5
MAX_DELAY = 8
CHECKPOINT_FILE = "emails_result.json"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("scraper.log")]
)

def load_keywords_from_excel(file_path):
    try:
        df = pd.read_excel(file_path, sheet_name=1)

        if "Keywords" not in df.columns:
            raise ValueError("Column 'Keywords' not found in Sheet 2.")

        keywords = df["Keywords"].astype(str).str.strip()
        unique_keywords = sorted(set(k for k in keywords if k))

        if not unique_keywords:
            logging.error("❌ No valid keywords found.")
            return []

        logging.info(f"📍 Loaded {len(unique_keywords)} unique keywords from Sheet 2 in {file_path}")
        return unique_keywords
    except Exception as e:
        logging.error(f"❌ Error loading Excel file: {str(e)}")
        return []



def load_checkpoint(checkpoint_file=CHECKPOINT_FILE):
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r") as f:
                content = f.read().strip()
                if not content:
                    logging.info(f"⚠️ Checkpoint file {checkpoint_file} is empty. Starting fresh.")
                    return {}
                return json.loads(content)
        except json.JSONDecodeError as e:
            logging.error(f"❌ Corrupted checkpoint file: {e}. Starting fresh.")
            return {}
        except Exception as e:
            logging.error(f"❌ Error loading checkpoint file: {e}")
            return {}
    return {}

def save_checkpoint(email_results, checkpoint_file=CHECKPOINT_FILE):
    try:
        with open(checkpoint_file, "w") as f:
            json.dump(email_results, f, indent=4)
        logging.info(f"💾 Checkpoint saved to: {checkpoint_file}")
    except Exception as e:
        logging.error(f"❌ Error saving checkpoint: {str(e)}")

def initialize_driver(extension_path, extension_path2):
    if not os.path.exists(extension_path):
        raise FileNotFoundError(f"Extension file not found at: {extension_path}")
    if not os.path.exists(extension_path2):
        raise FileNotFoundError(f"Buster extension file not found at: {extension_path2}")

    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_extension(extension_path)
    chrome_options.add_extension(extension_path2)
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"
    ]
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    try:
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        return driver
    except Exception as e:
        logging.error(f"❌ Error initializing WebDriver: {str(e)}")
        return None

def scrape_google_emails(extension_path, extension_path2, keywords):
    email_results = load_checkpoint()
    driver = None
    location = "USA"

    try:
        driver = initialize_driver(extension_path, extension_path2)
        if not driver:
            raise Exception("Failed to initialize WebDriver")

        for keyword in keywords:
            key = f"{keyword} | {location}"
            if key in email_results:
                logging.info(f"⏭️ Skipping already processed keyword: {key}")
                continue

            query = f'site:linkedin.com {keyword} in "{location}" "email" "com" -india'
            logging.info(f"🔍 Searching: {query}")
            encoded_query = urllib.parse.quote(query)
            url = f'https://www.google.com/search?q={encoded_query}&num=50'
            logging.info(f"🌐 Constructed URL: {url}")
            try:
                driver.get(url)
                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

                # Scroll to load more results
                last_height = driver.execute_script("return document.body.scrollHeight")
                while True:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(random.uniform(8, 12))
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                    time.sleep(random.uniform(5, 7))

                page_html = driver.page_source
                emails = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.(?!png|jpg|jpeg)[a-zA-Z]{2,}', page_html))
                email_results[key] = sorted(list(emails))

                logging.info(f"📧 Found {len(emails)} email(s) for: {key}")
                for email in emails:
                    logging.info(email)

                save_checkpoint(email_results)

            except WebDriverException as e:
                logging.error(f"❌ WebDriver error for {key}: {e}")
                if driver:
                    driver.quit()
                driver = initialize_driver(extension_path, extension_path2)
                if not driver:
                    logging.error("❌ Failed to restart WebDriver. Saving results and exiting.")
                    save_checkpoint(email_results)
                    return email_results
                logging.info(f"🔄 WebDriver restarted. Retrying {key}.")
                continue

    except Exception as e:
        logging.error(f"❌ Error during scraping: {str(e)}")
        save_checkpoint(email_results)

    finally:
        if driver:
            driver.quit()
            logging.info("🛑 Browser closed.")

    return email_results

if __name__ == "__main__":
    extension_path = "/Users/surindersuri/Desktop/email/KDPLAPECIAGKKJOIGNNKFPBFKEBCFBPB_0_3_24_0.crx"
    extension_path2 = "/Users/surindersuri/Desktop/email/Buster.crx"
    excel_file_path = "/Users/surindersuri/Desktop/email/country.xlsx"

    try:
        for path in [excel_file_path, extension_path, extension_path2]:
            if not os.path.exists(path):
                raise FileNotFoundError(f"Missing: {path}")

        # Load keywords from the Excel file
        keywords = load_keywords_from_excel(excel_file_path)
        if not keywords:
            logging.error("❗ No keywords loaded. Exiting.")
            exit(1)

        # Start scraping emails
        all_emails = scrape_google_emails(extension_path, extension_path2, keywords)
        total = sum(len(emails) for emails in all_emails.values())
        logging.info(f"✅ All scraping done. Total unique emails found: {total}")

    except Exception as e:
        logging.error(f"❗ Script execution failed: {str(e)}")
