import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import time

dataset = pd.read_csv("dataset.csv", encoding="ISO-8859-1", on_bad_lines='skip')
dataset = dataset.drop(columns=['Unnamed: 2'], errors='ignore')

def ensure_url_scheme(df, url_column):
    for index, row in df.iterrows():
        url = row[url_column]
        if not url.startswith('http://'):
            url = 'http://' + url
        row[url_column] = url

ensure_url_scheme(dataset, 'url')


def scrape_company_data(company):
    url = company["url"]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    attempts = 3  
    for attempt in range(attempts):
        try:
            print(f"Accessing URL: {url} (Attempt {attempt + 1})") 
            response = requests.get(url, headers=headers)
            response.raise_for_status() 
            soup = BeautifulSoup(response.text, 'html.parser')

            
            manufacturer = "Yes" if "manufacturer" in soup.text.lower() else "No"
            brand = "Yes" if "brand" in soup.text.lower() else "No"
            distributor = "Yes" if "distributor" in soup.text.lower() else "No"
            relevant = "Yes"
            category = "Bulk (Manufacturer)" if manufacturer == "Yes" else "Bulk (Distributor)" if distributor == "Yes" else "Brand"
            
            
            probiotics = "Yes" if "probiotic" in soup.text.lower() else "No"
            fortification = "Yes" if "fortified" in soup.text.lower() else "No"
            gut_health = "Yes" if "gut health" in soup.text.lower() else "No"
            womens_health = "Yes" if "women's health" in soup.text.lower() else "No"
            cognitive_health = "Yes" if "cognitive health" in soup.text.lower() else "No"
            
            return {
                "Company": company["company_name"],
                "Website": url,
                "Relevant": relevant,
                "Category": category,
                "Manufacturer": manufacturer,
                "Brand": brand,
                "Distributor": distributor,
                "F&B": "Yes",  
                "Probiotics": probiotics,
                "Fortification": fortification,
                "Gut Health": gut_health,
                "Womens Health": womens_health,
                "Cognitive Health": cognitive_health
            }

        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                print(f"Access denied for {company['company_name']}: {e}. Retrying...")
            else:
                print(f"Error processing {company['company_name']}: {e}")
            time.sleep(1)  

        except requests.exceptions.RequestException as e:
            print(f"Error processing {company['company_name']}: {e}")
            return None
    return None  


results = []


for index, company in dataset.iterrows():
    print(f"Processing {company['company_name']}...")  
    data = scrape_company_data(company)
    if data:
        results.append(data)
    
  

df = pd.DataFrame(results)


csv_file = 'company_data.csv'
df.to_csv(csv_file, index=False)

print(f"Data scraping completed. Results saved to '{csv_file}'.")
