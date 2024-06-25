"""
This script aggregates and processes data from multiple blockchain security incident sources. It scrapes data from Blocksec, 
DeFi Rekt, and DeFiLlama, focusing on incidents such as hacks and scams. The data is then cleaned, combined, and filtered 
for incidents occurring between April and June. The final dataset is sorted by date and saved to a CSV file.

Dependencies:
- pandas: For data manipulation and analysis.
- blocksec_scrape, defi_rekt_database_scrape, defillama_hacks_scrape: Custom modules for scraping security incident data 
  from respective sources.

Usage:
- Ensure all dependencies are installed.
- Run this script directly to execute all scrapers, process the data, and generate the output CSV file.
"""



import pandas as pd
from blocksec_scrape import BlocksecScrape
from defi_rekt_database_scrape import DefiRektScrape
from defillama_hacks_scrape import DefillamaHacksScrape





# Constants
APRIL = 4
JUNE = 6
COLUMN_MAPPINGS = {
    'blocksec_data': ['project', 'loss', 'chainIds', 'rootCause', 'date', 'media'],
    'defi_rekt_data': ['project_name', 'funds_lost', 'network', 'scam_type', 'date','proof_link'],
    'defillama_hacks_data': ['name', 'amount', 'chains', 'classification', 'date', 'link']
}
NEW_COLUMN_NAMES = ['Project', 'Loss Amount', 'Chain', 'Vulnerability', 'DATE','Proof Link']
FILE_PATHS = [
    'output/blocksec_data.csv',
    'output/defi_rekt-database_scrape.csv',
    'output/defillama_hacks_scrape.csv'
]



def run_all_scrapers():
    print("Running all scrapers...")

    print("Running Blocksec scraper...")
    blocksec = BlocksecScrape()
    blocksec.make_request()
    blocksec.save_data()

    print("Running Defi Rekt scraper...")
    defi_rekt = DefiRektScrape()
    defi_rekt.run()

    print("Running Defillama Hacks scraper...")
    defillama_hacks = DefillamaHacksScrape()
    scraper = DefillamaHacksScrape()
    scraper.run()

def load_and_prepare_data(file_path, column_names, new_column_names):
    try:
        data = pd.read_csv(file_path)
        data_relevant = data[column_names].copy()
        data_relevant.columns = new_column_names
        return data_relevant
    except FileNotFoundError:
        return pd.DataFrame(columns=new_column_names)
    except Exception as e:
        return pd.DataFrame(columns=new_column_names)


# func to make csv look better 
def fix_CSV(file_path):

    df = pd.read_csv(file_path)
    df['DATE'] = pd.to_datetime(df['DATE']).dt.date
    df = df.sort_values(by='DATE')
    df = df.drop_duplicates()
    df.to_csv(file_path, index=False)




def main():
    run_all_scrapers() 

    combined_data = pd.DataFrame()

    for file_path, column_names_key in zip(FILE_PATHS, COLUMN_MAPPINGS):
        combined_data = pd.concat([
            combined_data,
            load_and_prepare_data(file_path, COLUMN_MAPPINGS[column_names_key], NEW_COLUMN_NAMES)
        ], ignore_index=True)

    combined_data['DATE'] = pd.to_datetime(combined_data['DATE'], errors='coerce')
    filtered_data = combined_data[(combined_data['DATE'].dt.month >= APRIL) & (combined_data['DATE'].dt.month <= JUNE)]
    sorted_data = filtered_data.sort_values(by='DATE')
    

    output_file = 'output/sorted_combined_blockchain_security_data_april_may_june.csv'
    sorted_data.to_csv(output_file, index=False)
    fix_CSV(output_file)

if __name__ == "__main__":
    main()