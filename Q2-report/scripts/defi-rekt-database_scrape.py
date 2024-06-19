import csv
import os
import requests

BASE_URL = 'https://api.de.fi/v1/rekt/list'
OUTPUT_FILE = 'output/defi_rekt-database_scrape.csv'
HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://de.fi',
    'Referer': 'https://de.fi/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Sec-Ch-Ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site'
}

def get_page_api_call(page: int) -> dict:
    '''
    Get the data from the API for a given page number and return the JSON response if successful 
    Parameters:
        page (int): The page number to get the data from
    Returns:
        dict: The JSON response from the API
    '''
    parameters = {
        'sortField': 'fundsLost',
        'sort': 'desc',
        'sortDirection': 'desc',
        'limit': 200,
        'page': page
    }
    response = requests.get(url = BASE_URL,params=parameters, headers=HEADERS)
    if response.status_code != 200:
        print(f'Error: {response}')
        return None
    return response.json()



def write_page_to_csv(data: list) -> None:
    '''
    Write the data to a CSV file to store the scraped information

    Args:
        data (list): List of dictionaries containing the data to be written to the CSV file   
    '''
    with open(OUTPUT_FILE, mode='w', newline='' , encoding='utf-8') as file:
        # Create a CSV writer object
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        
        # Write the header
        writer.writeheader()
        
        # Write the data rows
        for row in data:
            writer.writerow(row)

            

 




def main():


    page = 1
    raw_page = get_page_api_call(page)
    # if file does not exist, create it and write the header
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['project_name','description','rekt_raw_data'])

    for page in range(1, raw_page['lastPage'] + 1):
        page_data = get_page_api_call(page)
        if page_data is None:
            #save the page number to a file to resume later
            with open('output/page_number.txt', 'w') as f:
                f.write(str(page))
        write_page_to_csv(page_data['items'])
        print(f'Page {page} done')


if __name__ == '__main__':
    main() 


