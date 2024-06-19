import pandas as pd
import requests
import json
import re

class DefillamaHacksScrape:
    """
    A class used to scrape data from the DeFiLlama hacks page.

    ...

    Attributes
    ----------
    html_content : str
        a string storing the HTML content of the page

    Methods
    -------
    make_api_call():
        Makes a GET request to the DeFiLlama hacks page and stores the HTML content.

    extract_json_data():
        Extracts JSON data from the HTML content.

    save_data_to_csv(data):
        Saves the extracted data to a CSV file.

    run():
        Executes the scraping process.
    """

    def __init__(self):
        """Initializes DefillamaHacksScrape with html_content set to None."""
        self.html_content = None

    def make_api_call(self):
        """
        Makes a GET request to the DeFiLlama hacks page and stores the HTML content.

        Returns
        -------
        bool
            True if the request was successful, False otherwise.
        """
        url = "https://defillama.com/hacks"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            self.html_content = response.text
        else:
            print(f"Failed to load page, status code: {response.status_code}")

        return response.status_code == 200

    def extract_json_data(self):
        """
        Extracts JSON data from the HTML content.

        Returns
        -------
        list
            A list of dictionaries containing the extracted data, or None if the API call failed.
        """
        if not self.make_api_call():
            return
        
        pattern = re.compile(r'\{"chains":.*?\}')
        json_string = self.html_content
        
        list_of_dicts = []
        for match in pattern.finditer(json_string):
            json_dict = json.loads(match.group())
            list_of_dicts.append(json_dict)

        return list_of_dicts
    
    def save_data_to_csv(self, data):
        """
        Saves the extracted data to a CSV file.

        Parameters
        ----------
        data : list
            The data to be saved.
        """
        df = pd.DataFrame(data)
        df.to_csv("output\defillama_hacks_scrape.csv", index=False)

    def run(self):
        """
        Executes the scraping process.
        """
        data = self.extract_json_data()
        if data is not None:
            self.save_data_to_csv(data)


if __name__ == "__main__":
    scraper = DefillamaHacksScrape()
    scraper.run()