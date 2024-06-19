# load json file and extract the data
from datetime import datetime
import json
import csv

import requests

HEADERS = {

    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Blocksec-Token": "",  # Add the actual token here
    "Content-Length": "63",
    "Content-Type": "application/json;charset=utf-8",
    "Cookie": "pdfcc=2",
    "Origin": "https://app.blocksec.com",
    "Priority": "u=1, i",
    "Referer": "https://app.blocksec.com/explorer/security-incidents",
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}


chinIdToName ={
  "1": "ethereum",
  "8": "ubiq",
  "10": "optimism",
  "14": "flare",
  "19": "songbird",
  "20": "elastos",
  "24": "kardiachain",
  "25": "cronos",
  "30": "rsk",
  "40": "telos",
  "50": "xdc",
  "52": "csc",
  "55": "zyx",
  "56": "binance",
  "57": "syscoin",
  "60": "gochain",
  "61": "ethereumclassic",
  "66": "okexchain",
  "70": "hoo",
  "82": "meter",
  "87": "nova network",
  "42161":"Arbitrum One",
  "88": "viction",
  "100": "xdai",
  "106": "velas",
  "108": "thundercore",
  "122": "fuse",
  "128": "heco",
  "137": "polygon",
  "148": "shimmer_evm",
  "169": "manta",
  "200": "xdaiarb",
  "204": "op_bnb",
  "246": "energyweb",
  "248": "oasys",
  "250": "fantom",
  "269": "hpb",
  "288": "boba",
  "311": "omax",
  "314": "filecoin",
  "321": "kucoin",
  "324": "era",
  "336": "shiden",
  "361": "theta",
  "369": "pulse",
  "416": "sx",
  "463": "areon",
  "534": "candle",
  "570": "rollux",
  "592": "astar",
  "820": "callisto",
  "888": "wanchain",
  "1030": "conflux",
  "1088": "metis",
  "1101": "polygon_zkevm",
  "1116": "core",
  "1231": "ultron",
  "1234": "step",
  "1284": "moonbeam",
  "1285": "moonriver",
  "1440": "living assets mainnet",
  "1559": "tenet",
  "1975": "onus",
  "1992": "hubblenet",
  "2000": "dogechain",
  "2222": "kava",
  "2332": "soma",
  "4337": "beam",
  "4689": "iotex",
  "5000": "mantle",
  "5050": "xlc",
  "5551": "nahmii",
  "6969": "tombchain",
  "7171": "bitrock",
  "7700": "canto",
  "8217": "klaytn",
  "8453": "base",
  "88888":"chiliz",
  "8899": "jbc",
  "9001": "evmos",
  "9790": "carbon",
  "10000": "smartbch",
  "15551": "loop",
  "17777": "eos_evm",
  "23888": "blast",
  "32520": "bitgert",
  "32659": "fusion",
  "32769": "zilliqa",
  "42161": "arbitrum",
  "42170": "arbitrum_nova",
  "42220": "celo",
  "42262": "oasis",
  "43114": "avalanche",
  "47805": "rei",
  "55555": "reichain",
  "59144": "linea",
  "71402": "godwoken",
  "333999": "polis",
  "420420": "kekchain",
  "888888": "vision",
  "245022934": "neon",
  "1313161554": "aurora",
  "1666600000": "harmony",
  "11297108109": "palm",
  "836542336838601": "curio"  
}



class BlocksecScrape:
    '''
    this class is used to scrape the data from the blocksec website
    '''
    def __init__(self, time_stamp: int = 1735682399000):
        '''
        the time_stamp should be updated to the latest time_stamp !!
        '''
        self.base_url = 'https://app.blocksec.com/api/v1/attack/events'
        self.headers = HEADERS
        self.output_file_path = 'output/blocksec_data.csv'
        self.time_stamp = time_stamp
        self.data = None

    def convert_timestamp_to_date(self, timestamp: int) -> str:
        '''
        Convert the timestamp to a human-readable date format
        Parameters:
            timestamp (int): The timestamp to convert
        Returns:
            str: The formatted date
        '''
        timestamp_in_seconds = timestamp / 1000
        date_time = datetime.fromtimestamp(timestamp_in_seconds)
        formatted_date = date_time.strftime('%Y-%m-%d %H:%M:%S')
        
        return formatted_date

    def make_request(self):
        '''
        Make a POST request to the Blocksec API and store the response data
        '''
        payload = {
            "page": 1,
            "pageSize": 200,
            "endTime": self.time_stamp,
            "date": "desc"
            }

        response = requests.post(url=self.base_url, headers=HEADERS, json=payload)

        if response.status_code != 200:
            print(f'Error: {response}')
            return None
        self.data = response.json()['list']

    def save_data(self):
        '''
        Save the scraped data to a CSV file

        '''
        with open(self.output_file_path, mode='w', newline='' , encoding='utf-8') as file:
            # Create a CSV writer object
            writer = csv.DictWriter(file, fieldnames=self.data[0].keys())
            
            # Write the header
            writer.writeheader()
            
            # Write the data rows
            for row in self.data:
                row['chainIds'] = ', '.join([chinIdToName[str(chainId)] for chainId in row['chainIds']])
                row['date'] =self.convert_timestamp_to_date(row['date'])
                writer.writerow(row)


if __name__ == '__main__':
    blocksec = BlocksecScrape()
    blocksec.make_request()
    blocksec.save_data()

