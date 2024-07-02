import requests
import pandas as pd

PAYLOAD_TEMPLATE = {
    "operationName": "GetReports",
    "variables": {
        "input": {
            "chains": [],
            "scamCategories": ["PIGBUTCHERING"],
            "orderBy": {
                "field": "CREATED_AT",
                "direction": "DESC"
            }
        },
        "first": 50,
        "after": None
    },
    "query": """
    query GetReports($input: ReportsInput, $after: String, $before: String, $last: Float, $first: Float) {
      reports(
        input: $input
        after: $after
        before: $before
        last: $last
        first: $first
      ) {
        pageInfo {
          hasNextPage
          hasPreviousPage
          startCursor
          endCursor
          __typename
        }
        edges {
          cursor
          node {
            ...Report
            __typename
          }
          __typename
        }
        count
        totalCount
        __typename
      }
    }
    
    fragment Report on Report {
      id
      isPrivate
      ...ReportPreviewDetails
      ...ReportAccusedScammers
      ...ReportAuthor
      ...ReportAddresses
      ...ReportEvidences
      ...ReportCompromiseIndicators
      ...ReportTokenIDs
      ...ReportTransactionHashes
      __typename
    }
    
    fragment ReportPreviewDetails on Report {
      createdAt
      scamCategory
      categoryDescription
      biDirectionalVoteCount
      viewerDidVote
      description
      lexicalSerializedDescription
      commentsCount
      source
      checked
      __typename
    }
    
    fragment ReportAccusedScammers on Report {
      accusedScammers {
        id
        info {
          id
          contact
          type
          __typename
        }
        __typename
      }
      __typename
    }
    
    fragment ReportAuthor on Report {
      reportedBy {
        id
        username
        trusted
        __typename
      }
      __typename
    }
    
    fragment ReportAddresses on Report {
      addresses {
        id
        address
        chain
        domain
        label
        __typename
      }
      __typename
    }
    
    fragment ReportEvidences on Report {
      evidences {
        id
        description
        photo {
          id
          name
          description
          url
          __typename
        }
        __typename
      }
      __typename
    }
    
    fragment ReportCompromiseIndicators on Report {
      compromiseIndicators {
        id
        type
        value
        __typename
      }
      __typename
    }
    
    fragment ReportTokenIDs on Report {
      tokens {
        id
        tokenId
        __typename
      }
      __typename
    }
    
    fragment ReportTransactionHashes on Report {
      transactionHashes {
        id
        hash
        chain
        label
        __typename
      }
      __typename
    }
    """
}

BASE_URL = 'https://www.chainabuse.com/api/graphql-proxy'

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9,he;q=0.8",
    "Content-Length": "2295",
    "Content-Type": "application/json",
    "Cookie": "cookie_consent_is_accepted=true; ...",
    "Newrelic": "eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIi,...",
    "Origin": "https://www.chainabuse.com",
    "Priority": "u=1, i",
    "Referer": "https://www.chainabuse.com/category/pigbutchering",
    "Sec-Ch-Ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Google Chrome\";v=\"126\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"Windows\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Traceparent": "00-b8a7a1eeb05cf163f5a409c7ab407c39-9c9b93455a6b8de4-01",
    "Tracestate": "3032839@nr=0-1-3032839-1386158001-9c9b93455a6b8de4----1719918207642",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
}


class ChainabuseScraper:

    def __init__(self):
        self.data = []

    def get_data(self):
        '''
        Get all reports from Chainabuse

        Returns:
            list: List of reports
        '''
        payload = PAYLOAD_TEMPLATE.copy()
        while True:
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            if response.status_code != 200:
                print('Error: ', response.status_code)
                return None
            response_json = response.json()
            self.data.extend(response_json['data']['reports']['edges'])
            if not response_json['data']['reports']['pageInfo']['hasNextPage']:
                break
            payload['variables']['after'] = response_json['data']['reports']['pageInfo']['endCursor']
        return self.data

    def to_dataframe(self):
        '''
        Convert data to pandas DataFrame
        
        Returns:
            DataFrame: DataFrame containing the data

        '''
        records = []
        for edge in self.data:
            records.append(edge['node'])

        df = pd.DataFrame(records)
        return df



    def format_data(self):
            '''
            Format the data and save it to a new CSV file. aacording to the requirements
            '''
            df = pd.read_csv('output\chainabuse_raw_data.csv')

            extracted_data = []

            for index, row in df.iterrows():
                addresses = eval(row['addresses'])
                for item in addresses:
                    if item['address'] is not None and item['chain'] is not None:
                        extracted_data.append({
                            'id': row['id'],
                            'address': item['address'],
                            'chain': item['chain'],
                            'evidences': row['evidences']
                        })
            new_df = pd.DataFrame(extracted_data)
            
            # remove all rows with chain = BTC or chain = Tron
            new_df = new_df[new_df['chain'] != 'BTC']
            new_df = new_df[new_df['chain'] != 'TRON']

            new_df.to_csv('output/formated_data.csv', index=False)

            print("New CSV file created with 'id', 'address', and 'chain' columns.")



if __name__ == '__main__':
    scraper_obj = ChainabuseScraper()
    data = scraper_obj.get_data()
    if data:
        df = scraper_obj.to_dataframe()
        print(df.head())
        df.to_csv('output/chainabuse_raw_data.csv', index=False)
        scraper_obj.format_data()
    else:
        print('Failed to get data')

