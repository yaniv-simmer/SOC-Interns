import argparse
import pandas as pd
import openai
import re

# Load the CSV file
file_path = 'output\data_scrape.csv'
data = pd.read_csv(file_path)


# Define a function to classify addresses using the OpenAI API
def classify_addresses(description):
    prompt = f"""Classify the following blockchain addresses into categories: 
    'malicious transaction hash', 'scammer', 'exploiter', 'drainer', 'hacker address', 'malicious smart contract'. 
    If an address cannot be classified, return 'NULL'.

    Description: {description}

    Return the result in the format:
    {{
        "chain": <chain>,
        "malicious transaction hash": <malicious transaction hash>,
        "scammer": <scammer>,
        "exploiter": <exploiter>,
        "drainer": <drainer>,
        "hacker address": [<hacker address>],
        "malicious smart contract": <malicious smart contract>
    }}
    """

    response = openai.Completion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=500,
        temperature=0.7)

    return response.choices[0].text.strip()


def main():
    # Extract relevant data from the CSV file and classify addresses
    results = []

    for index, row in data.iterrows():
        description = row['rekt_raw_data']
        classification = classify_addresses(description)
        results.append(classification)

    # Convert the results into a DataFrame
    results_df = pd.DataFrame(results)

    # Save the results to a new CSV file
    # if file does not exist, create it and write the header
    if not os.path.exists('output/classified_addresses.csv'):
        with open('output/classified_addresses.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['classification'])

    output_file_path = 'output/classified_addresses.csv'
    results_df.to_csv(output_file_path, index=False)

    print(f"Classified data saved to {output_file_path}")

if __name__ == '__main__':
    #usage: python gpt_helper.py yourapikey
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('apikey', metavar='N', type=str,
                        help='an API key for OpenAI')

    args = parser.parse_args()

    # Set your OpenAI API key, arg from command line
    openai.api_key = args.apikey
    
    main()