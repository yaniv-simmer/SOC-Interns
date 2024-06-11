import pandas as pd
import re

file_path = 'output/rekt_raw_data.csv'
df = pd.read_csv(file_path)

def extract_http_links(text):
        links = re.findall(r'http[s]?://\S+', text)
        cleaned_links = [re.sub(r'[\n\r<>]', '', link).strip() for link in links]
        return cleaned_links

all_links = []
for col in df.columns:
    df[col].dropna().apply(lambda x: all_links.extend(extract_http_links(x)))

links_df = pd.DataFrame(all_links, columns=['Links'])

output_file_path = 'output/extracted_links.csv'
links_df.to_csv(output_file_path, index=False)

