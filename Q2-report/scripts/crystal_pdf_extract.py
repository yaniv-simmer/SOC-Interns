
import pandas as pd
import pdfplumber
import re



file_path = 'output\Crystal-Intelligence-Thirteen-Years-of-Crypto-Crimes-Unveiled.pdf'


def split_my(string):
    x = ['DeFi Breach', 'Breach', 'Fraud', 'Fraud/RugPull','DeFi Breach/Flash', 'Breach/Flash']
    
    # Create a pattern that matches any word in the list x
    pattern = '(?=' + '|'.join(map(re.escape, x)) + ')'
    # Split the string s at the first occurrence of any word in the list x
    # if there is no match, the result will be a list with a single element, the original string
    parts = re.split(pattern, string, 1)

    if len(parts) == 1:
        print(f'No match found in {string}')
        raise ValueError(f'No match found in {string}')
    #print(parts)
    return parts

    



def a():
    for page_num in range(16, 19):
        print(f'Processing page {page_num}...\n\n\n')
        all_rows = []
        with pdfplumber.open(file_path) as pdf:
            page = pdf.pages[page_num]
            text = page.extract_text()

            lines_raw = text.split('\n')
            
            all_rows = []
            start = 4
            if page_num == 16:
                start = 4 
            else:
                start = 2
            range_len = len(lines_raw[4:]) if page_num == 16 else len(lines_raw)

            for i in range(start,range_len-2,7):  # Skip the first 4 lines (headers)
                #print(start)
                #print(lines_raw[i:i+6])
                tow_first_fields = split_my(lines_raw[i+1])
                row = tow_first_fields + lines_raw[i:i+6]
                #row = lines_raw[i:i+6]
                print(row)
                all_rows.append([w.strip() for w in row])
                
    df = pd.DataFrame(all_rows, columns=['Entity', 'Type', '#','Country', 'Amount, USD', 'Currencies', 'Date'])
    df.to_csv('output\crystal.csv', index=False)


a()






