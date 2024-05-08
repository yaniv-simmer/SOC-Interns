from twikit import Client
import pandas as pd
import os
import csv
import argparse
import sys


'''
ther is some bug in the code that i can't find. the code extracts tweets up to almost half a year ago 
but for some reson it skipps some of the twwets. notice the #TODO also

usege is :
python3 nitter_scrape.py <twitter_user_name> <password>
'''


class TwitterScraper:

    def __init__(self, username: str, password: str):

        self.client = Client('en-US')
        self.client.login(auth_info_1=username, password=password)
        self.client.save_cookies('cookies.json')
        self.client.load_cookies(path='cookies.json')

    def get_all_tweets(self, screen_name: str) -> bool:
        user = self.client.get_user_by_screen_name(screen_name)
        old_tweets = user.get_tweets('Tweets')
        self.write_tweets_to_file(old_tweets, 'tweets123.csv')
        i=0
        while(i<3000):
            i += 1
            more_tweets = old_tweets.next()  # Retrieve more tweets
            self.write_tweets_to_file(more_tweets, 'tweets123.csv')
            old_tweets = more_tweets
        return True
    
    def write_tweets_to_file(self, tweets, filename):
        tweets_to_store = []
        for tweet in tweets:
            tweets_to_store.append({\
                'id': tweet.id,
                'created_at': tweet.created_at,
                'author of teewt': tweet.user,
                'full_text': tweet.full_text,
                "in_reply_to":tweet.in_reply_to,
                'reply_to' : tweet.in_reply_to})
        # We can make the data into a pandas dataframe and store it as a CSV file
        df = pd.DataFrame(tweets_to_store)
        
        # Check if file exists to avoid writing header multiple times
        if not os.path.isfile(filename):
            df.to_csv(filename, index=False)
        else:  # else it exists so append without writing the header
            df.to_csv(filename, mode='a', header=False, index=False)
    

    def format_csv(self):
        #TODO extract the tweets that contain "victim:.... attaker:.... scam:...."


        with open('tweets123.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            data = list(reader)

        for row in data:
            row[3] = row[3].replace('\n', '\\n')


        with open('tweets123_nice.csv', 'w', newline='',  encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)


if __name__ == "__main__":
    # user name and password will be passed from CMD
    user_name = sys.argv[1]
    password = sys.argv[2]


    scraper = TwitterScraper(user_name, password)
    tweets = scraper.get_all_tweets('realScamSniffer')
    scraper.format_csv()