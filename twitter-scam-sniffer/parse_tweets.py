from datetime import datetime
import pandas as pd
import dataclasses
import requests
import codecs
import json
import html
import bs4
import sys
import re

ADDR_REGEX = r"0x[\w\d]{1,42}(?![0-9a-fA-F])"

@dataclasses.dataclass
class Tweet(object):
	timestamp: datetime
	raw: str
	text: str
	attack_type: str
	victim: str
	attacker: str
	correct_attress: str
	image_url: str
	connected_previous: bool
	connected_next: bool
	transaction: str
	tweet_id: int

# decorator to ignore exceptions
def nothrow(name, default_value=None, verbose=True):
	def decorator(func):
		# return func
		def wrapper(*args, **kwargs):
			try:
				return func(*args, **kwargs)
			except Exception as e:
				if verbose:
					print(f"Error parsing {name}:")
					print(e)
				return default_value

		return wrapper
	return decorator

@nothrow("timestamp")
def get_tweet_timestamp(tweet: bs4.BeautifulSoup):
	time_tag = tweet.find("time")

	return datetime.fromisoformat(time_tag["datetime"]).replace(tzinfo=None)

# recursively get the inner text from a tag
@nothrow("tag_text", "")
def get_tag_text(tag):
	if type(tag) == str or type(tag) == bs4.element.NavigableString:
		return tag

	name = tag.name
	if name == "img":
		return tag["alt"]
	else:
		return u"".join( map(get_tag_text, tag.children) )

@nothrow("text")
def get_tweet_text(tweet: bs4.BeautifulSoup):
	text_tag = tweet.find(attrs={"data-testid": "tweetText"})

	return get_tag_text(text_tag)

# get aggress marked by one of <markers> from tweet
def extract_address(tweet: str, markers):
	for marker in markers:
		regex = f"{re.escape(marker)}:\\s+({ADDR_REGEX})"
		match_data = re.search(regex, tweet)
		if match_data != None:
			return match_data[1]

	return None

# get multiple addresses marked by one of <markers> from a tweet
def get_multi_addresses(tweet: str, markers):
	for marker in markers:
		marker_index = tweet.find(marker)
		if marker == -1:
			continue

		addrs = tweet[marker_index + len(marker):]
		return re.findall(ADDR_REGEX, addrs)

	return None

@nothrow("victim")
def get_tweet_victim(tweet: str):
	victims = get_multi_addresses(tweet, ("victims:", "victim addresses:", "victims' addresses:"))
	if victims is not None and len(victims) > 0:
		return "|".join(victims)
	return extract_address(tweet, ("victim address", "victim"))

@nothrow("attacker")
def get_tweet_attacker(tweet: str):
	return extract_address(tweet, ("attacker address", "attacker", "scammer", "wrong address"))

@nothrow("correct_address")
def get_tweet_correct_address(tweet: str):
	return extract_address(tweet, ("correct address", "correct"))

@nothrow("attack_type")
def get_tweet_attack_type(tweet: str):
	if ("wrong address" in tweet) or ("poison" in tweet):
		return "address poisoning"
	# not sure how to check for other types
	return "phishing/other"

# get etherscan.io address from tweeter minimizer link by calling it
def get_transaction_by_calling(tweet: bs4.BeautifulSoup):
	etherscan_tag = tweet.find(lambda t: t.name == "a" and "from etherscan.io" in t.contents)
	addr = etherscan_tag.src
	print(f"getting \"{addr}\"")
	response = requests.get(addr)

	response_page = bs4.BeautifulSoup(response.text)
	return response_page.head.title.string

@nothrow("transaction", verbose=False)
def get_tweet_transaction(tweet: bs4.BeautifulSoup, tweet_str: str):
	etherscan_regex = re.compile("etherscan\\.io/tx/0x[\\w\\d]+")
	match_data = etherscan_regex.search(tweet_str)
	if match_data != None:
		return match_data[0]

	return get_transaction_by_calling(tweet)

@nothrow("image")
def get_tweet_image(tweet: bs4.BeautifulSoup):
	image_tags = tweet.find_all("img", {"alt": "Image"})
	image_urls = map(lambda tag: tag["src"], image_tags)
	return "|".join(image_urls)

@nothrow("connections", default_value=(False, False))
def get_tweet_connections(tweet: bs4.BeautifulSoup):
	prev_connection_tags = tweet.find_all(class_="css-175oi2r r-1bimlpy r-1jgb5lz r-m5arl1 r-1p0dtai r-1d2f490 r-u8s1d r-zchlnj r-ipm5af")
	next_connection_tags = tweet.find_all(class_="css-175oi2r r-1bimlpy r-1jgb5lz r-m5arl1 r-16y2uox r-14gqq1x")

	return (len(prev_connection_tags) > 0, len(next_connection_tags) > 0)

def parse_tweet(raw_tweet: str, index: int):
	print(f"parsing tweet #{index}")
	tweet = bs4.BeautifulSoup(raw_tweet)

	timestamp = get_tweet_timestamp(tweet)
	text = get_tweet_text(tweet)
	victim = get_tweet_victim(text)
	attacker = get_tweet_attacker(text)
	correct_attress = get_tweet_correct_address(text)
	attack_type = get_tweet_attack_type(text)
	image = get_tweet_image(tweet)
	connected_previous, connected_next = get_tweet_connections(tweet)
	transaction = get_tweet_transaction(tweet, text)

	return Tweet(timestamp, raw_tweet, text, attack_type, victim, attacker,
	 correct_attress, image, connected_previous, connected_next, transaction, 
	 0)

# unused
def is_null(value):
	return (value is None) or (type(value) == int and value == 0) or (len(value) == 0)

# unused
def get_matched_value(value1, value2, default_value):
	if not is_null(value1):
		return value1
	elif not is_null(value2):
		return value2
	return default_value

def make_related_tweets_indices(df: pd.DataFrame):
	counter = 1
	for (idx_current, row_current), (idx_next, row_next) in zip(df[:-1].iterrows(), df[1:].iterrows()):
		print(idx_current) # debug
		print(df.loc[idx_current, ["tweet_id", "connected_previous", "connected_next"]]) # debug
		print(idx_next) # debug
		print(df.loc[idx_next, ["tweet_id", "connected_previous", "connected_next"]]) # debug
		if row_current.connected_previous and row_next.connected_next:
			print("!!connected!!") # debug
			tweet_id = 0
			if df.loc[idx_current].tweet_id != 0:
				tweet_id = df.loc[idx_current].tweet_id
			elif df.loc[idx_next].tweet_id != 0:
				tweet_id = df.loc[idx_next].tweet_id
			else:
				tweet_id = counter
				counter += 1

			print(f"tweet_id = {tweet_id}") # debug
			df.loc[idx_current, "tweet_id"] = tweet_id
			df.loc[idx_next, "tweet_id"] = tweet_id

def fix_related_tweets(df: pd.DataFrame):
	make_related_tweets_indices(df)
	columns = ["attacker", "victim", "correct_attress", "transaction", "attack_type"]
	mask = df.tweet_id > 0
	# need to fill forward & back separately bc pandas doesn't have a function to do this at once
	df[mask].groupby(["tweet_id"])[columns].apply(lambda s: s.ffill(inplace=True))
	df[mask].groupby(["tweet_id"])[columns].apply(lambda s: s.bfill(inplace=True))

	return df

def add_missing_ids(df: pd.DataFrame):
	no_id_rows = df[df.tweet_id == 0]
	max_tweet_id = df.tweet_id.max()
	counter = 1

	for index, _ in no_id_rows.iterrows():
		df.loc[index, "tweet_id"] = max_tweet_id + counter
		counter += 1

def main(filename: str):
	# read tweets from JSON
	tweets = []
	with codecs.open(filename, "r", encoding="utf-8") as file:
		tweets = json.load(file)

	parsed_tweets = [parse_tweet(t, i) for i, t in enumerate(tweets)]
	df = pd.DataFrame([dataclasses.asdict(t) for t in parsed_tweets])
	df = df[df.timestamp.notnull() & df.text.notnull()]
	df = df.drop_duplicates(subset=["text"])
	df.sort_values("timestamp", inplace=True, ascending=False)
	df = fix_related_tweets(df)
	print(f"max id = {df.tweet_id.max()}") # debug
	add_missing_ids(df)

	df.to_excel("tweets.xlsx", index=False)

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print(f"USAGE: {sys.argv[0]} json_file")

	main(sys.argv[1])
