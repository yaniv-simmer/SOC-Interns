# required for grequests to work
import gevent.monkey
gevent.monkey.patch_all()

# import re # regex
import grequests # for async fetching
import requests # for fetching
import dataclasses
import sys
import os # for folder & file work
from pathlib import Path # for folder & file work
import pandas as pd # for tables
from typing import Set, List
import math # ceil()
import itertools # chain.from_iterable
import time

DOMAIN = "https://api.arkhamintelligence.com"

HEADERS = {
	"API-Key": "qhN0gnXcxf8G3Z1RqSuTZB7uJdjRgq0E"
}

PAGE_SIZE = 50 # can't seem to change it

MAX_PAGES_AT_ONCE = 10000

REQUEST_TIMEOUT = 10

# dataclasses that hold the information of each data type
# using dict.get instead of dict[] in case the key doesn't exist
@dataclasses.dataclass
class Category:
	id: str
	name: str
	super: str
	description: str

	@staticmethod
	def from_raw(raw_data):
		return Category(raw_data.get("id"), raw_data.get("tagName"), raw_data.get("category"), \
			raw_data.get("description"))

@dataclasses.dataclass
class Entity:
	id: str
	name: str
	category: str
	website: str
	twitter: str
	crunchbase: str
	linkedin: str
	categories: str

	@staticmethod
	def from_raw(raw_data):
		categories = set(tag["id"] for tag in raw_data.get("populatedTags"))
		return Entity(raw_data.get("id"), raw_data.get("name"), raw_data.get("type"), \
			raw_data.get("website"), raw_data.get("twitter"), raw_data.get("crunchbase"), \
			raw_data.get("linkedin"), ",".join(categories))

@dataclasses.dataclass
class Address:
	address: str
	label: str
	entity: str
	chain: str
	category: str

	@staticmethod
	def list_from_raw(raw_data, category):
		'''
		 * return list of addressses for each category in
		 * the raw data's "populatedTags", because they might be
		 * in different chains
		'''
		# CHANGE SO IT WOULD MAKE DIFFERENT LINES ONLY FOR DIFFERENT CHAINS
		addr = raw_data.get("address")
		label_raw = raw_data.get("arkhamLabel")
		label = label_raw.get("name") if label_raw is not None else None
		entity_raw = raw_data.get("arkhamEntity")
		entity = entity_raw.get("id") if entity_raw is not None else None
		categories = raw_data.get("populatedTags")

		if categories == None or len(categories) == 0: # no categories to add
			return [Address(addr, label, entity, raw_data.get("chain"), category)]

		categories.append({"chain": raw_data.get("chain"), "id": category})

		return [Address(addr, label, entity, cat.get("chain"), cat.get("id")) \
		 for cat in categories]

# helpers
def dataframe_from_dataclass(data: List):
	return pd.DataFrame([dataclasses.asdict(row) for row in data])

def flatten(data: List[List]):
	return list(itertools.chain.from_iterable(data))

def get_df_with_csv(data: List, filename: str, override):
	'''
	 * returns <data> as a DataFrame. 
	 * if not <override>, reads data from <filename> and 
	 * appends the new data at the end
	'''
	df = dataframe_from_dataclass(data)
	if not override and os.path.isfile(filename):
		old_df = pd.read_csv(filename, dtype=str)
		df = pd.concat([old_df, df], ignore_index=True)

	df.fillna("", inplace=True)
	return df


def fetch_categories():
	'''
	 * fetch all categories ('tags') from Arkham API
	 * returns data as Category objects
	'''
	endpoint = f"{DOMAIN}/tag/all"

	response = requests.get(endpoint, headers=HEADERS, timeout=REQUEST_TIMEOUT)
	if not response.ok:
		raise Exception(f"request failed: {response.status_code}")

	raw_data = response.json()
	return [Category.from_raw(row) for row in raw_data]

def write_categories(categories: List[Category], filename: str, override = True):
	'''
	 * write categories to file
	'''
	df = get_df_with_csv(categories, filename, override)
	df = df.drop_duplicates("id")
	df.to_csv(filename, index=False)

def fetch_category_address_count(category: str):
	print("fetching address count")
	endpoint = f"{DOMAIN}/tag/{category}/count_addresses"
	response = requests.get(endpoint, headers=HEADERS)
	if not response.ok:
		raise Exception(f"request failed: {response.status_code}")

	return int(response.text)

def fetch_category_entity_count(category: str):
	print("fetching category count")
	endpoint = f"{DOMAIN}/tag/{category}/count_entities"
	response = requests.get(endpoint, headers=HEADERS)
	if not response.ok:
		raise Exception(f"request failed: {response.status_code}")

	return int(response.text)

# list of pages that need to be fetched again
fetch_again = []
def fetch_page_callback(page, response, category):
	'''
	 * this function is called for every async request when it finishes. 
	 * must return an object like <response>, or None
	 * if the response is successful, turn the data into object and add
	 * them to <response> under 'extracted_data'.
	 * if the response is 429 (too many requests), save the page number to
	 * fetch again later. 
	 * otherwise, throw
	'''
	if not response.ok:
		# if response.status_code == 429 or response.status_code == 500:
		print("!", end="", flush=True)
		fetch_again.append(page)
		return
		# print(f"request failed: {response.status_code}") # debug
		# raise Exception(f"request failed: {response.status_code}")
	print(".", end="", flush=True)

	raw_data = response.json()
	raw_entities = raw_data.get("entities")
	raw_addresses = raw_data.get("addresses")

	entities = [Entity.from_raw(entity) for entity in raw_entities]
	addresses = [Address.list_from_raw(addr, category) for addr in raw_addresses]

	response.extracted_data = (entities, flatten(addresses))
	return response

def fetch_category_page(category, page):
	'''
	 * make an async request to <category> on page <page>
	 * callback to fetch_page_callback() with the page number
	 * in addition to the response
	'''
	if isinstance(page, grequests.AsyncRequest):
		return page

	endpoint = f"{DOMAIN}/tag/top"
	params = {"tag": category, "page": page}

	return grequests.get(endpoint, params=params, headers=HEADERS,
	 callback=lambda response, **kwargs: \
	 fetch_page_callback(page, response, category))

def handle_fetch_error(req, err):
	print(err)
	fetch_again.append(req)
	return None

def make_all_fetches_async(fetchers):
	try:
		return list( grequests.imap(fetchers, exception_handler=handle_fetch_error, \
		 size=5 ) )
	except KeyboardInterrupt as e:
		fetch_again += fetchers
		return []
	

def fetch_from_category(category: str, start: int, end: int):
	'''
	 * fetch all entities & addresses from category. 
	 * first, async fetch the number of addresses in the category (assume the 
	 * number of entities is smaller than the number of addresses)
	 * each request returns 50 of each, if able. 
	 * try again repeatedly any that failed, up to 5 times. 
	 * then turn the result into a flat list and return them. 
	 * using async fetches because we make a lot of requests, 
	 * and we don't want to wait for each one before starting the
	 * next one
	'''
	global fetch_again
	fetch_again = []
	count_retries = 0
	fetchers = [fetch_category_page(category, page) for page in range(start, end)]

	# adding exception_handler, otherwise exceptions are silent
	# size is the number of workers. 5 because this API only allowes 5/sec
	results = make_all_fetches_async(fetchers)
	# try again all the ones that failed
	while len(fetch_again) != 0:
		if count_retries >= 5:
			print("\nenough is enough")
			break
		time.sleep(5)
		count_retries += 1

		print(f"\ntry again {len(fetch_again)}")
		fetch_again_loc = fetch_again
		fetch_again = []
		fetchers = [fetch_category_page(category, page) for page in fetch_again_loc]
		results += make_all_fetches_async(fetchers)
	print("\nfinished fetching")

	data = map(lambda res: res.extracted_data, filter(None, results))
	# returns list of (entities, addresses), so turn it into two lists
	entities = []
	addresses = []
	for entity_list, address_list in data:
		entities += entity_list
		addresses += address_list

	return (entities, addresses)

def write_entities(entities: List[Entity], filename: str, override=False):
	df = get_df_with_csv(entities, filename, override)
	# no entities with duplicate id
	# maybe merge instead of drop? probably unnecessary
	df = df.drop_duplicates("id")
	df.to_csv(filename, index=False)

def write_addresses(addresses: List[Address], filename: str, override=False):
	df = get_df_with_csv(addresses, filename, override)
	# no addresses with duplicate address, chain AND category
	df = df.drop_duplicates(["address", "chain", "category"])
	# concat rows with the same address/chain but different category
	df = df.groupby(["address", "entity", "chain"])[["label", "category"]].agg(",".join)
	df = df.reset_index()
	df.to_csv(filename, index=False)

def fetch_category_and_write(category: str, total_pages: int, results_folder):
	for start in range(0, total_pages+1, MAX_PAGES_AT_ONCE):
		end = min(total_pages, start+MAX_PAGES_AT_ONCE)
		print(f"fetch [{start}, {end})");
		entities, addresses = fetch_from_category(category, start, end)

		# print("entities head 1:") # debug
		# print(entities[0]) # debug
		# print("addresses head 1:") # debug
		# print(addresses[0]) # debug
		print("write entities")
		write_entities(entities, results_folder / "entities.csv")
		print("write addresses")
		write_addresses(addresses, results_folder / "addresses.csv")
		# give a bit of a break to the server, otherwise
		# it might send error
		time.sleep(10)


# categories already read in previous runs
# categories too large: tron-energy-delegate, early-token-holder, tron-account-created
SKIP_CATEGORIES = ["tron-energy-delegate", "early-token-holder", "tron-account-created", "tron-staker", \
	"genesis-block-address", "tron-account-creator", "donor", "burn-address", "tron-energy-delegator", \
	"airdrop-recipient", "airdrop-distributor", "coinbase-prime-custody", "tornadocash-depositor", \
	"input-data-messenger", "tornadocash-recipient", "beacon-depositor", "tokenized-eth2-staker", \
	"governance-token", "nft-marketplace", "nft", "lending-centralized", "cex", "dex-aggregator", \
	"yield", "lending-decentralized", "dex", "derivatives", "gaming", "insurance", "oracle", "privacy", \
	"stablecoin", "cdp", "brokerage", "charity", "dao", "meme", "real-world-assets", "etp", "custodian", \
	"bitcoin-suisse-custody", "swissquote-custody", "kingdom-trust-custody", "nydig-custody", "zodia-custody", \
	"gemini-custody", "cantor-fitzgerald-custody", "bitgo-custody", "komainu-custody", "copper-custody", \
	"hashkey-custody", "suspicious", "ransomware", "scam", "darkweb", "ponzi", "gambling", "hacker", \
	"ofac-sanctioned", "banned-by-usdt", "banned-by-usdc", "liquid-staking", "fund", "otc", "mev", \
	"individual", "fund-decentralized", "governance-voter", "governance-governor", "snapshot-delegate", \
	"governance-delegator", "snapshot-delegator", "governance-delegatee", "government", "crosschain-interoperability", \
	"blockchain-scaling"]
# SKIP_CATEGORIES = ["tron-energy-delegate", "early-token-holder", "tron-account-created"]

def main(super_category=None, category=None):
	results_folder = Path("./out")
	if not results_folder.is_dir():
		os.mkdir(results_folder)

	print("fetching categories")
	categories = fetch_categories()
	print("writing categories")
	write_categories(categories, results_folder / "categories.csv")

	if super_category is not None:
		categories = list(filter(lambda cat: cat.super == super_category, categories))
	if category is not None:
		categories = list(filter(lambda cat: cat.id == category, categories))
	if len(categories) == 0:
		print(f"no category found: \"{category}\" in \"{super_category}\"")


	for category in categories:
		if category.id in SKIP_CATEGORIES:
			# i've already fetched these, skip for now
			continue
		tries = 0
		while tries < 5:
			try:
				print(f"\n\nfetching from {category.id}")
				total_count = max(fetch_category_address_count(category.id), \
					fetch_category_entity_count(category.id))
				total_pages = math.ceil(float(total_count) / PAGE_SIZE)
				print(f"count = {total_count}, pages = {total_pages}")

				fetch_category_and_write(category.id, total_pages, results_folder)
				break
			except Exception as e:
				print(e)
				tries += 1

	print("\nFINISHED ALL.");

def print_categories_count():
	print("fetching categories")
	categories = fetch_categories()
	categories_counts = {}

	for category in categories:
		print(f"\n\nfetching from {category.id}")
		address_count = fetch_category_address_count(category.id)
		entity_count = fetch_category_entity_count(category.id)
		total_pages = math.ceil(float(max(address_count, entity_count)) / PAGE_SIZE)
		categories_counts[category.id] = (address_count, entity_count, total_pages)
		time.sleep(1)

	print("fetched all")
	for category, counts in categories_counts.items():
		address_count, entity_count, total_pages = counts
		print(f"{category} -> count = {address_count} addresses, {entity_count} entities, pages = {total_pages}")

if __name__ == '__main__':
	# asyncio.run(main())
	if len(sys.argv) not in [1, 2, 3]:
		print(f"USAGE: {sys.argv[0]} [((super|category) <id> | count)]")
		quit()

	if len(sys.argv) == 1:
		main()
	elif sys.argv[1] == "super":
		main(super_category=sys.argv[2])
	elif sys.argv[1] == "category":
		main(category=sys.argv[2])
	elif sys.argv[1] == "count":
		print_categories_count()
	else:
		print(f"USAGE: {sys.argv[0]} [(super|category) <id> | count]")
