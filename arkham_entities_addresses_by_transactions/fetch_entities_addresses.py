import json
import pandas as pd
import requests
from dataclasses import dataclass
from typing import Set
import datetime
import dateutil.parser
import math
import time

ENDPOINT = "https://api.arkhamintelligence.com/transfers"

HEADERS = {
	"API-Key": "qhN0gnXcxf8G3Z1RqSuTZB7uJdjRgq0E"
}

EARLIEST_TIME = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)

TRANSACTION_FETCH_COUNT = 250

@dataclass
class Address:
	address: str
	chain: str

	@staticmethod
	def from_transaction_side(transaction_side):
		return Address(transaction_side["address"], transaction_side["chain"])

	def __hash__(self):
		return hash(self.address) * 31 + hash(self.chain)

@dataclass
class FetchResults:
	addresses: Set[Address]
	min_time: datetime.datetime
	max_time: datetime.datetime
	count: int

def address_from_transaction(entity, transaction) -> Address:
	try:
		for direction in ["fromAddress", "toAddress"]:
			if direction in transaction and transaction[direction] is not None and\
			 "arkhamEntity" in transaction[direction] and\
			 transaction[direction]["arkhamEntity"]["id"] == entity:
				return Address.from_transaction_side(transaction[direction])

		print("!!!!transaction error!!!!")
		print(transaction)
		raise ValueError(f"transaction does not fit entity {entity}")
	except TypeError as e: # debug
		print(transaction)
		raise e

def get_transactions(entity: str, max_time: datetime.datetime | None = None) -> FetchResults:
	params = {"base": entity, "flow": "all", "sortDir": "desc", "sortKey": "time", "limit": TRANSACTION_FETCH_COUNT}
	if max_time is not None:
		params["timeLte"] = math.ceil(max_time.timestamp())

	response = requests.get(ENDPOINT, params=params, headers=HEADERS)
	if not response.ok:
		raise RuntimeError(f"Request failed with status code {response.status_code}")
	print(".", end="", flush=True)

	data = response.json()
	transfers = data["transfers"]
	if data["count"] == 0:
		return FetchResults(set(), None, None, 0)

	min_time = min(dateutil.parser.isoparse(transaction["blockTimestamp"]) for transaction in transfers)
	max_time = max(dateutil.parser.isoparse(transaction["blockTimestamp"]) for transaction in transfers)
	addresses = set(address_from_transaction(entity, transaction) for transaction in transfers)

	return FetchResults(addresses, min_time, max_time, data["count"])

def get_entity_addresses(entity: str) -> Set[Address]:
	addresses = set()
	results = get_transactions(entity)
	while results.count == TRANSACTION_FETCH_COUNT and EARLIEST_TIME < results.min_time:
		addresses = addresses.union(results.addresses)
		time.sleep(3) # for rate limitation
		results = get_transactions(entity, max_time=results.min_time)

	addresses = addresses.union(results.addresses)
	print(f"\nfinished {entity}")
	return addresses

def main(targets_filename: str, entities_filename: str):
	with open(targets_filename, "r") as file:
		targets = json.load(file)
	targets = [t["name"] for t in targets]

	entities_df = pd.read_csv(entities_filename)
	target_entities_df = entities_df[entities_df.name.isin(targets)]
	results_df = pd.read_excel("addresses.xlsx")

	try:
		for entity in target_entities_df.id:
			if entity in results_df.entity.values:
				continue;

			print(f"fetching for {entity}")
			addresses = get_entity_addresses(entity)
			print(f"got {len(addresses)} new rows")
			addresses_df = pd.DataFrame([{
				"entity": entity,
				"address": addr.address, 
				"chain": addr.chain
				}
				for addr in addresses
			])

			results_df = pd.concat((results_df, addresses_df), ignore_index=True)
			print(f"{len(results_df)} total rows")
			time.sleep(10)
	finally:
		if len(results_df.index) > 0:
			results_df.to_excel("addresses.xlsx", index=False)


if __name__ == '__main__':
	main("entities.json", "entities.csv")
