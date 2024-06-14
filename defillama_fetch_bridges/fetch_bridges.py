import pandas as pd
import requests
import json
import dataclasses
import re

HEADERS = {
	# "Referer": "https://defillama.com/bridges", 
	# "Sec-Ch-Ua": "\"Google Chrome\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
	# "Sec-Ch-Ua-Mobile": "?0", 
	# "Sec-Ch-Ua-Platform": "\"Windows\"", 
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0", 
	# "x-nextjs-data": "1", 
	# "Host": "defillama.com", 
	"Priority": "u=4", 
	# "purpose": "prefetch", 
	"Sec-Fetch-Dest": "document", 
	"Sec-Fetch-Mode": "navigate", 
	"Sec-Fetch-Site": "none", 
	"Sec-Fetch-User": "?1", 
	"Upgrade-Insecure-Requests": "1",
	# "TE": "trailers", 
	"Accept": "*/*", 
	"Accept-Encoding": "gzip, deflate, br, zstd", 
	"Accept-Language": "en-US,en;q=0.5", 
	"Connection": "keep-alive", 
	# "If-None-Match": "\"b579b9u10d34ot\"", 
}

@dataclasses.dataclass
class Entity:
	name: str
	display_name: str

	def __hash__(self):
		return hash(f"{self.name}#{self.display_name}")

@dataclasses.dataclass
class Address:
	entity: Entity
	address: str
	chain: str

	@staticmethod
	def from_address_data(entity, data):
		chain, address = data["address"].split(":")
		return Address(entity, address, chain)

def read_bridges():
	# with open("bridges.json", "r") as file:
	# 	data = json.load(file)

	# bridges = data["bridges"]
	# return [Entity(b["name"], b["displayName"]) for b in bridges]

	with open("bridges.html", "r") as file:
		data = file.read()

	bridges_raw = re.findall(r"<a href=\"/bridge/(.+?)\" class=\"sc-8c920fec-3 dvOTWR\">(.+?)</a>", data)

	return [Entity(b[0], b[1]) for b in bridges_raw]

def get_bridge_addresses(bridge):
	name = bridge.name
	print(f"fetching from {name}")
	endpoint = f"https://defillama.com/_next/data/2a3681df91cd6df0e606da4520cfd82770614438/bridge/{name}.json"

	response = requests.get(endpoint, params={"bridge": name}, headers=HEADERS)
	print(response.url)
	if not response.ok:
		raise RuntimeError(f"Response status {response.status_code}")

	data = response.json()
	address_data = data["pageProps"]["tableDataByChain"]["allChains"]["addressesTableData"]

	return [Address.from_address_data(bridge, address) for address in address_data]

def add_addresses_to_dataframe(df, addresses):
	addresses_data = [{"Bridge": addr.entity.display_name, "Address": addr.address, "Chains": addr.chain} for addr in addresses]

	new_df = pd.DataFrame(addresses_data)
	return pd.concat((df, new_df), ignore_index=True)


def main():
	bridges = read_bridges()
	bridges = list(set(bridges))
	df = pd.DataFrame([{"name": b.name, "display_name": b.display_name} for b in bridges])

	# df = pd.DataFrame(columns=["Bridge", "Address", "Chains"])

	# for bridge in bridges:
	# 	bridge_addresses = get_bridge_addresses(bridge)
	# 	df = add_addresses_to_dataframe(df, bridge_addresses)

	df.to_excel("bridges.xlsx")


if __name__ == '__main__':
	main()
