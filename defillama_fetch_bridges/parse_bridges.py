import json
import pandas as pd
import os

'''
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * JSON files were collected manually from https://defillama.com/bridge/[name]
 * under tag <script id="__NEXT_DATA__" type="application/json">
 * name should be the smaller verison, spaces turned to dashes
 * sometimes you can get them from the endpoint:
 * https://defillama.com/_next/data/434b71dd9019b92008d4a8150395983b71573162/bridge/[name].json?bridge=[name]
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
'''

def split_address(addr: str):
	chain, address = addr["address"].split(":")
	return {"chain": chain, "address": address}

def extract_data(data):
	if "props" in data:
		data = data["props"]
	if "pageProps" in data:
		data = data["pageProps"]

	name = data["displayName"]
	addresses_data = data["tableDataByChain"]["All Chains"]["addressesTableData"]

	addresses = [split_address(addr) for addr in addresses_data]

	return [{"Bridge": name, "Address": addr["address"], "Chain": addr["chain"]} for addr in addresses]

def main(folderpath: str):
	df = pd.DataFrame(columns=["Bridge", "Address", "Chain"])
	directory = os.fsencode(folderpath)
	
	for file in os.listdir(directory):
		filename = os.fsdecode(file)
		if not filename.endswith(".json"):
			continue
		
		filepath = os.path.join(os.fsdecode(directory), filename)
		print(filepath)
		with open(filepath, "r") as json_file:
			entity_data = extract_data(json.load(json_file))

			df = pd.concat( (df, pd.DataFrame(entity_data)), ignore_index=True)

	df.to_excel("bridges_addresses.xlsx", index=False)



if __name__ == '__main__':
	main(".\\raw")
