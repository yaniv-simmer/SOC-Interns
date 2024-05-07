import pandas as pd
import dataclasses
import json
import codecs
import sys

def main(filename: str):
	tweets = []
	with codecs.open(filename, "r", encoding="utf-8") as file:
		tweets = json.load(file)

	# complete here

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print(f"USAGE: {sys.argv[0]} json_file")

	main(sys.argv[1])
