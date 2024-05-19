import sys
import pandas as pd


def main(filename: str):
	df = pd.read_csv(filename)
	df.fillna("", inplace=True)
	# split joined labels into different rows
	df.label = df.label.str.split(",")
	df = df.explode("label")
	# drop duplicate labels
	df = df.drop_duplicates(["address", "chain", "label"])
	# reconnect remaining labels
	df = df.groupby(["address", "entity", "chain", "category"])["label"].agg( \
		lambda labels: ",".join(filter(None, labels)) )
	df = df.reset_index()
	# write to file
	df.to_csv(filename.replace(".csv", ".fixed.csv"), index=False)


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print(f"USAGE: {sys.argv[0]} <csv file path>")
		quit()

	main(sys.argv[1])
