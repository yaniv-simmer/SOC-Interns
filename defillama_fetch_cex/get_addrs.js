const fs = require("fs")
const {stringify} = require("csv-stringify/sync");

const cex_list = require("./cex.json")

let missing = []
const result_filename = "cex_data.csv";

let translated = {
	Firi: "fire", 
	"WOO X": "woo-cex", 
	BitMart: "bitmark", 
	"HashKey Global": "hashkey", 

}

function fix_index_file(filename) {
	fs.writeFileSync(filename, "\nmodule.exports.config = config;\n", {flag: "a"});
}

async function get_data_from_index_file(filename, exchange_name) {
	let raw_data = (await import(filename)).config;
	let transformed_data = [];

	for (let [chain, chain_data] of Object.entries(raw_data)) {
		let chain_owners = chain_data.owners;
		let transformed_owners = chain_owners.map((addr) => ({
			exchange: exchange_name,
			address: addr, 
			chain
		}) );

		transformed_data = transformed_data.concat(transformed_owners);
	}

	return transformed_data;
}

function write_data_to_results(data, is_first) {
	let data_str = stringify(data, {header: is_first});
	fs.writeFileSync(result_filename, data_str, {flag: "a"});
}

function get_folder_name(exchange) {
	if (translated[exchange.name])
		return `./DefiLlama-Adapters/projects/${translated[exchange.name]}`
	let options = [
		exchange.cgId, 
		exchange.cgId + "-cex", 
		exchange.name.toLowerCase(), 
		exchange.name.toLowerCase() + "-cex", 
	]

	if (exchange.slug) {
		options.push(exchange.slug.toLowerCase()); 
		options.push(exchange.slug.toLowerCase() + "-cex")
	}

	for (let name of options) {
		console.log(`try ${name}`)
		if (fs.existsSync(`./DefiLlama-Adapters/projects/${name}`))
			return `./DefiLlama-Adapters/projects/${name}`
	}

	return undefined;
}

async function main() {
	let is_first = true;
	for (let exchange of cex_list) {
		console.log(`reading ${exchange.name}`);

		let folder_name = get_folder_name(exchange);
		if (!folder_name)
		{
			console.log("\tdoesn't exist");
			missing.push(exchange.name);
			continue;
		}

		let file_name = folder_name + "/index.js";

		fix_index_file(file_name);

		let data = await get_data_from_index_file(file_name, exchange.name);
		// if (exchange.name == "Binance")
		// 	console.log(data);

		write_data_to_results(data, is_first);
		is_first = false;
	}

	console.log("\nMissing:");
	for (let name of missing) {
		console.log(name);
	}
}

main().catch((e) => console.log(e));
