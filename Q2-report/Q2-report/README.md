# Blockchain Security Data Scraping

This repository contains Python scripts for scraping and collecting data related to blockchain security incidents. The scripts extract data from various sources, including BlockSec, DeFi Rekt, and DeFi Llama, to build a comprehensive database of security events in the blockchain space.


## Overview

The goal of this project is to provide a reliable and up-to-date dataset of blockchain security incidents. The collected data can be used for analysis, research, and enhancing the security of blockchain applications.

## Features

- **BlockSec Scraper**: Extracts data from BlockSec's incident reports.
- **DeFi Rekt Scraper**: Gathers information from DeFi Rekt's database.
- **DeFi Llama Scraper**: Retrieves hack data from DeFi Llama's records.

## Structure

The repository is structured as follows:

- `blocksec_scrape.py`: Script for scraping data from BlockSec.
- `defi_rekt_database_scrape.py`: Script for scraping data from DeFi Rekt.
- `defillama_hacks_scrape.py`: Script for scraping data from DeFi Llama.
- `main.py`: Main script to run all scrapers.
- `README.md`: This file, providing an overview and usage instructions.
- `LICENSE`: License file for the project.

## Requirements

- Python 3.7+
- Requests library
- BeautifulSoup library
- Pandas library

Install the required libraries using:

```bash
pip install requests beautifulsoup4 pandas
```

## Usage
Clone the repository:
```bash
git clone https://github.com/yourusername/blockchain-security-scraping.git
cd blockchain-security-scraping
```
Run the main script to execute all scrapers:
```bash
python main.py
```
Each scraper script can also be run individually if needed:

```bash
python blocksec_scrape.py
python defi_rekt_database_scrape.py
python defillama_hacks_scrape.py
```
