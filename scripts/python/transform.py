import argparse
import os
import urllib
from datetime import datetime
from re import search

import dask.dataframe as dd
import unicodedata
from dask.dataframe import DataFrame
from typing import List
from urllib.parse import urlparse, urlunparse

DB_CONFIG = {  # TODO Read from config
    'dbname': 'de_challenge',
    'user': 'user',
    'password': 'password',
    'host': 'postgres',
    'port': '5432'
}

country_tlds = {
    '.it': 'Italy',
    '.fr': 'France',
    '.de': 'Germany',
    '.uk': 'United Kingdom',
    '.us': 'United States',
    '.ca': 'Canada',
    '.jp': 'Japan',
    '.br': 'Brazil',
    '.in': 'India',
    '.es': 'Spain',
    '.ru': 'Russia',
    '.au': 'Australia',
    '.mx': 'Mexico',
    '.cn': 'China',
    '.za': 'South Africa',
}

def is_ad_based(category: str):
    if category == 'Unknown':
        return True
    else:
        return False

def is_home_page(url: str):
    parsed_url = urlparse(url)
    return parsed_url.path in ['', '/']

def normalize_url(url: str):
    try:
        parsed_url = urlparse(url)
        normalized_netloc = unicodedata.normalize('NFKC', parsed_url.netloc)
        normalized_url = urlunparse((parsed_url.scheme, normalized_netloc, parsed_url.path,
                                     parsed_url.params, parsed_url.query, parsed_url.fragment))

        return normalized_url
    except Exception as e:
        return None

def get_primary_link(url: str):
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}"

def extract_domain(url: str):
    parsed_url = urllib.parse.urlparse(url)
    return parsed_url.netloc

def extract_country_from_domain(domain: str):
    # Mock
    match = search(r'\.([a-z]{2,})$', domain)

    if match:
        tld = '.' + match.group(1)
        return country_tlds.get(tld, 'Unknown')
    else:
        return 'Invalid domain'

def categorize_website(url: str):
    # Mock
    categories = ['News', 'Technology', 'Entertainment', 'Unknown', 'Unknown']
    return categories[hash(url) % len(categories)]

def take_n_rows(partition, n=10):
    return partition.iloc[:n]

def write_data(ddf: DataFrame, partitions: List[str], destination: str):
    current_datetime = datetime.now()
    ddf['run_ts'] = int(current_datetime.timestamp())
    ddf['run_date'] = str(current_datetime.date())
    ddf.to_parquet(destination,
                   partition_on=partitions,
                   compression='snappy')


def apply_transformations(ddf):
    ddf['is_home_page'] = ddf['url'].apply(is_home_page, meta=('is_home_page', 'bool'))
    ddf['primary_link'] = ddf['url'].apply(get_primary_link, meta=('primary_link', 'object'))
    ddf['domain'] = ddf['primary_link'].map(extract_domain, meta=('domain', 'str'))

    agg_ddf = ddf.groupby('domain').agg(
        subsections=('url', 'count'),
        home_page_count=('is_home_page', 'sum')
    ).reset_index()

    joined_ddf = agg_ddf.merge(ddf, on='domain', how='inner')

    joined_ddf['country'] = joined_ddf['domain'].map(extract_country_from_domain, meta=('country', 'str'))
    joined_ddf['category'] = joined_ddf['domain'].map(categorize_website, meta=('category', 'str'))
    joined_ddf['is_ad_based'] = joined_ddf['category'].map(is_ad_based, meta=('is_ad_based', 'bool'))

    return joined_ddf


def load_table(table_name, connection_string):
    return dd.read_sql_table(table_name, connection_string, index_col='id')

def parse_args():
    parser = argparse.ArgumentParser(description="Transform data")
    parser.add_argument('--destination', default='../../data/output', help='Directory to store transformed data.')
    return parser.parse_args()

def main():
    args = parse_args()

    if not os.path.exists(args.destination):
        os.makedirs(args.destination)

    username = DB_CONFIG['user']
    password = DB_CONFIG['password']
    host = DB_CONFIG['host']
    port = DB_CONFIG['port']
    database = DB_CONFIG['dbname']

    connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
    table_name = "external_links"

    ddf = load_table(table_name, connection_string)
    transformed_ddf = apply_transformations(ddf)
    write_data(transformed_ddf, ['crawl_data_version', 'country', 'category'], args.destination)
    print("Transform completed!")

if __name__ == "__main__":
    main()
