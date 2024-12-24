import argparse
import os
from pgcopy import CopyManager
import psycopg2

DB_CONFIG = {  # TODO Read from config
    'dbname': 'de_challenge',
    'user': 'user',
    'password': 'password',
    'host': 'postgres',
    'port': '5432'
}

folder_path = '../extraction/processed_data'

def get_rows_from_txt(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file if line.strip()]

def insert_urls_to_db_batch(rows):
    if not rows:
        return

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        truncate_sql = f"TRUNCATE TABLE external_links;"
        cur = conn.cursor()
        cur.execute(truncate_sql)
        conn.commit()

        copy_mgr = CopyManager(conn, 'external_links', ['url','crawl_data_version'])
        copy_mgr.copy([(row.split(',')[0], row.split(',')[1]) for row in rows])
        conn.commit()
    except Exception as e:
        print(f"Error while inserting Rows: {e}")
        raise e
    finally:
        conn.close()


def process_folder(folder_path):
    for root, _, files in os.walk(folder_path):
        for file in files:
            print(file)
            if file.endswith('.txt'):
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                rows = get_rows_from_txt(file_path)
                insert_urls_to_db_batch(rows)


def parse_args():
    parser = argparse.ArgumentParser(description="Download and process files from Common Crawl.")
    parser.add_argument('--source', default='../../data/extracted/',
                        help='Path to the source file (default is set).')
    return parser.parse_args()


def main():
    args = parse_args()
    process_folder(args.source)
    print("Loading completed!")

if __name__ == '__main__':
    main()
