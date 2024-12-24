import argparse
import os
import re
from warcio.archiveiterator import ArchiveIterator

def extract_links_from_file(file_path, file_format):
    if file_format.lower() == "wet":
        return extract_links_from_wet(file_path)
    elif file_format.lower() == "warc":
        return extract_links_from_warc(file_path)
    else:
        raise Exception(f"ERROR: File format {file_format} not supportes")

def extract_links_from_warc(warc_file_path):
    links = []

    with open(warc_file_path, 'rb') as f:
        for record in ArchiveIterator(f):
            if record.rec_type == 'response':
                payload = record.payload.read().decode(errors='ignore')
                links.extend(extract_urls_from_html(payload))
    return links


def extract_urls_from_html(html_content):
    urls = re.findall(r'href=["\'](https?://[^\s"\'<>]+)', html_content)
    return urls

def extract_links_from_wet(file_path):
    links = []

    with open(file_path, 'rb') as f:
        for record in ArchiveIterator(f):
            if record.rec_type == 'conversion':
                content = record.content_stream().read().decode(errors='ignore')
                links.extend(extract_urls_from_text(content))
    return links

def extract_urls_from_text(text_content):
    url_regex = re.compile(r'https?://[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+(?:/[a-zA-Z0-9_-]*)?')
    urls = url_regex.findall(text_content)
    return urls


def parse_args():
    parser = argparse.ArgumentParser(description="Download and process files from Common Crawl.")
    parser.add_argument('--source', default='../../data/raw/',
                        help='Path to the source file (default is set).')
    parser.add_argument('--destination', default='../../data/extracted/', help='Directory to store extracted data.')
    parser.add_argument('--format', choices=['warc', 'wet', 'wat'], default='warc',
                        help='Format of the data to download.')
    parser.add_argument('--max-filtered', type=int, default=0, help='Limit the number of filtered paths to download.')
    return parser.parse_args()

def extract_crawl_data_version(path):
    pattern = r"CC-MAIN-\d{4}-\d+"
    match = re.search(pattern, path)

    if match:
        return match.group()
    else:
        return None

def main():
    args = parse_args()

    if not os.path.exists(args.destination):
        os.makedirs(args.destination)

    file_format = args.format.upper()
    files_to_process = []
    for root, dirs, files in os.walk(args.source):
        for file in files:
            if file.endswith(f'.{args.format}'):
                files_to_process.append(os.path.join(root, file))

    if not files_to_process:
        print(f"No files found with the {file_format} format in {args.source}")
        return

    for file_to_process in files_to_process:
        print(f"Processing the {file_format.upper()} file: {file_to_process}")

        links = extract_links_from_file(file_to_process, file_format)
        print(f"Links extracted: {len(links)}")

        crawl_data_version = extract_crawl_data_version(file_to_process)

        relative_path = os.path.relpath(file_to_process, args.source)
        output_path = os.path.join(args.destination, os.path.splitext(relative_path)[0] + '.txt')

        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_path, 'w') as f:
            for link in links:
                f.write(f"{link},{crawl_data_version}\n")
        print(f"Links saved to {output_path}")
        print("Extration completed!")

if __name__ == '__main__':
    main()
