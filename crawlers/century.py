from sgrequests import SgRequests
from bs4 import BeautifulSoup as bs
import pdb
import os
from pathvalidate import sanitize_filename
import math
from concurrent.futures import ThreadPoolExecutor
import csv
import re
import time

max_workers = 1

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

journal_titles = [
    "AMERICAN JOURNAL OF CANCER RESEARCH",
    "AMERICAN JOURNAL OF TRANSLATIONAL RESEARCH",
]

eissns = [
    "2156-6976",
    "1943-8141",
]

links = [
    "https://e-century.us/web/journal_toc.php?journal=ajcr",
    "https://e-century.us/web/journal_toc.php?journal=ajtr",
]

domain = "https://e-century.us/web/"
BASE_PATH = "/media/com/C69A54D99A54C817/pwj/"
proxy_url = "https://api.scraperapi.com?api_key=3664a7c3***ecd461ad4c8bf0&url="

csv_header = ['Title', 'Source', 'Subject', 'Sub Category', 'Type', 'Authors', 'Published At', 'Published Year', 'Abstract', 'File Path', 'PDF URL', 'Article URL', 'Created At', 'Updated At']
csv_name = "e-century"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "E-CENTURY PUBLISHING CORP"

def fetch_url(url):
    with SgRequests() as s:
        try:
            return s.get(f"{url}", headers=_headers)
        except:
            pdb.set_trace()

def create_dir(dir_name):
    dir_path = os.path.join(BASE_PATH, dir_name)
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

def filter_out_2019(tr):
    year = -1
    a = tr.a
    if 'Volume' in a.text:
        year = a.text.strip().split('-')[0].strip()

    try:
        return int(year)
    except:
        return int(year.split()[-1].strip())

def get_page_data(page_source):
    return page_source.select('div.representation.overview.search div.product-listing-with-inputs-content ul.details')

def fetchSingle(base_url, journal_title, issn):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')
    volumes = res.select('div#welcome ul')

    for volume in volumes:
        try:
            year = int(volume.find_previous_sibling('h5').text.split(',')[-1].strip())
        except Exception as error:
            print(f'[year] {error}')
            continue
        print(f"[{start_url}] {year}")
        if year > 2018:
            numbers = volume.select("li a")
            for number in numbers:
                issue_url = domain + number['href']
                page_source = bs(fetch_url(issue_url).text, 'lxml')
                papers = page_source.select("div#welcome ul li")

                print(f"[{volume.text}] [{year}] [{issue_url}] {len(papers)}")
                for paper in papers:
                    parse_paper_info(paper, journal_title, issn, year)

def fetch_all(list, journal_titles, eissns, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(fetchSingle, list, journal_titles, eissns):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def export_csv():
    new_csv_data = []
    for data in csv_data:
        if data['File Path'] in failed_files:
            continue
        data.pop('download_url')
        data.pop('file_name')
        new_csv_data.append(data)

    print('[export csv] total: ', len(new_csv_data), ', failed files: ', len(failed_files))
    with open(f'../output/{csv_name}.csv', 'w', encoding='UTF8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_header)
        writer.writeheader()
        writer.writerows(new_csv_data)

def read_csv_data():
    print("[read_csv_data]")
    with open(f'../output/{csv_name}.csv', 'r') as data:
        for line in csv.DictReader(data):
            file_name = sanitize_filename(line['Title']) + '.pdf'
            line['file_name'] = file_name
            line['download_url'] = line['PDF URL']
            csv_data.append(line)

def fetch_pdfs(pdf_list, path_list, occurrence=max_workers):
    output = []
    total = len(pdf_list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(download, pdf_list, path_list):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def download(pdf_link, file_path):
    file_path = os.path.join(BASE_PATH, file_path)
    if os.path.exists(file_path):
        return
    print('--- downloading --- ', file_path, pdf_link)
    try:
        with SgRequests() as s:
            new_res = s.get(f"{pdf_link}", headers=_headers)
            with open(file_path, "wb") as download_file:
                with s.stream(new_res.url) as response:
                    for chunk in response.iter_bytes():
                        download_file.write(chunk)
                        
            time.sleep(1)
    except Exception as err:
        print('[Download error]: ', err)
        failed_files.append(file_path)

    return True

def download_pdfs_via_thread():
    for data in csv_data:
        pdfs.append(data['download_url'])
        file_paths.append(data['File Path'].replace(".pdf", "")[:255] + ".pdf")

    print('[start downloading] total: ', len(csv_data))
    fetch_pdfs(pdfs, file_paths)

def parse_paper_info(paper, journal_title, issn, published_year):
    if not paper.find('a', href=re.compile(r"pdf$")):
        return

    pdf_url = paper.find('a', href=re.compile(r"pdf$"))['href']
    article_url = pdf_url
    title = [ch for ch in paper.children if ch][1].split(';')[0].strip()
    authors = paper.strong.text.replace(':', '').strip()
    abstract = ''
    published_at = published_year
    file_name = sanitize_filename(title) + '.pdf'
    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    csv_data.append({
        'Title': title, 
        'Source': PUBLISHER_NAME, 
        'Subject': journal_title, 
        'Sub Category': '', 
        'Type': '', 
        'Authors': authors, 
        'Published At': published_at, 
        'Published Year': published_year, 
        'Abstract': abstract,
        'File Path': file_path, 
        'PDF URL': pdf_url, 
        'Article URL': article_url, 
        'Created At': '',
        'Updated At': '',
        'download_url': pdf_url,
        'file_name': file_name
    })

    
def start():
    fetch_all(links, journal_titles, eissns)
        
if __name__ == '__main__':
    # start()

    # export_csv()

    read_csv_data()

    download_pdfs_via_thread()