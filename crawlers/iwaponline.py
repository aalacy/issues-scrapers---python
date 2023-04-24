import httpx
import requests
from sgrequests import SgRequests
from bs4 import BeautifulSoup as bs
import pdb
import os
from pathvalidate import sanitize_filename
import math
from concurrent.futures import ThreadPoolExecutor
import csv
import shutil
import re
import time
from tenacity import retry, stop_after_attempt, wait_exponential
from urllib.parse import urlparse

max_workers = 1

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

header1 = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Host": "iwaponline.com",
}

journal_titles = [
    "AQUA-WATERINFRASTRUCTURE ECOSYSTEMS AND SOCIETY", 
    "HYDROLOGY RESEARCH",
    "JOURNAL OF HYDROINFORMATICS",
    "JOURNAL OF WATER AND HEALTH",
    "JOURNAL OF WATER SANITATION AND HYGIENE FOR DEVELOPMENT",
    "WATER POLICY",
    "WATER QUALITY RESEARCH JOURNAL",
    "WATER REUSE",
    "WATER SCIENCE AND TECHNOLOGY",
    "WATER SUPPLY",
]

eissns = [
    "2709-8036",
    "2224-7955",
    "1465-1734",
    "1996-7829",
    "2408-9362",
    "1996-9759",
    "2709-8052",
    "2709-6106",
    "1996-9732",
    "1607-0798",
]

links = [
    "https://iwaponline.com/aqua",
    "https://iwaponline.com/hr",
    "https://iwaponline.com/jh",
    "https://iwaponline.com/jwh",
    "https://www.iwaponline.com/washdev",
    "https://iwaponline.com/wp",
    "https://iwaponline.com/wqrj",
    "https://iwaponline.com/jwrd",
    "https://iwaponline.com/wst",
    "https://iwaponline.com/ws",
]

domain = "https://iwaponline.com"
BASE_PATH = "/home/com/SData/"
proxy_url = "https://api.scraperapi.com?api_key=3664a7c3***ecd461ad4c8bf0&url="

csv_header = ['Title', 'Source', 'Subject', 'Sub Category', 'Type', 'Authors', 'Published At', 'Published Year', 'Abstract', 'File Path', 'PDF URL', 'Article URL', 'Created At', 'Updated At']
csv_data = []
pdfs = []
file_paths = []
failed_files = []
csv_name = "iwaponline"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_url(url):
    with SgRequests() as s:
        try:
            res = s.get(f"{proxy_url}{url}", headers=_headers)
            if res.status_code != 200:
                print(f'[retry] {url}')
                raise Exception

            return res
        except Exception as error:
            print(error)

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
    start_url = base_url + '/issue/browse-by-year'
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')
    years = res.select('div.widget-ContentBrowseAllYearsManifest ol li a')

    for _year in years:
        year = int(_year.text.strip())
        print(f"[{start_url}] {year}")
        if year > 2018:
            volumes = bs(fetch_url(_year['href']).text, 'lxml').select("div.widget-ContentBrowseByYearManifest ol li a")
            for volume in volumes:
                issue_url = domain + volume['href']
                page_source = bs(fetch_url(issue_url).text, 'lxml')
                papers = page_source.select("div.al-article-item-wrap")

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

def get_redirect(pdf_link):
    count = 0
    while True:
        with requests.Session() as s:
            new_res = s.head(f"{pdf_link}", headers=header1, allow_redirects=True, proxies={"http": "5.79.66.2:13080", "https": "5.79.66.2:13080"})
            if urlparse(str(new_res.url)).netloc != urlparse(pdf_link).netloc:
                return new_res
            time.sleep(1)
            count += 1
            print(f'[get_redirect] [{count}] {new_res.url}')

def download(pdf_link, file_path):
    file_path = os.path.join(BASE_PATH, file_path)
    if os.path.exists(file_path):
        return
    print('--- downloading --- ', file_path, pdf_link)
    try:
        with httpx.Client(proxies={"http://": "http://5.79.73.131:13080", "https://": "http://5.79.73.131:13080"}) as s:
            new_res = get_redirect(pdf_link)
            new_res.url
            with open(file_path, "wb") as download_file:
                with s.stream('GET', new_res.url) as response:
                    for chunk in response.iter_bytes():
                        download_file.write(chunk)

        time.sleep(10)
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
    if not paper.select_one('i.js-access-icon-placeholder'):
        return

    if not paper.select_one('a.al-link.pdf'):
        return

    article_url = domain + paper.select_one('h5 a')['href']
    title = paper.select_one('h5.item-title').text.strip()
    pdf_url = domain + paper.select_one('a.al-link.pdf')['href']
    authors = ""
    if paper.select_one('div.al-authors-list'):
        authors = paper.select_one('div.al-authors-list').text.strip()
    abstract = ''
    published_at = published_year
    file_name = sanitize_filename(title) + '.pdf'
    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    csv_data.append({
        'Title': title, 
        'Source': 'IWA PUBLISHING', 
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