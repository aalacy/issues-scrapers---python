import os
import math
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
from pathvalidate import sanitize_filename
import re
import pdb

from util import (
    fetch_url, 
    create_dir, 
    export_csv, 
    read_csv_data, 
    download_pdfs_via_thread,
    set_up_selenium_webdriver,
    download_via_selenium
)

max_workers = 6

domain = "https://www.biosciencetrends.com"

csv_name = "biosciencetrends"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "FOUNDATION REHABILITATION INFORMATION"

journal_titles = [
    "BIOSCIENCE TRENDS"
]

eissns = [
    "1881-7823"
]

links = [
    "https://www.biosciencetrends.com/archives"
]

subjects = [
    "Biology"
]

def parse_paper_info(paper, journal_title, issn, published_year, subject):
    article_url = paper.a['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = paper.a.text.strip()
    pdf_url = paper.find('a', href=re.compile(r"downloadpdf/"))['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url
    authors = ""
    if paper.select_one('p.author'):
        authors = paper.select_one('p.author').text.strip()
    abstract = ''
    published_at = published_year
    file_name = sanitize_filename(title) + '.pdf'
    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    csv_data.append({
        'Title': title, 
        'Source': PUBLISHER_NAME, 
        'Subject': subject, 
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
    

def parse_volumes(volumes, journal_title, issn, subject):
    fetch_all(volumes, journal_title, issn, subject)

def parse_archive(base_url, journal_title, issn, subject):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')

    volumes = None
    _vols = res.select("div.article_content div.panel-group div.panel")
    volumes = [volume for volume in _vols if filter_year(volume)]
    print(f"{len(volumes)} volumes")
    parse_volumes(volumes, journal_title, issn, subject)

def filter_year(volume):
    year = volume.h4.text.strip()
    if not year:
        print(f"[no year] {volume['href']}")
        return False
    try:
        year = int(year)
        if year < 2019:
            return False
    except Exception as error:
        print(f'[error] [year] {error}')
        if 'Advanced' not in year:
            return False

    return year

def fetch_single(volume, journal_title, issn, subject):
    year = filter_year(volume)
    if not year:
        return
    issues = volume.select("div.panel-body > div.row > div")
    for issue in issues:
        issue_url = domain + issue.a['href']
        papers = bs(fetch_url(issue_url).text, 'lxml').select("div.doc_main li.media")
        print(f'[issue] [{issue_url}] {len(papers)} papers')
        for paper in papers:
            parse_paper_info(paper, journal_title, issn, year, subject)

def fetch_all(list, journal_title, eissn, subject, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(fetch_single, list, [journal_title] * len(list), [eissn] * len(list), [subject] * len(list)):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x], subjects[x])
        
if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)
    # set_up_selenium_webdriver(BASE_PATH)

    # download_via_selenium(BASE_PATH, csv_data)