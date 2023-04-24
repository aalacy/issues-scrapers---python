import os
import math
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
from pathvalidate import sanitize_filename
import re
import time

from util import (
    fetch_url, 
    create_dir, 
    export_csv, 
    read_csv_data, 
    download_pdfs_via_thread,
    set_up_selenium_webdriver,
    download_via_selenium
)

max_workers = 12

domain = "https://sciencepress.mnhn.fr"

csv_name = "sciencepress"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "PUBLICATIONS SCIENTIFIQUES DU MUSEUM, PARIS"

journal_titles = [
    "COMPTES RENDUS PALEVOL",
    "ADANSONIA",
    "ZOOSYSTEMA"
]

eissns = [
    "1777-571X",
    "1639-4798",
    "1638-9387"
]

links = [
    "https://sciencepress.mnhn.fr/en/articles/comptes-rendus-palevol?titre=&motscles=&annees%5B%5D=2019&annees%5B%5D=2020&annees%5B%5D=2021&annees%5B%5D=2022&annees%5B%5D=2023&fascicule=&doi=&items_per_page=50&page=",
    "https://sciencepress.mnhn.fr/en/articles/adansonia?titre=&motscles=&annees%5B0%5D=2019&annees%5B1%5D=2020&annees%5B2%5D=2021&annees%5B3%5D=2022&annees%5B4%5D=2023&fascicule=&doi=&items_per_page=50&page=",
    "https://sciencepress.mnhn.fr/en/articles/zoosystema?titre=&motscles=&annees%5B%5D=2019&annees%5B%5D=2020&annees%5B%5D=2021&annees%5B%5D=2022&annees%5B%5D=2023&fascicule=&doi=&items_per_page=50&page="
]

subjects = [
    "Paleontology",
    "Plant Sciences",
    "Zoology"
]

def filter_year(paper):
    year = paper.select_one('h2').text.strip()
    if not year:
        return False
    try:
        year = int(year)
        if year < 2019:
            return False
    except Exception as error:
        print(f'[erro year] {year}', error)
        return False
    
    return year

def parse_paper_info(paper, journal_title, eissn, subject):
    pdf_url = paper.select_one('div.text-left a')['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url
    if '/pdf/' not in pdf_url:
        return

    article_url = paper.a['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = paper.h3.text.strip()

    abstract = ''
   
    authors = ""
    if paper.select_one('div.infos-article p'):
        authors = paper.select_one('div.infos-article p').text.replace('\n', '').strip()
    
    published_at = paper.select_one('span.date-display-single').text.strip()
    published_year = published_at.split()[-1]
    try:
        if int(published_year) < 2019:
            print(f'[< 2019] {published_year} == {article_url}')
            return
    except Exception as error:
        print(f'[error] [year] {published_year} === {article_url} === {error}')
        return
    file_name = sanitize_filename(title) + '.pdf'
    dir = journal_title + '(' + eissn +')'
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
        for result in executor.map(parse_paper_info, list, [journal_title] * len(list), [eissn] * len(list), [subject] * len(list)):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def parse_archive(base_url, journal_title, eissn, subject):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    page = 0
    while True:
        url = start_url + f'&page={page}'
        res = bs(fetch_url(url).text, 'lxml')
        papers = res.select("div.view-display-id-all > div.clamp")

        paginations = res.select('ul.pager li.pager-next')
        print(f'[{url}] [{page}] {len(papers)} papers')
        if not paginations:
            break
        page += 1
        time.sleep(1)
        fetch_all(papers, journal_title, eissn, subject)
        
def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x], subjects[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)