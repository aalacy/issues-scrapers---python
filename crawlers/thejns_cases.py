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

max_workers = 16

domain = "https://thejns.org"

csv_name = "thejns_cases"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "AMER ASSOC NEUROLOGICAL SURGEONS"

journal_titles = [
    "JOURNAL OF NEUROSURGERY CASE LESSONS",
]

eissns = [
    "1933-0693"
]

links = [
    "https://thejns.org/browse?access=all&pageSize=50&sort=datedescending",
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

def parse_paper_info(paper):
    article_url = paper.h2.a['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = paper.h2.a.text.strip()
    
    print(f"[parse paper] {article_url}")
    article = bs(fetch_url(article_url).text, 'lxml')
    if not article.select_one('a.pdf-download'):
        return

    journal_title = article.select_one('span.source-link a').text.strip()
    issn = "1092-0684"
    pdf_url = article.select_one('a.pdf-download')['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url

    abstract = ''
    if article.select_one('div.container-abstract'):
        abstract = article.select_one('div.container-abstract').text.replace('\n\n', '').strip()

    authors = ""
    if article.select_one('div.content-contributor-author'):
        authors = article.select_one('div.content-contributor-author').text.replace('\n', '').strip()
    
    published_at = ""
    published_year = ""
    if article.select_one('dl.date-published-online dd'):
        published_at = article.select_one('dl.date-published-online dd').text.strip()
        published_year = published_at.split()[-1].strip()
    elif article.select_one('dl.date-accepted dd'):
        published_at = article.select_one('dl.date-accepted dd').text.strip()
        published_year = published_at.split()[-1].strip()
    else:
        try:
            published_year = article.select_one('div.component-content-title h1.text-headline a').text.split('(')[1].split(')')[0].strip().split()[-1]
            published_at = published_year
        except Exception as error:
            print(f'[error] [year] {published_year} === {article_url} === {error}')
            return
    try:
        if int(published_year) < 2019:
            print(f'[< 2019] {published_year} == {article_url}')
            return
    except Exception as error:
        print(f'[error] [year] {published_year} === {article_url} === {error}')
        return
    subjects = ''

    
    file_name = sanitize_filename(title) + '.pdf'
    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    csv_data.append({
        'Title': title, 
        'Source': PUBLISHER_NAME, 
        'Subject': journal_title, 
        'Sub Category': subjects, 
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

def fetch_all(list, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(parse_paper_info, list):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def parse_archive(base_url):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    page = 1
    while True:
        url = start_url + f'&page={page}'
        res = bs(fetch_url(url).text, 'lxml')
        papers = res.select("div#searchContent div.hasAccess.contentItem.chunkResult")

        print(f'[{url}] [{page}] {len(papers)} papers')
        if not papers:
            break
        page += 1
        time.sleep(1)
        fetch_all(papers)
        
def start():
    for x in range(len(links)):
        parse_archive(links[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)