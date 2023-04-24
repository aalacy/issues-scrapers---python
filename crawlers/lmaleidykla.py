import os
import math
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
from lxml import etree
from pathvalidate import sanitize_filename
import re

from util import (
    fetch_url, 
    create_dir, 
    export_csv, 
    read_csv_data, 
    download_pdfs_via_thread,
)

max_workers = 10

domain = "https://www.lmaleidykla.lt"

csv_name = "lmaleidykla"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "LIETUVOS MOKSLU AKAD LEIDYKLA"

journal_titles = [
    "CHEMIJA"
]

eissns = [
    "0235-7216"
]

links = [
    "https://www.lmaleidykla.lt/ojs/index.php/chemija/issue/archive"
]

subjects = ["Chemistry, Multidisciplinary"]

def filter_year(paper):
    year = paper.a.text.split(')')[0].split('(')[-1].strip()
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

def parse_paper_info(paper, journal_title, issn, published_year, published_at, subject):
    if not paper.select_one('a.obj_galley_link.pdf'):
        return
    
    article_url = paper.a['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url

    pdf_url = paper.select_one('a.obj_galley_link.pdf')['href'].replace('view/', 'download/')
    title = paper.a.text.strip()
    authors = ""
    if paper.select('div.authors'):
        authors = paper.select_one('div.authors').text.strip()
    abstract = ''
    file_name = sanitize_filename(title) + '.pdf'
    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    csv_data.append({
        'Title': title, 
        'Source': PUBLISHER_NAME, 
        'Subject': journal_title, 
        'Sub Category': subject, 
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


def parse_archive(base_url, journal_title, eissn, subject):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url, need_proxies=True).text, 'lxml')
    volumes = res.select("ul.issues_archive li")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        volume_url = volume.a['href']
        volume_data = bs(fetch_url(volume_url, need_proxies=True).text, 'lxml')
        papers = volume_data.select('ul.articles > li')
        print(f'[{year}] {len(papers)} papers')
        published_at = volume_data.select_one('div.published span.value').text.strip()
        for paper in papers:
            parse_paper_info(paper, journal_title, eissn, year, published_at, subject)
 
def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x], subjects[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)