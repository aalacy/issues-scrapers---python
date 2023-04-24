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

domain = "http://www.mijst.mju.ac.th"

csv_name = "mju"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "MAEJO UNIVL"

journal_titles = [
    "MAEJO INTERNATIONAL JOURNAL OF SCIENCE AND TECHNOLOGY"
]

eissns = [
    "1905-7873"
]

links = [
    "http://www.mijst.mju.ac.th/index.asp"
]

subjects = ["Multidisciplinary Sciences"]

def filter_year(paper):
    year = paper.text.split(',')[-1].strip()
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

def parse_paper_info(paper, journal_title, issn, published_year, subject):
    paper = paper.findChildren('td', recusive=False)[-1]
    if not paper.text.strip():
        return
    if not paper.find('', text=re.compile(r"Full(.+)Paper")) and not paper.find('', text=re.compile(r"Review")) and not paper.find('', text=re.compile(r"Technical(.+)Note")) and not paper.find('', text=re.compile(r"Report")):
        return
    if not paper.find('a', href=re.compile(r".pdf$")):
        return
    
    article_url = paper.a['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url

    pdf_url = paper.find_all('a', href=re.compile(r".pdf$"))[-1]['href']
    title = paper.find('a', href=re.compile(r".pdf$")).text.strip()
    authors = ""
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
    volumes = res.select("table")[1].select_one('table').select("tr")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        volume_url = domain + '/' + volume.a['href'].replace('..', '')
        volume_data = bs(fetch_url(volume_url, need_proxies=True).text, 'lxml')
        papers = []
        for table in volume_data.select('table tr div table div > div > table'):
            for tr in table.select('tr'):
                if not tr.text.strip():
                    continue
                papers.append(tr)
        print(f'[{year}] {len(papers)} papers')
        for paper in papers:
            parse_paper_info(paper, journal_title, eissn, year, subject)
 
def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x], subjects[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)