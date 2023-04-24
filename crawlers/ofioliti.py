import os
import math
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
from pathvalidate import sanitize_filename
import re

from util import (
    fetch_url, 
    create_dir, 
    export_csv, 
    read_csv_data, 
    download_pdfs_via_thread
)

max_workers = 1

domains = [
    "https://www.ofioliti.it/"
]

csv_name = "ofioliti"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "OFIOLITI"

journal_titles = [
    "OFIOLITI",
]

eissns = [
    "0391-2612",
]

links = [
    "https://www.ofioliti.it/index.php/ofioliti/issue/archive"
]

subjects = [
    "Geology",
]

def filter_year(paper):
    year = paper.h2.text.strip().split('(')[-1].split(')')[0].strip()
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

def parse_paper_info(paper, journal_title, issn, published_year, subject, domain):
    article_url = paper.a['href']
    if not article_url.startswith("http"):
        article_url = domain +  article_url
    title = paper.a.text.strip()
    if 'reviewers' in title.lower():
        return

    pdf_url = paper.select_one('a.obj_galley_link.pdf')['href'].replace('/view/', '/download/')
    authors = ""
    if paper.select_one('div.authors'):
        authors = paper.select_one('div.authors').text.replace('\n', '').strip()
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

def parse_archive(base_url, journal_title, eissn, subject, domain):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url, need_proxies=True).text, 'lxml')
    issues = res.select("ul.issues_archive > li")
    for issue in issues:
        year = filter_year(issue)
        if not year:
            continue
        parse_issue(issue, journal_title, eissn, year, subject, domain)
            
def parse_issue(issue, journal_title, eissn, year, subject, domain):
    issue_url = issue.h2.a['href']
    if not issue_url.startswith('http'):
        issue_url = domain + issue_url
    papers = bs(fetch_url(issue_url).text, 'lxml').select("ul.articles > li")
    print(f'[{year}] {issue_url} {len(papers)} papers')
    for paper in papers:
        parse_paper_info(paper, journal_title, eissn, year, subject, domain)

def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x], subjects[x], domains[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)