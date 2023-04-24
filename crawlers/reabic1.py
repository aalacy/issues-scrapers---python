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
    download_pdfs_via_thread,
    download
)

max_workers = 2

domains = [
    "https://www.reabic.net/aquaticinvasions/",
]

csv_name = "reabic1"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "REGIONAL EURO-ASIAN BIOLOGICAL INVASIONS CENTRE-REABIC"

journal_titles = [
    "AQUATIC INVASIONS"
]

eissns = [
    "1818-5487"
]

links = [
    "https://www.reabic.net/aquaticinvasions/archive.aspx"
]

subjects = [
    "Ecology | Marine & Freshwater Biology"
]

def filter_year(paper):
    year = paper.find_parent('table').find_parent().find_previous_sibling('h1').text.strip().split()[-1]
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

def parse_paper_info(article, journal_title, issn, published_year, subject, domain):
    paper = article.find_parent('table').find_parent('tr')
    if not paper.text.strip():
        return
    
    article_url = ''

    if not paper.find('a', href=re.compile(r".pdf$")):
        return
    
    title = paper.find_previous_sibling('tr').b.text.strip()
    pdf_url = domain + str(published_year) + '/' + paper.find('a', href=re.compile(r".pdf$"))['href']
    authors = ""
    abstract = ''
    if paper.find_next_sibling('tr').p:
        abstract = paper.find_next_sibling('tr').p.text.strip()
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
    issues = res.select("a.menu4a")
    for issue in issues:
        if 'summary' in issue['href']:
            continue
        year = filter_year(issue)
        if not year:
            continue
        parse_issue(issue, journal_title, eissn, year, subject, domain)
            
def parse_issue(issue, journal_title, eissn, year, subject, domain):
    issue_url = issue['href']
    if not issue_url.startswith('http'):
        issue_url = domain + issue_url
    res = bs(fetch_url(issue_url, need_proxies=True).text, 'lxml')
    papers = res.find_all('a', href=re.compile(r".pdf$"))
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