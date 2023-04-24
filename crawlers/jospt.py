import os
import math
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
from pathvalidate import sanitize_filename
import re
from time import sleep

from util import (
    fetch_url, 
    create_dir, 
    export_csv, 
    read_csv_data, 
    download_pdfs_via_thread,
    setup_chrome_profile
)


# 403 forbidden

max_workers = 1

domain = "https://www.jospt.org"

csv_name = "jospt"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "JOSPT"

journal_titles = [
    "JOURNAL OF ORTHOPAEDIC & SPORTS PHYSICAL THERAPY",
]

eissns = [
    "1938-1344",
]

links = [
    "https://www.jospt.org/loi/jospt",
]

subjects = [
    "Orthopedics | Rehabilitation | Sport Sciences",
]

def filter_year(paper):
    year = paper.text.strip()
    if not year:
        return False
    if 's' in year:
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
    if not paper.select_one('span.issue-item-access') and not paper.select_one('span.full-access'):
        return

    if not paper.find('a', title=re.compile(r"PDF")):
        return

    pdf_url = paper.find('a', title=re.compile(r"PDF"))['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url.replace("epdf", "pdf") + '?download=true'
  
    article_url = paper.select_one('h5.issue-item__title a')['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = paper.select_one('h5.issue-item__title').text.strip()
    authors = ""
    if paper.select_one('div.issue-item__loa'):
        authors = paper.select_one('div.issue-item__loa').text.replace('\n', '').strip()
 
    abstract = ''
    published_at = paper.select('div.issue-item__header > span')[-1].text.strip()
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
    driver = setup_chrome_profile()
    driver.get(start_url)
    sleep(1)
    res = bs(driver.page_source, 'lxml')
    volumes = res.select("div.nested-tab div.swipe__wrapper ul li a")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        volume_url = domain + volume['href']
        driver.get(volume_url)
        sleep(1)
        volume_data = bs(driver.page_source, 'lxml')
        issues = volume_data.select('li.issue-items__bordered')
        print(f'[volume] {volume_url} {len(issues)} issues')
        for issue in issues:
            parse_issue(issue, journal_title, eissn, year, subject)
            
def parse_issue(issue, journal_title, eissn, year, subject):
    issue_url = issue.a['href']
    if not issue_url.startswith('http'):
        issue_url = domain + issue_url
    papers = bs(fetch_url(issue_url).text, 'lxml').select("div.issue-item")
    print(f'[{year}] {issue_url} {len(papers)} papers')
    for paper in papers:
        parse_paper_info(paper, journal_title, eissn, year, subject)

def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x], subjects[x])

if __name__ == '__main__':
    start()

    export_csv(csv_name, csv_data)

    # read_csv_data(csv_name, csv_data)

    # download_pdfs_via_thread(csv_data)