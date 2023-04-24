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
    set_up_selenium_webdriver,
    download_via_selenium
)

domain = "https://www.nms.ac.jp/"

csv_name = "nms"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "MEDICAL ASSOC NIPPON MEDICAL SCH"

journal_titles = [
    "JOURNAL OF NIPPON MEDICAL SCHOOL"
]

eissns = [
    "1347-3409"
]

links = [
    "https://www.nms.ac.jp/sh/jnms/jnms.html"
]

subjects = ["Medicine, General & Internal"]

def filter_year(paper):
    if not paper.find_previous_sibling('b'):
        return
    year = paper.find_previous_sibling('b').text.strip().split()[-1]
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
    if not paper.find('a', href=re.compile(r".pdf$")):
        return

    pdf_url = domain + f'sh/jnms/{published_year}/' + paper.find('a', href=re.compile(r".pdf$"))['href']
    article_url = pdf_url
    title = paper.select_one('span.article_title').text.strip()
    authors = ""
    if paper.select_one('span.author'):
        authors = paper.select_one('span.author').text.strip()
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

class Scraper:
    driver = None

    def parse_archive(self, base_url, journal_title, eissn, subject):
        start_url = base_url
        print(f"[sci] [{csv_name}] {start_url}")
        self.driver = set_up_selenium_webdriver()
        self.driver.get(start_url)
        res = bs(self.driver.page_source, 'lxml')
        volumes = res.select("body > div > table")
        for volume in volumes:
            if not volume.select_one('tr.backnumber td'):
                continue
            year = filter_year(volume)
            if not year:
                continue
            issues = volume.select('tr.backnumber td a')
            print(f'[{year}] {len(issues)} issues')
            for issue in issues:
                self.parse_issue(issue, journal_title, eissn, year, subject)
                
    def parse_issue(self, issue, journal_title, eissn, year, subject):
        issue_url = issue['href']
        if not issue_url.startswith('http'):
            issue_url = domain + 'sh/jnms/' + issue_url
        if '.pdf' in issue_url:
            return
        self.driver.get(issue_url)
        papers = bs(self.driver.page_source, 'lxml').select("div.toc > p.toc")
        print(f'[{year}] {issue_url} {len(papers)} papers')
        for paper in papers:
            parse_paper_info(paper, journal_title, eissn, year, subject)

    def start(self):
        for x in range(len(links)):
            self.parse_archive(links[x], journal_titles[x], eissns[x], subjects[x])

if __name__ == '__main__':
    # scraper = Scraper()
    # scraper.start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_via_selenium(csv_name=csv_name, csv_data=csv_data)