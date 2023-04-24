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

domain = "https://www.jssm.org/"

csv_name = "jssm"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "JOURNAL SPORTS SCIENCE & MEDICINE"

journal_titles = [
    "JOURNAL OF SPORTS SCIENCE AND MEDICINE"
]

eissns = [
    "1303-2968"
]

links = [
    "https://www.jssm.org/newarchives.php"
]

subjects = ["Sport Sciences"]

def filter_year(paper):
    year = paper.select_one('table.tableback2').text.strip().split()[0]
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
    if not paper.find('a', href=re.compile(r">fulltext$", re.I)):
        return
    article_url = paper.find('a', href=re.compile(r">fulltext$", re.I))['href']
    if not article_url.startswith("http"):
        article_url = domain +  article_url

    if not paper.find('button', text=re.compile(r"PDF")):
        return

    pdf_url = domain + paper.find('button', text=re.compile(r"PDF")).find_parent('a')['href']
    title = paper.select_one('td.tablemaster5_td_1').text.strip()
    authors = ""
    if paper.select_one('td.tablemaster5_td_2'):
        authors = paper.select_one('td.tablemaster5_td_2').text.strip()
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
        volumes = res.select("table.tableback1")
        for volume in volumes:
            year = filter_year(volume)
            if not year:
                continue
            issues = volume.select('table.tableback3 a')
            print(f'[{year}] {len(issues)} issues')
            for issue in issues:
                self.parse_issue(issue, journal_title, eissn, year, subject)
                
    def parse_issue(self, issue, journal_title, eissn, year, subject):
        issue_url = issue['href']
        if not issue_url.startswith('http'):
            issue_url = domain + issue_url
        self.driver.get(issue_url)
        papers = bs(self.driver.page_source, 'lxml').select("table.tablemaster3 > tbody > tr > td > table.tablemaster5")
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