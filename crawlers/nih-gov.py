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

domain = "https://www.ncbi.nlm.nih.gov"

csv_name = "nih-gov"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "MEDICINA ORAL S L"

journal_titles = [
    "MEDICINA ORAL PATOLOGIA ORAL Y CIRUGIA BUCAL"
]

eissns = [
    "1698-6946"
]

links = [
    "https://www.ncbi.nlm.nih.gov/pmc/journals/1898/"
]

subjects = ["Dentistry, Oral Surgery & Medicine"]

max_workers = 6

def filter_year(paper):
    year = [t.text for t in paper.select_one('th.vol-year-cell').children][-1]
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

    pdf_url = domain + paper.find('a', href=re.compile(r".pdf$"))['href']
    article_url = domain + paper.a['href']
    title = paper.select_one('div.title').text.strip()
    authors = ""
    if paper.select_one('div.desc'):
        authors = paper.select_one('div.desc').text.strip()
    abstract = ''
    published_at = paper.select_one('span.citation-publication-date').text.strip()
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
    def fetch_all(self, list, journal_title, eissn, published_year, subject, occurrence=max_workers):
        output = []
        total = len(list)
        reminder = math.floor(total / 50)
        if reminder < occurrence:
            reminder = occurrence

        count = 0
        with ThreadPoolExecutor(
            max_workers=occurrence, thread_name_prefix="fetcher"
        ) as executor:
            for result in executor.map(self.parse_issue, list, [journal_title]*len(list), [eissn]*len(list), [published_year]*len(list), [subject] * len(list)):
                if result:
                    count = count + 1
                    if count % reminder == 0:
                        print("Concurrent Operation count = ", count)
                    output.append(result)
        return output

    def parse_archive(self, base_url, journal_title, eissn, subject):
        start_url = base_url
        print(f"[sci] [{csv_name}] {start_url}")
        res = bs(fetch_url(start_url).text, 'lxml')
        volumes = res.select("table.vlist > tr")
        for volume in volumes:
            if not volume.select_one('td.issues-cell'):
                continue
            year = filter_year(volume)
            if not year:
                continue
            issues = volume.select('td.iss-cell a.arc-issue')
            print(f'[{year}] {len(issues)} issues')
            self.fetch_all(issues, journal_title, eissn, year, subject)
                
    def parse_issue(self, issue, journal_title, eissn, year, subject):
        issue_url = issue['href']
        if not issue_url.startswith('http'):
            issue_url = domain + issue_url
        issue_data = bs(fetch_url(issue_url).text, 'lxml')
        papers = issue_data.select("div.rprt")
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