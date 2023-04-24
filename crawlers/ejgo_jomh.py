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

max_workers = 6

domains = [
    "https://www.jomh.org",
    "https://www.ejgo.net"
]

csv_name = "ejgo_jomh"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "IMR PRESS"

journal_titles = [
    "JOURNAL OF MENS HEALTH",
    "EUROPEAN JOURNAL OF GYNAECOLOGICAL ONCOLOGY"
]

eissns = [
    "1875-6859",
    "2709-0086"
]

links = [
    "https://www.jomh.org/articles/archive",
    "https://www.ejgo.net/articles/archive",
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

def parse_paper_info(paper, journal_title, issn, published_year, domain):
    if not paper.find('a', href=re.compile(r"/pdf")):
        return

    pdf_url = paper.find('a', href=re.compile(r"/pdf"))['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url
  
    article_url = paper.a['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = paper.a.text.strip()
    authors = ""
    if paper.a.find_next_sibling():
        authors = paper.a.find_next_sibling().text.replace('\n', '').strip()
 
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


def parse_archive(base_url, journal_title, eissn, domain):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')
    volumes = res.select("div.content div.main > div")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        issues = volume.select('ul li a')
        fetch_all(issues, journal_title, eissn, year, domain)
            
def parse_issue(issue, journal_title, eissn, year, domain):
    issue_url = domain + issue['href']
    papers = bs(fetch_url(issue_url).text, 'lxml').select("div.block.main-data > div")
    print(f'[{domain}] [{year}] {issue_url} {len(papers)} papers')
    for paper in papers:
        parse_paper_info(paper, journal_title, eissn, year, domain)

def fetch_all(list, journal_title, eissn, year, domain, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(parse_issue, list, [journal_title] * len(list), [eissn] * len(list), [year] * len(list), [domain] * len(list)):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x], domains[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)