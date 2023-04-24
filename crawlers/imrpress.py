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
)

max_workers = 6

domain = "https://www.imrpress.com"

csv_name = "imrpress"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "IMR PRESS"

journal_titles = [
    "CLINICAL AND EXPERIMENTAL OBSTETRICS & GYNECOLOGY",
    "JOURNAL OF INTEGRATIVE NEUROSCIENCE",
    "REVIEWS IN CARDIOVASCULAR MEDICINE"
]

eissns = [
    "2709-0094",
    "1757-448X",
    "2153-8174"
]

links = [
    "https://www.imrpress.com/journal/CEOG/volumes_and_issues",
    "https://www.imrpress.com/journal/JIN/volumes_and_issues",
    "https://www.imrpress.com/journal/RCM/volumes_and_issues"
]

def filter_year(paper):
    year = paper.select_one('div.ipubw-p1').text.split('|')[-1].strip()
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

def parse_paper_info(paper, journal_title, issn, published_year):
    if not paper.find('a', href=re.compile(r"/pdf$")):
        return

    pdf_url = domain + paper.find('a', href=re.compile(r"/pdf$"))['href']
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


def parse_archive(base_url, journal_title, eissn):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')
    volumes = res.select("li.archive-page-content-item")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        issues = volume.select('div.archive-page-content-item-issue a')
        fetch_all(issues, journal_title, eissn, year)
            
def parse_issue(issue, journal_title, eissn, year):
    issue_url = domain + issue['href']
    papers = bs(fetch_url(issue_url).text, 'lxml').select("div.issue-page-content li.ipub-article-item")
    print(f'[{year}] {issue_url} {len(papers)} papers')
    for paper in papers:
        parse_paper_info(paper, journal_title, eissn, year)

def fetch_all(list, journal_title, eissn, year, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(parse_issue, list, [journal_title] * len(list), [eissn] * len(list), [year] * len(list)):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)