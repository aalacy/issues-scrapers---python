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

max_workers = 2

domains = [
    "https://www.actaitalica.it/"
]

csv_name = "actaitalica"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "PACINI EDITORE"

journal_titles = [
    "ACTA OTORHINOLARYNGOLOGICA ITALICA",
]

eissns = [
    "1827-675X",
]

links = [
    "https://www.actaitalica.it/issue/archive"
]

subjects = [
    "Otorhinolaryngology",
]

def filter_year(paper):
    year = paper.h4.text.strip().split()[-1].strip()
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
    if 'Editorial' in title.lower():
        return

    print(f'[{published_year}][article] {article_url}')
    article = bs(fetch_url(article_url, need_proxies=True).text, 'lxml')
    if not article.select_one('a.obj_galley_link.pdf'):
        return
    pdf_url = article.select_one('a.obj_galley_link.pdf')['href'].replace('/view/', '/download/')
    authors = ""
    if article.select_one('div.author'):
        authors = article.select_one('div.author').text.replace('\n', '').strip()
    abstract = ''
    if article.find('h2', text=re.compile(r"^Abstract$")):
        abstract = article.find('h2', text=re.compile(r"^Abstract$")).find_next_sibling().text.replace('\n\n', '').strip()
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
    issues = res.select("div.row div.issue-block")
    for issue in issues:
        year = filter_year(issue)
        if not year:
            continue
        parse_issue(issue, journal_title, eissn, year, subject, domain)
            
def parse_issue(issue, journal_title, eissn, year, subject, domain):
    issue_url = issue.find_parent('a')['href']
    if not issue_url.startswith('http'):
        issue_url = domain + issue_url
    papers = bs(fetch_url(issue_url, need_proxies=True).text, 'lxml').select("div.article-item")
    print(f'[{year}] {issue_url} {len(papers)} papers')
    fetch_all(papers, journal_title, eissn, year, subject, domain)

def fetch_all(list, journal_title, eissn, year, subject, domain, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(parse_paper_info, list, [journal_title] * len(list), [eissn] * len(list), [year] * len(list), [subject] * len(list), [domain] * len(list)):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x], subjects[x], domains[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)