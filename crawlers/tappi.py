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

max_workers = 2

domains = [
    "https://www.tappi.org",
]

csv_name = "tappi"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "TECH ASSOC PULP PAPER IND INC"

journal_titles = [
    "TAPPI JOURNAL",
]

eissns = [
    "0734-1415",
]

links = [
    "https://www.tappi.org/publications-standards/tappi-journal/home/",
]

subjects = [
    "Materials Science, Paper & Wood",
]

def filter_year(paper):
    year = paper.text.strip()
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
    if not paper.select_one('div.access-icon'):
        return

    title = paper.h5.a.text.strip()
  
    article_url = paper.a['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    print(f'[paper] [{published_year}] {article_url}')
    article = bs(fetch_url(article_url).text, 'lxml')
    if not article.find('a', href=re.compile(r".pdf$")):
        return

    authors = ''
    if article.select_one('span.AuthorValue'):
        authors = article.select_one('span.AuthorValue').text.strip()

    if not article.select_one('div.PanelDownloadLink a'):
        return

    pdf_url = article.select_one('div.PanelDownloadLink a')['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url
   
    abstract = ''
    if article.select_one('div.ProductDetails span'):
        abstract = article.select_one('div.ProductDetails span').text.replace('ABSTRACT', '').replace(':', '').strip()
    published_at = paper.p.text.strip()
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
    res = bs(fetch_url(start_url).text, 'lxml')
    years = res.select('select[name="year"] option')
    parse_issue(years, start_url, journal_title, eissn, subject, domain)
                
  
def parse_issue(years, start_url, journal_title, eissn, subject, domain):
    for _year in years:
        url = f"{start_url}?year={_year.text.strip()}&month=JAN"
        res = bs(fetch_url(url).text, 'lxml')
        year = _year.text.strip()
        if int(year) < 2019:
            break
        months = res.select('select[name="month"] option')
        print(f'[{year}] [{start_url}] {len(months)} months')
        for month in months:
            url = f"{start_url}?year={year}&month={month.text.strip()}"
            res = bs(fetch_url(url).text, 'lxml')
            papers = res.select("div.result-single")
            print(f'[{year}] [{month.text}] {len(papers)} papers')
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