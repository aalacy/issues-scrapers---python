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


# 403 forbidden

max_workers = 1

domain = "https://www.tsoc.org.tw"

csv_name = "tsoc"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "TAIWAN SOC CARDIOLOGY"

journal_titles = [
    "ACTA CARDIOLOGICA SINICA",
]

eissns = [
    "1011-6842",
]

links = [
    "https://www.tsoc.org.tw/index_en.php?action=journal",
]

subjects = [
    "Cardiac & Cardiovascular System",
]

def filter_year(paper):
    year = paper.text.split('(')[-1].split(')')[0].strip().split()[-1].strip()
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
    if not paper.select_one('div.download a'):
        return

    pdf_url = paper.select_one('div.download a')['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + '/' + pdf_url
  
    article_url = ''
    title = paper.select_one('div.title').text.strip()
    authors = ""
    if paper.select_one('div.author'):
        authors = paper.select_one('div.author').text.replace('\n', '').strip()
 
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


def parse_archive(base_url, journal_title, eissn, subject):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')
    volumes = res.select("div.journal-item-list  div.box a")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        volume_url = domain + volume['href']
        volume_data = bs(fetch_url(volume_url).text, 'lxml')
        papers = volume_data.select('div.journal-list div.box')
        print(f'[{year}] {volume_url} {len(papers)} papers')
        fetch_all(papers, journal_title, eissn, year, subject)
            
def fetch_all(list, journal_title, eissn, year, subject, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(parse_paper_info, list, [journal_title] * len(list), [eissn] * len(list), [year] * len(list), [subject] * len(list)):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x], subjects[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)