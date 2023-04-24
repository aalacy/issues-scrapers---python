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

max_workers = 6

domain = "https://www.hh.um.es"

csv_name = "hh_um"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "F HERNANDEZ"

journal_titles = [
    "HISTOLOGY AND HISTOPATHOLOGY",
]

eissns = [
    "1699-5848",
]

links = [
    "https://www.hh.um.es/Issues.htm",
]

subjects = [
    "Cell Biology | Pathology",
]

def filter_year(paper):
    if 'Volume' in paper.text:
        return False

    year = None
    for parent in paper.find_parent().find_previous_siblings('tr'):
        if parent['bgcolor'] == '#ffffcc':
            year = parent.text.split('(')[0].strip()
            break
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
    if not paper.find('a', href=re.compile(r".pdf$")):
        return

    pdf_url = paper.find('a', href=re.compile(r".pdf$"))['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url.replace('..', '')
  
    article_url = domain + paper.a['href'].replace('..', '')
    title = paper.strong.text.strip() if paper.strong else paper.b.text.strip()
    authors = ""
    txts = []
    if paper.p.font:
        txts = [p.text.strip() for p in paper.p.font.children if p.text.strip()]
    if len(txts) > 2:
        authors = txts[1]
    else:
        pass
 
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
    volumes = res.select("div.content table tr td")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        volume_url = volume.select_one('strong a')['href']
        if not volume_url.startswith('http'):
            volume_url = domain + volume['href']

        volume_data = bs(fetch_url(volume_url).text, 'lxml')
        papers = volume_data.select('div.content table tr')
        if not papers:
            papers = volume_data.select('body > table > tbody > tr')
            if not papers:
                papers = [volume_data]
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