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

domain = "https://comptes-rendus.academie-sciences.fr"

csv_name = "academie-sciences"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "CENTRE MERSENNE POUR LADITION SCIENTIFIQUE OUVERTE"

journal_titles = [
    "COMPTES RENDUS BIOLOGIES",
    "COMPTES RENDUS CHIMIE",
    "COMPTES RENDUS GEOSCIENCE",
    "COMPTES RENDUS MATHEMATIQUE",
    "COMPTES RENDUS MECANIQUE",
    "COMPTES RENDUS PHYSIQUE",
]

eissns = [
    "1768-3238",
    "1878-1543",
    "1778-7025",
    "1778-3569",
    "1873-7234",
    "1878-1535",
]

links = [
    "https://comptes-rendus.academie-sciences.fr/biologies",
    "https://comptes-rendus.academie-sciences.fr/chimie",
    "https://comptes-rendus.academie-sciences.fr/geoscience",
    "https://comptes-rendus.academie-sciences.fr/mathematique",
    "https://comptes-rendus.academie-sciences.fr/mecanique",
    "https://comptes-rendus.academie-sciences.fr/physique",
]

def parse_paper_info(paper, journal_title, issn, published_year):
    if not paper.find('a', href=re.compile(r".pdf")):
        return

    article_type = ''
    if paper.select_one('div.article-subj'):
        article_type = paper.select_one('div.article-subj').text.strip()
    article_url = paper.select_one('span.article-title a')['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = paper.select_one('span.article-title').text.strip()
    pdf_url = paper.find('a', href=re.compile(r".pdf"))['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url
    authors = ""
    if paper.select_one('div.article-author'):
        authors = paper.select_one('div.article-author').text.strip()
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
        'Type': article_type, 
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

def parse_archive(base_url, journal_title, issn):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')
    browse = res.select_one('div#search-bar a')
    if not browse:
        print('no browse')
        return
    browse_url = domain + browse['href']
    volumes = bs(fetch_url(browse_url).text, 'lxml').select('div.list-of-issues div.flex-item')

    volume_urls = []
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            break
        if 'sciencedirect' in volume.a['href']:
            break

        urls = [a['href'] for a in volume.select('a')]
        for volume_url  in urls:
            if volume_url in volume_urls:
                continue

            volume_urls.append(volume_url)
            print(f"[{volume.text.strip()}] [{volume_url}]")
            volume_data = bs(fetch_url(domain + volume_url).text, 'lxml')
            papers = volume_data.select("div.article-div  div.row")
            for paper in papers:
                parse_paper_info(paper, journal_title, issn, year)

def filter_year(volume):
    year = volume.text.split('(')[-1].split(')')[0].strip()
    if not year:
        print(f"[no year] {volume['href']}")
        return False
    try:
        year = int(year)
        if year < 2019:
            return False
    except Exception as error:
        print(f'[error] [year] {error}')
        if 'Advanced' not in year:
            return False

    return year

def fetch_all(list, journal_titles, eissns, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(parse_archive, list, journal_titles, eissns):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def start():
    fetch_all(links, journal_titles, eissns)
        
if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)