import os
import math
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
from pathvalidate import sanitize_filename
import re
from time import sleep
from random import randint

from util import (
    fetch_url, 
    create_dir,
    export_csv, 
    read_csv_data, 
    download_pdfs_via_thread,
)

max_workers = 1

domains = [
    "https://www.termedia.pl",
    "https://www.termedia.pl",
    "https://www.termedia.pl",
    "https://www.termedia.pl",
    "https://www.termedia.pl",
    "https://www.termedia.pl",
    "https://www.termedia.pl",
]

csv_name = "termedia"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "TERMEDIA PUBLISHING HOUSE LTD"

journal_titles = [
    "BIOLOGY OF SPORT",
    "CENTRAL EUROPEAN JOURNAL OF IMMUNOLOGY",
    "FOLIA NEUROPATHOLOGICA",
    "JOURNAL OF CONTEMPORARY BRACHYTHERAPY",
    "POSTEPY W KARDIOLOGII INTERWENCYJNEJ",
    "VIDEOSURGERY AND OTHER MINIINVASIVE TECHNIQUES",
    "POLISH JOURNAL OF PATHOLOGY",
]

eissns = [
    "2083-1862",
    "1644-4124",
    "1509-572X",
    "2081-2841",
    "1897-4295",
    "2299-0054",
    "1233-9687",
]

links = [
    "https://www.termedia.pl/Journal/-78/Archiwum",
    "https://www.termedia.pl/Journal/Central_European_Journal_of_nbsp_Immunology-10/Archivehttps://www.termedia.pl/Journal/Folia_Neuropathologica-20/Archive",
    "https://www.termedia.pl/Journal/Folia_Neuropathologica-20/Archive",
    "https://www.termedia.pl/Journal/-54/Archiwum",
    "https://www.termedia.pl/Journal/Postepy_w_Kardiologii_Interwencyjnej-35/Archive",
    "https://www.termedia.pl/Journal/Wideochirurgia_i_inne_techniki_maloinwazyjne-42/Archive",
    "https://www.termedia.pl/Journal/Polish_Journal_of_Pathology-55/Archive",
]

subjects = [
    "Sport Sciences",
    "Immunology",
    "Neurosciences | Pathology",
    "Oncology | Radiology, Nuclear Medicine & Medical Imaging",
    "Cardiac & Cardiovascular System",
    "Surgery",
    "Pathology",
]

def filter_year(paper):
    year = paper.select_one('div.archiveYear').text.strip().split()[-1]
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
    if not paper.select_one('div.magAuthors'):
        return

    title = paper.a.text.strip()
    authors = paper.select_one('div.magAuthors').text.strip()
  
    article_url = paper.a['href'].replace('"', "").replace('”', '')
    if not article_url.startswith("http"):
        article_url = domain + article_url
    abstract = ''
    pdf_link = paper.select_one('div.magMoreLinks a.magFullT')
    if pdf_link and '.pdf' not in pdf_link.text:
        print(f'[{published_year}] {article_url}')
        article = bs(fetch_url(article_url, scraper_api=True, need_proxies=True).text, 'lxml')

        if not article.select_one('div.magArticleButtons a.darkButton'):
            return
        full_text_url = article.select_one('div.magArticleButtons a.darkButton')['href']
        if not full_text_url.startswith('http'):
            full_text_url = domain + full_text_url
        
        sleep(1)
        full_text = bs(fetch_url(full_text_url, scraper_api=True, need_proxies=True).text, 'lxml')
        if not full_text.select_one('div.articlePDF a'):
            return

        pdf_url = full_text.select_one('div.articlePDF a')['href']
        if not pdf_url.startswith('http'):
            pdf_url = domain + pdf_url
    
        abstract = ''
        if article.select_one('div.magArticleAbstract'):
            abstract = article.select_one('div.magArticleAbstract').text.strip()
    else:
        pdf_url = pdf_link['href']
        if not pdf_url.startswith('http'):
            pdf_url = domain + pdf_url
    published_at = published_year
    if paper.select('div.magBibliography'):
        published_at = paper.select('div.magBibliography')[-1].text.split(':')[-1].strip()
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
    res = bs(fetch_url(start_url, scraper_api=True, need_proxies=True).text, 'lxml')
    volumes = res.select("td.magContent table td")
    for volume in volumes:
        if not volume.text.strip():
            continue
        year = filter_year(volume)
        if not year:
            continue
        issues = volume.select("a.archiveVolume")
        for issue in issues:
            issue_url = domain + issue['href']
            sleep(1)
            issue_data = bs(fetch_url(issue_url, scraper_api=True, need_proxies=True).text, 'lxml')
            papers = issue_data.select('div.magArticle')
            print(f'[volume] {issue_url} {len(papers)} papers')
            for paper in papers:
                parse_paper_info(paper, journal_title, eissn, year, subject, domain)
                sleep(randint(3, 9))
 
def start():
    for x in range(len(links)):
        if x == 0:
            continue
        parse_archive(links[x], journal_titles[x], eissns[x], subjects[x], domains[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)