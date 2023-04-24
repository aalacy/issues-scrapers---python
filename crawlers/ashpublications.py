import os
import math
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
from pathvalidate import sanitize_filename

from util import (
    fetch_url, 
    create_dir, 
    export_csv, 
    read_csv_data, 
    download_pdfs_via_thread,
    set_up_selenium_webdriver,
    download_via_selenium
)

max_workers = 1

domain = "https://www.ashpublications.org"
BASE_PATH = "/media/com/C69A54D99A54C817/pwj/BOSNIAN JOURNAL OF BASIC MEDICAL SCIENCES(1840-4812)/" #os.path.abspath(os.curdir)
proxy_url = "https://api.scraperapi.com?api_key=3664a7c3***ecd461ad4c8bf0&url="

csv_name = "ashpublications"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "AMER SOC HEMATOLOGY"

journal_titles = [
    "BLOOD",
    "HEMATOLOGY-AMERICAN SOCIETY OF HEMATOLOGY EDUCATION PROGRAM"
]

eissns = [
    "1528-0020",
    "1520-4383"
]

links = [
    "https://ashpublications.org/search-results?f_JournalDisplayName=Blood&fl_SiteID=1&rg_PublicationDate=01%2f01%2f2019+TO+12%2f31%2f2023&f_ContentType=Journal+Articles&page=1",
    "https://ashpublications.org/search-results?f_JournalDisplayName=Hematology&fl_SiteID=1&rg_PublicationDate=01%2f01%2f2019+TO+12%2f31%2f2023&f_ContentType=Journal+Articles&page=1"
]

def parse_paper_info(article, journal_title, issn):
    published_year = filter_year(article)
    if not published_year:
        return
        
    article_url = article.select_one('h4 a')['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = article.select_one('h4').text.strip()
    authors = ""
    if article.select_one('div.sri-authors.al-authors-list'):
        authors = article.select_one('div.sri-authors.al-authors-list').text.strip()
 
    print(f'[parse article] {article_url}')
    paper = bs(fetch_url(article_url, need_proxies=True).text, 'lxml')
    if not paper.select_one('a.article-pdfLink'):
        return
    pdf_url = paper.select_one('a.article-pdfLink')['href']
    if not pdf_url:
        return

    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url
  
    abstract = ''
    if paper.select_one('section.abstract'):
        abstract = paper.select_one('section.abstract').text.strip()

    published_at = paper.select_one('span.article-date').text.strip()
    file_name = sanitize_filename(title) + '.pdf'
    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    csv_data.append({
        'Title': title, 
        'Source': PUBLISHER_NAME, 
        'Subject': journal_title, 
        'Sub Category': 'Hematology', 
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

def fetch_all(list, journal_title, eissn, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(parse_paper_info, list, [journal_title] * len(list), [eissn] * len(list)):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def filter_year(paper):
    year = paper.select_one('div.sri-date.al-pub-date').text.replace('Published', '').replace(':', '').strip()
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


def parse_archive(base_url, journal_title, eissn):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url, need_proxies=True).text, 'lxml')

    while True:
        paginations = res.select("div.pagination-bottom-outer-wrap div.pagination.al-pagination a.al-nav-next")
        _papers = res.select("div.sr-list_wrap.new-results div.item-info")
        papers = [paper for paper in _papers if filter_year(paper)]
        print(f'[list] {len(papers)} papers')
        for x in range(len(papers)):
            parse_paper_info(papers[x], journal_title, eissn)

        if len(papers) < len(_papers):
            print('[page break] [< 2019]')
            break

        if not paginations:
            break

        next_page_url = "https://ashpublications.org/search-results?" + paginations[0]['data-url']
        print(f'[page] {next_page_url}')
        res = bs(fetch_url(next_page_url, need_proxies=True).text, 'lxml')

def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    BASE_PATH = "/home/com/SData/BLOOD(1528-0020)"
    download_via_selenium(csv_name, csv_data)