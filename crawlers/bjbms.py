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

max_workers = 2

domain = "https://www.bjbms.org"
BASE_PATH = "/media/com/C69A54D99A54C817/pwj/BOSNIAN JOURNAL OF BASIC MEDICAL SCIENCES(1840-4812)/" #os.path.abspath(os.curdir)
proxy_url = "https://api.scraperapi.com?api_key=3664a7c3***ecd461ad4c8bf0&url="

csv_name = "bjbms"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "FOUNDATION REHABILITATION INFORMATION"

journal_titles = [
    "BOSNIAN JOURNAL OF BASIC MEDICAL SCIENCES"
]

eissns = [
    "1840-4812"
]

links = [
    "https://www.bjbms.org/ojs/index.php/bjbms/issue/archive"
]

def parse_paper_info(paper, journal_title, issn, published_year):
    if not paper.select_one('a.pdf'):
        return

    article_url = paper.select_one('h3 a')['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = paper.select_one('h3 a').text.strip()
    pdf_url = bs(fetch_url(paper.select_one('a.pdf')['href']).text, 'lxml').select_one('a.download')['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url
    authors = ""
    if paper.select_one('div.authors'):
        authors = paper.select_one('div.authors').text.strip()
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

def get_content(page_source):
    return page_source.select("ul.cmp_article_list.articles > li")

def parse_volumes(volumes, journal_title, issn):
    fetch_all(volumes, journal_title, issn)

def parse_archive(base_url, journal_title, issn):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')
    paginations = res.select("div.cmp_pagination a.next")

    volumes = None
    while True:
        _vols = res.select("ul.issues_archive > li a")
        volumes = [volume for volume in _vols if filter_year(volume)]
        parse_volumes(volumes, journal_title, issn)

        if not paginations:
            break
        if len(volumes) < len(_vols):
            print('[page break] [< 2019]')
            break

        next_page_url = paginations[0]['href']
        print(f'[page] {next_page_url}')
        res = bs(fetch_url(next_page_url).text, 'lxml')

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

def fetch_single(volume, journal_title, issn):
    year = filter_year(volume)
    if not year:
        return
    volume_url = volume['href']
    print(f"[{volume.text.strip()}] [{volume_url}]")
    volume_data = bs(fetch_url(volume_url).text, 'lxml')
    papers = volume_data.select("ul.cmp_article_list.articles > li")
    for paper in papers:
        parse_paper_info(paper, journal_title, issn, year)

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
        for result in executor.map(fetch_single, list, [journal_title] * len(list), [eissn] * len(list)):
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

    # download_pdfs_via_thread(csv_data)
    set_up_selenium_webdriver(BASE_PATH)

    download_via_selenium(BASE_PATH, csv_data)