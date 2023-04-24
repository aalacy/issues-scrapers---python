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

domain = "https://www.lyellcollection.org"

csv_name = "lyellcollection"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "GEOLOGICAL SOC PUBL HOUSE"

journal_titles = [
    "GEOCHEMISTRY-EXPLORATION ENVIRONMENT ANALYSIS",
    "JOURNAL OF THE GEOLOGICAL SOCIETY",
    "PETROLEUM GEOSCIENCE",
    "PROCEEDINGS OF THE YORKSHIRE GEOLOGICAL SOCIETY",
    "QUARTERLY JOURNAL OF ENGINEERING GEOLOGY AND HYDROGEOLOGY",
    "SCOTTISH JOURNAL OF GEOLOGY",
]

eissns = [
    "2041-4943",
    "2041-479X",
    "2041-496X",
    "2041-4811",
    "2041-4803",
    "2041-4951",
]

links = [
    "https://lyellcollection.org/journal/loi/geea",
    "https://lyellcollection.org/journal/loi/jgs",
    "https://lyellcollection.org/journal/loi/pg",
    "https://lyellcollection.org/journal/loi/pygs",
    "https://lyellcollection.org/journal/loi/qjegh",
    "https://lyellcollection.org/journal/loi/sjg",
]

subjects = [
    "Geochemistry & Geophysics",
    "Geosciences, Multidisciplinary",
    "Geosciences, Multidisciplinary",
    "Geology",
    "Engineering, Geological | Geosciences, Multidisciplinary",
    "Geology",
]

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "cookie": "MAID=CGlr6/FX5dxiaXXY5v1L3g==; cookiePolicy=accept; _gid=GA1.2.764233054.1675884477; __atuvc=1%7C5%2C1%7C6; JSESSIONID=ced4f76f-bfe5-4c7e-a05d-80f14faa834d; SERVER=OaDmzsPRQ+Wqq1vvAiikhWnYKk2egCfX; MACHINE_LAST_SEEN=2023-02-08T12%3A40%3A31.110-08%3A00; __cf_bm=eTYqKR4FwwgeJ3Z0h27O.3EqwPLGcnjDIQOIIjhsIj0-1675889026-0-AV+MDofveNcVI7YgfLjAPrEsdJvAFbodfX8VU0vFc9q0PaJPa8VB0RVfoSKebrdMx7EgSdTbNcmjclok+TQxo/jUrxxw579ObPbwRwlA4jd/8zQY+WEDdyepI9vfvA6jGoUblTb7SbrsiH1H8GRMin0fGZFYNwbKGSxKy13ttlOM1t0RKqraUpQaOCre/If+XQ==; _dc_gtm_UA-55900466-1=1; _ga_CVD5VRS0K3=GS1.1.1675888831.3.1.1675889499.0.0.0; _ga=GA1.1.1539267343.1675188166; _ga_PFRX4WPGSL=GS1.1.1675888831.3.1.1675889499.0.0.0"
}

def filter_year(paper):
    year = paper.text.strip()
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
    if not paper.select_one('span.issue-item-access'):
        return

    if not paper.find('a', title=re.compile(r"PDF/EPUB")):
        return

    pdf_url = paper.find('a', title=re.compile(r"PDF/EPUB"))['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url.replace("reader", "pdf") + '?download=true'
  
    article_url = paper.select_one('div.issue-item__title a')['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = paper.select_one('div.issue-item__title').text.strip()
    authors = ""
    if paper.select_one('div.issue-item__authors'):
        authors = paper.select_one('div.issue-item__authors').text.replace('\n', '').strip()
 
    abstract = ''
    if paper.select_one('div.issue-item__footer div.accordion__content'):
        abstract = paper.select_one('div.issue-item__footer div.accordion__content').text.strip()
    published_at = paper.select('div.issue-item__header > span')[-1].text.strip()
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
    res = bs(fetch_url(start_url, headers).text, 'lxml')
    volumes = res.select("div.nested-tab div.swipe__wrapper ul li a")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        volume_url = domain + '/journal' + volume['href']
        volume_data = bs(fetch_url(volume_url).text, 'lxml')
        issues = volume_data.select('li.loi__issue')
        print(f'[volume] {volume_url} {len(issues)} issues')
        fetch_all(issues, journal_title, eissn, year, subject)
            
def parse_issue(issue, journal_title, eissn, year, subject):
    issue_url = issue.a['href']
    if not issue_url.startswith('http'):
        issue_url = domain + issue_url
    papers = bs(fetch_url(issue_url).text, 'lxml').select("div.issue-item")
    print(f'[{year}] {issue_url} {len(papers)} papers')
    for paper in papers:
        parse_paper_info(paper, journal_title, eissn, year, subject)

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
        for result in executor.map(parse_issue, list, [journal_title] * len(list), [eissn] * len(list), [year] * len(list), [subject] * len(list)):
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
    start()

    export_csv(csv_name, csv_data)

    # read_csv_data(csv_name, csv_data)

    # download_pdfs_via_thread(csv_data)