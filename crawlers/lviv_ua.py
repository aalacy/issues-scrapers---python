import os
import math
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
from pathvalidate import sanitize_filename
import re
import pdb

from util import (
    fetch_url, 
    create_dir, 
    export_csv, 
    read_csv_data, 
    download_pdfs_via_thread,
    set_up_selenium_webdriver,
    download_via_selenium
)

max_workers = 10

domain = "http://ifo.lviv.ua"

csv_name = "lviv_ua"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "INST PHYSICAL OPTICS"

journal_titles = [
    "UKRAINIAN JOURNAL OF PHYSICAL OPTICS"
]

eissns = [
    "1609-1833"
]

links = [
    "http://ifo.lviv.ua/journal/all_issues.html"
]

subjects = [
    "Optics"
]

def parse_paper_info(paper, journal_title, issn, published_year, subject):
    title = paper.a.strong.text.strip()
    article_url = paper.a['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    print(f'[paper] {article_url}')
    article = bs(fetch_url(article_url).text, 'lxml')
    if not article.find('a', href=re.compile(r".pdf$")):
        return
    pdf_url = article.find('a', href=re.compile(r".pdf$"))['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + '/journal' + pdf_url.replace('..', '')
    authors = ""
    if article.select_one('div[data-element_type="heading.default"] div.elementor-widget-container div > p'):
        authors = article.select_one('div[data-element_type="heading.default"] div.elementor-widget-container div > p').text.strip()
    abstract = ''
    if article.select_one('div[data-element_type="text-editor.default"] div.elementor-widget-container div div'):
        abstract = article.select_one('div[data-element_type="text-editor.default"] div.elementor-widget-container div div').text.strip()
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
    

def parse_paper_info1(paper, journal_title, issn, published_year, subject):
    if not paper.text.strip():
        return
    title = paper.b.text.strip()
    article_url = paper.a['href']
    if not article_url.startswith("http"):
        article_url = domain + f'/journal/{published_year}/' + article_url
    print(f'[paper] {article_url}')
    article = bs(fetch_url(article_url).text, 'lxml')
    if not article.find('a', href=re.compile(r".pdf$")):
        return
    pdf_url = article.find('a', href=re.compile(r".pdf$"))['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + '/journal' + pdf_url.replace('..', '')
    authors = ""
    if article.p:
        authors = article.p.text.strip()
    abstract = ''
    if article.find('b', text=re.compile(r"Abstract.")):
        abstract = article.find('b', text=re.compile(r"Abstract.")).next.next.text.strip()
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
   
def parse_volumes(volumes, journal_title, issn, subject):
    fetch_all(volumes, journal_title, issn, subject)

def parse_archive(base_url, journal_title, issn, subject):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')

    _vols = res.select("div.elementor-toggle div.elementor-toggle-item")
    volumes = [volume for volume in _vols if filter_year(volume)]

    print(f"{len(volumes)} volumes")
    parse_volumes(volumes, journal_title, issn, subject)

def filter_year(volume):
    year = volume.select_one('div.elementor-tab-title').text.split('-')[-1].strip()
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

def parse_full_issue(issue, journal_title, issn, year, subject):
    if not issue.select_one('a.full-issue-galley-link'):
        return

    title = issue.select_one('div.callout-title').text.strip()
    file_name = sanitize_filename(title) + '.pdf'
    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    pdf_url = issue.select_one('a.full-issue-galley-link')['href']
    csv_data.append({
        'Title': title, 
        'Source': PUBLISHER_NAME, 
        'Subject': journal_title, 
        'Sub Category': subject, 
        'Type': '', 
        'Authors': '', 
        'Published At': year, 
        'Published Year': year, 
        'Abstract': '',
        'File Path': file_path, 
        'PDF URL': pdf_url, 
        'Article URL': '', 
        'Created At': '',
        'Updated At': '',
        'download_url': pdf_url,
        'file_name': file_name
    })

def fetch_single(volume, journal_title, issn, subject):
    year = filter_year(volume)
    if not year:
        return
    issues = volume.select("ul.volume-issue-list li")
    for issue in issues:
        issue_url = issue.a['href']
        issue_data = bs(fetch_url(issue_url).text, 'lxml')
        papers = issue_data.select('div.elementor-widget-wrap ol li')
        if not papers:
            papers = issue_data.select('body table tr')
            for paper in papers:
                parse_paper_info1(paper, journal_title, issn, year, subject)
        else:
            for paper in papers:
                parse_paper_info(paper, journal_title, issn, year, subject)

def fetch_all(list, journal_title, eissn, subject, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(fetch_single, list, [journal_title] * len(list), [eissn] * len(list), [subject] * len(list)):
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
    # set_up_selenium_webdriver()

    # download_via_selenium(csv_data)