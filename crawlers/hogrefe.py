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
    invalid_year
)


max_workers = 1

domain = "https://econtent.hogrefe.com"

csv_name = "hogrefe"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "HOGREFE AG-HOGREFE AG SUISSE"

journal_titles = [
    "INTERNATIONAL JOURNAL FOR VITAMIN AND NUTRITION RESEARCH",
    "PFLEGE",
    "VASA-EUROPEAN JOURNAL OF VASCULAR MEDICINE",
    "ZEITSCHRIFT FUR NEUROPSYCHOLOGIE",
    "JOURNAL OF PSYCHOPHYSIOLOGY",
]

eissns = [
    "1664-2821",
    "1664-283X",
    "1664-2872",
    "1664-2902",
    "2151-2124",
]

links = [
    "https://econtent.hogrefe.com/loi/vit",
    "https://econtent.hogrefe.com/loi/pfl",
    "https://econtent.hogrefe.com/loi/vas",
    "https://econtent.hogrefe.com/loi/znp",
    "https://econtent.hogrefe.com/loi/jop",
]

subjects = [
    "Nutrition & Dietetics",
    "Nursing",
    "Peripheral Vascular Diseases",
    "Clinical Neurology | Psychology",
    "Neurosciences",
]

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "cookie": "MAID=J+Su1OqbVQWx5OdCfaIgfw==; _ga=GA1.2.526034038.1675187834; OptanonAlertBoxClosed=2023-01-31T17:57:15.447Z; __atssc=google%3B2; JSESSIONID=32c94b52-a5ae-4c93-b3a7-f26a52b41994; SERVER=lZreQKWihaaxplRZrnof02UQ0dzEEbI9; MACHINE_LAST_SEEN=2023-02-15T03%3A34%3A30.397-08%3A00; _gid=GA1.2.1039952512.1676460900; OptanonConsent=isIABGlobal=false&datestamp=Wed+Feb+15+2023+03%3A52%3A08+GMT-0800+(Pacific+Standard+Time)&version=6.8.0&hosts=&landingPath=NotLandingPage&groups=C0002%3A1%2CC0001%3A1%2CC0003%3A1%2CBG1%3A1&geolocation=US%3BCA&AwaitingReconsent=false; __atuvc=1%7C5%2C0%7C6%2C12%7C7; __atuvs=63ecc35faee10409006"
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
    if not paper.select_one('span.access__icon--open'):
        return

    if not paper.find('a', title=re.compile(r"PDF")):
        return

    pdf_url = paper.find('a', title=re.compile(r"PDF"))['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url.replace("epdf", "pdf") + '?download=true'
  
    article_url = paper.select_one('h5 a')['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = paper.select_one('h5.issue-item__title').text.strip()
    authors = ""
    if paper.select_one('div.issue-item__loa'):
        authors = paper.select_one('div.issue-item__loa').text.replace('\n', '').strip()
 
    abstract = ''
    if paper.select_one('div.issue-item__abstract p'):
        abstract = paper.select_one('div.issue-item__abstract p').text.strip()
    if paper.select_one('div.issue-item__details'):
        details = [ch for ch in paper.select_one('div.issue-item__details').children if ch]
        if details:
            published_at = details[0].split('•')[-1].strip()
            if published_at.endswith(','):
                published_at = published_at[:-1]
    else:
        published_at = published_year
    if invalid_year(published_at):
        return
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
    volumes = res.select("div.nested-tab  ul li a")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        volume_url = domain + volume['href']
        volume_data = bs(fetch_url(volume_url).text, 'lxml')
        issues = volume_data.select('li.issue-items__bordered')
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
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)