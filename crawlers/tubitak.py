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

max_workers = 6

domain = "https://journals.tubitak.gov.tr"

csv_name = "tubitak"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "SCIENTIFIC TECHNICAL RESEARCH COUNCIL TURKEY-TUBITAK"

journal_titles = [
    "TURKISH JOURNAL OF CHEMISTRY",
    "TURKISH JOURNAL OF EARTH SCIENCES",
    "TURKISH JOURNAL OF MATHEMATICS",
    "TURKISH JOURNAL OF VETERINARY & ANIMAL SCIENCES",
    "TURKISH JOURNAL OF AGRICULTURE AND FORESTRY",
    "TURKISH JOURNAL OF BIOLOGY",
    "TURKISH JOURNAL OF BOTANY",
    "TURKISH JOURNAL OF ELECTRICAL ENGINEERING AND COMPUTER SCIENCES",
    "TURKISH JOURNAL OF MEDICAL SCIENCES",
    "TURKISH JOURNAL OF ZOOLOGY",
]

eissns = [
    "1300-0527",
    "1300-0985",
    "1303-6149",
    "1300-0128",
    "1303-6173",
    "1303-6092",
    "1303-6106",
    "1303-6203",
    "1303-6165",
    "1303-6114",
]

links = [
    "https://journals.tubitak.gov.tr/chem/",
    "https://journals.tubitak.gov.tr/earth/",
    "https://journals.tubitak.gov.tr/math/",
    "https://journals.tubitak.gov.tr/veterinary/",
    "https://journals.tubitak.gov.tr/agriculture/",
    "https://journals.tubitak.gov.tr/biology/",
    "https://journals.tubitak.gov.tr/botany/",
    "https://journals.tubitak.gov.tr/elektrik/",
    "https://journals.tubitak.gov.tr/medical/",
    "https://journals.tubitak.gov.tr/zoology/",
]

subjects = [
    "Chemistry, Multidisciplinary | Engineering, Chemical",
    "Geosciences, Multidisciplinary",
    "Mathematics",
    "Veterinary Sciences",
    "Agronomy",
    "Biology",
    "Plant Sciences",
    "Computer Science, Artificial Intelligence | Engineering, Electrical & Electronic",
    "Medicine, General & Internal",
    "Zoology",
]

def filter_year(paper):
    year = paper.h2.text.split('(')[-1].split(')')[0].strip()
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

def parse_paper_info(paper, journal_title, issn, published_year, subject):
    if not paper.select_one('p.pdf a'):
        return

    pdf_url = paper.select_one('p.pdf a')['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url
  
    article_url = paper.select('a')[1]['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    title = paper.select('a')[1].text.strip()
    if 'Cover and Contents' in title:
        return
    authors = ""
    if paper.select_one('span.auth'):
        authors = paper.select_one('span.auth').text.replace('\n', '').strip()
 
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
    start_url = base_url + 'all_issues.html'
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')
    volumes = res.select("div#toc div.item")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        issues = volume.select('h3.issue a')
        fetch_all(issues, journal_title, eissn, year, subject)
            
def parse_issue(issue, journal_title, eissn, year, subject):
    issue_url = issue['href']
    papers = bs(fetch_url(issue_url).text, 'lxml').select("div.article-list div.doc")
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