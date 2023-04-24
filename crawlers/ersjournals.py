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

max_workers = 16

domains = [
    "https://openres.ersjournals.com",
    "http://erj.ersjournals.com",
    "http://err.ersjournals.com",
]

csv_name = "ersjournals"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "EUROPEAN RESPIRATORY SOC JOURNALS LTD"

journal_titles = [
    "ERJ OPEN RESEARCH",
    "EUROPEAN RESPIRATORY JOURNAL",
    "EUROPEAN RESPIRATORY REVIEW",
]

eissns = [
    "2312-0541",
    "1399-3003",
    "1600-0617",
]

links = [
    "https://openres.ersjournals.com",
    "http://erj.ersjournals.com",
    "http://err.ersjournals.com",
]

subjects = [
    "Respiratory System",
    "Respiratory System",
    "Respiratory System",
]

def filter_year(paper):
    year = paper.text.strip()
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
    if not paper.select_one('div.highwire-cite-access'):
        return

    title = ''
    authors = ''
    if paper.select_one('div.highwire-cite-authors'):
        title = paper.select_one('span.highwire-cite-title').text.strip()
        authors = paper.select_one('div.highwire-cite-authors').text.replace('\n\n', '').strip()
    else:
        _title = paper.a.text.strip()
        title = _title.split('”')[0].replace('“', '')
        authors = _title.split('”')[-1].strip()
  
    article_url = paper.a['href']
    if not article_url.startswith("http"):
        article_url = domain + article_url
    print(f'[paper] [{published_year}] {article_url}')
    article = bs(fetch_url(article_url).text, 'lxml')
    if not article.find('a', href=re.compile(r".pdf$")):
        return

    pdf_url = article.find('a', href=re.compile(r".pdf$"))['href']
    if not pdf_url.startswith('http'):
        pdf_url = domain + pdf_url
   
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


def parse_archive(base_url, journal_title, eissn, subject, domain):
    start_url = base_url + '/content/by/year'
    print(f"[sci] [{csv_name}] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')
    volumes = res.select("ul.issue-browser.years li a")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        volume_url = domain + volume['href']
        volume_data = bs(fetch_url(volume_url).text, 'lxml')
        issues = volume_data.select('div.archive-issue-list ul div.issue-link a')
        print(f'[volume] {volume_url} {len(issues)} issues')
        for issue in issues:
            parse_issue(issue, journal_title, eissn, year, subject, domain)
            
def parse_issue(issue, journal_title, eissn, year, subject, domain):
    issue_url = domain + issue['href']
    issue_data = bs(fetch_url(issue_url).text, 'lxml')
    papers = issue_data.select("ul.toc-section li.toc-item")
    print(f'[{year}] {issue_url} {len(papers)} papers')
    if not papers:
        sub_issues = issue_data.select('div.issue-toc-section a')
        for sub_issue in sub_issues:
            sub_issue_url = domain + sub_issue['href'].replace('relevance-rank?', "relevance-rank%20numresults%3A100?")
            while True:
                sub_issue_data = bs(fetch_url(sub_issue_url).text, 'lxml')
                sub_papers = sub_issue_data.select("ul.highwire-search-results-list li")
                print(f'[sub page] [{year}] {sub_issue_url} {len(sub_papers)} papers') 
                fetch_all(sub_papers, journal_title, eissn, year, subject, domain)

                pagers = sub_issue_data.select_one('ul.pager li.pager-next a')
                if not pagers:
                    break
                
                print(f'[sub page] [next] {sub_issue_url}')
                sub_issue_url = domain + pagers['href']
    else:
        fetch_all(papers, journal_title, eissn, year, subject, domain)

def fetch_all(list, journal_title, eissn, year, subject, domain, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(parse_paper_info, list, [journal_title] * len(list), [eissn] * len(list), [year] * len(list), [subject] * len(list), [domain] * len(list)):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def start():
    for x in range(len(links)):
        parse_archive(links[x], journal_titles[x], eissns[x], subjects[x], domains[x])

if __name__ == '__main__':
    # start()

    # export_csv(csv_name, csv_data)

    read_csv_data(csv_name, csv_data)

    download_pdfs_via_thread(csv_data)