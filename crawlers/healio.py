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
    invalid_year,
    set_up_selenium_webdriver,
    download_via_selenium
)

max_workers = 1

domain = "https://journals.healio.com"

csv_name = "healio"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "SLACK INC"

journal_titles = [
    "JOURNAL OF CONTINUING EDUCATION IN NURSING",
    "JOURNAL OF GERONTOLOGICAL NURSING",
    "JOURNAL OF NURSING EDUCATION",
    "JOURNAL OF PEDIATRIC OPHTHALMOLOGY & STRABISMUS",
    "JOURNAL OF PSYCHOSOCIAL NURSING AND MENTAL HEALTH SERVICES",
    "JOURNAL OF REFRACTIVE SURGERY",
    "ORTHOPEDICS",
    "PEDIATRIC ANNALS",
    "RESEARCH IN GERONTOLOGICAL NURSING",
]

eissns = [
    "1938-2472",
    "1938-243X",
    "1938-2421",
    "1938-2405",
    "1938-2413",
    "1938-2391",
    "1938-2367",
    "1938-2359",
    "1938-2464",
]

links = [
    "https://journals.healio.com/loi/JCEN",
    "https://journals.healio.com/loi/JGN",
    "https://journals.healio.com/loi/JNE",
    "https://journals.healio.com/loi/JPOS",
    "https://journals.healio.com/loi/JPN",
    "https://journals.healio.com/loi/JRS",
    "https://journals.healio.com/loi/JPN",
    "https://journals.healio.com/loi/ped",
    "https://journals.healio.com/loi/RGN",
]

subjects = [
    "Nursing",
    "Geriatrics & Gerontology | Nursing",
    "Nursing",
    "Ophthalmology | Pediatrics",
    "Nursing",
    "Ophthalmology | Surgery",
    "Orthopedics",
    "Pediatrics",
    "Nursing",
]

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "cookie": "MAID=DtRSxA76B9pmTsDkochSCw==; bc_tstgrp=9; visid_incap_659627=2ZeTQLPRTZ2qhpyFnhJDYTZ72WMAAAAAQUIPAAAAAABXlcfjTlU11EQexYK+C8dA; _hjSessionUser_980112=eyJpZCI6Ijk3N2Q3NTJkLTU2OTctNTkwMS04Zjg2LWZlODU3MmIzZDhjNSIsImNyZWF0ZWQiOjE2NzUxOTcyNDQ5MzMsImV4aXN0aW5nIjp0cnVlfQ==; OptanonAlertBoxClosed=2023-02-07T21:48:06.243Z; _iidt=UXbxmWotnYkm+Mnqnm7NrAZJWFipoItIajITfcMJv0H8XNyNOlS1iMC+9HQB7LkZGeuo3/wdFS6VV6A6W85KxyUbRw==; _vid_t=B3NRUfcHSdatZkhvGw94xK//Fv7QmKPFLBLiOW+XPEGPOSPPIXzx+QGphnCFaMv3xJAQV8NEzo9+lMKSDvjV6A3FrQ==; incap_ses_880_659627=MJvHNPSZ20SyeYytHmQ2DA4l7WMAAAAA6Dv+bcjFGGSMMKRk8n1rmQ==; JSESSIONID=adb64597-0840-4468-9c44-ce51d8c3e2b9; SERVER=OaDmzsPRQ+WP+eNin7MeI2nYKk2egCfX; MACHINE_LAST_SEEN=2023-02-15T10%3A31%3A44.217-08%3A00; _gid=GA1.2.118823595.1676485907; BCSessionID=c0331677-3e71-4ffc-a0e1-48de86eed6b7; ASP.NET_SessionId=y1xlalccwbstuhcnos2k4znk; _hjSession_980112=eyJpZCI6IjU1Y2M2NWIyLWRlNTItNDQwYi05NTc3LTg4Nzk5NTI2ZjUwYSIsImNyZWF0ZWQiOjE2NzY0ODYyMjg2NDAsImluU2FtcGxlIjp0cnVlfQ==; _hjAbsoluteSessionInProgress=0; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Feb+15+2023+10%3A37%3A11+GMT-0800+(Pacific+Standard+Time)&version=6.38.0&isIABGlobal=false&hosts=&consentId=865162a0-3bda-4e5e-8c00-f7341b225513&interactionCount=2&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&geolocation=US%3BCA&AwaitingReconsent=false; _ga=GA1.2.1872458609.1675197192; _ga_TF2TKVNCJC=GS1.1.1676486226.6.0.1676486259.0.0.0; last_visit_bc=1676486986704; __atuvc=1%7C5%2C12%7C6%2C15%7C7; __atuvs=63ed25c2a42395e900e; _gat_gtag_UA_671605_73=1; __cf_bm=4tEbY.NzoEb.HLTSdyrSCEoADFgUE3WHRvq94tGoxQc-1676486987-0-AW9ecCP0K9dTFXjhZsXkw3cR4HS72dwCnwme3TyJyUwCjvCebbGPyqXbEuuSH8FOVzpxjMQ8cju1CO07ibbDDHWq1NdrbB43CpQt1a6dBYFNEGuyJpCz79wOku6rKUIBv1PX/cNuNKybCN/O6cat9NjkQRvyxI9TMlDeG90RveqWoyD7l18xhUWseRcbXPm/mQ=="
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
    if not paper.select_one('i.access-icon--open'):
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
            published_at = details[1].strip()
            if published_at.endswith('|'):
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
    driver = set_up_selenium_webdriver()
    driver.get(start_url)
    res = bs(driver.page_source, 'lxml')
    driver.close()
    volumes = res.select("div.nested-tab  ul li a")
    for volume in volumes:
        year = filter_year(volume)
        if not year:
            continue
        volume_url = domain + volume['href']
        driver = set_up_selenium_webdriver()
        driver.get(volume_url)
        sleep(randint(2, 5))

        volume_data = bs(driver.page_source, 'lxml')
        driver.close()
        issues = volume_data.select('li.issue-items__bordered')
        print(f'[volume] {volume_url} {len(issues)} issues')
        for issue in issues:
            parse_issue(issue, journal_title, eissn, year, subject)
            sleep(randint(2, 5))
            
def parse_issue(issue, journal_title, eissn, year, subject):
    issue_url = issue.a['href']
    if not issue_url.startswith('http'):
        issue_url = domain + issue_url

    driver = set_up_selenium_webdriver()
    driver.get(issue_url)

    papers = bs(driver.page_source, 'lxml').select("div.issue-item")
    driver.close()
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

    download_via_selenium(csv_name, csv_data)