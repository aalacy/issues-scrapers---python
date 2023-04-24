from sgrequests import SgRequests
from bs4 import BeautifulSoup as bs
import pdb
import os
import httpx
from pathvalidate import sanitize_filename
import math
from concurrent.futures import ThreadPoolExecutor
import csv
import re
import time
from tenacity import retry, stop_after_attempt, wait_exponential

from util import download_via_selenium

max_workers = 1

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

journal_titles = [
    "EUROPEAN JOURNAL OF PHYSICAL AND REHABILITATION MEDICINE",
    "INTERNATIONAL ANGIOLOGY",
    "ITALIAN JOURNAL OF DERMATOLOGY AND VENEREOLOGY",
    "JOURNAL OF CARDIOVASCULAR SURGERY",
    "JOURNAL OF NEUROSURGICAL SCIENCES",
    "JOURNAL OF SPORTS MEDICINE AND PHYSICAL FITNESS",
    "MEDICINA DELLO SPORT",
    "MINERVA ANESTESIOLOGICA",
    "MINERVA BIOTECHNOLOGY AND BIOMOLECULAR RESEARCH",
    "MINERVA CARDIOLOGY AND ANGIOLOGY",
    "MINERVA ENDOCRINOLOGY",
    "MINERVA GASTROENTEROLOGY",
    "MINERVA MEDICA",
    "MINERVA PEDIATRICS",
    "MINERVA SURGERY",
    "MINERVA UROLOGY AND NEPHROLOGY",
    "PANMINERVA MEDICA",
    "QUARTERLY JOURNAL OF NUCLEAR MEDICINE AND MOLECULAR IMAGING",
]

eissns = [
    "1973-9095",
    "1827-1839",
    "2784-8450",
    "1827-191X",
    "1827-1855",
    "1827-1928",
    "1827-1863",
    "1827-1596",
    "2724-5934",
    "2724-5772",
    "2724-6116",
    "2724-5365",
    "1827-1669",
    "2724-5780",
    "2724-5438",
    "2724-6442",
    "1827-1898",
    "1827-1936",
]

links = [
    "http://www.minervamedica.it/en/journals/europa-medicophysica/index.php",
    "http://www.minervamedica.it/en/journals/international-angiology/index.php",
    "https://www.minervamedica.it/en/journals/Ital-J-Dermatol-Venereol/index.php",
    "http://www.minervamedica.it/en/journals/cardiovascular-surgery/index.php",
    "http://www.minervamedica.it/en/journals/neurosurgical-sciences/index.php",
    "http://www.minervamedica.it/en/journals/sports-med-physical-fitness/index.php",
    "http://www.minervamedica.it/en/journals/medicina-dello-sport/index.php",
    "http://www.minervamedica.it/en/journals/minerva-anestesiologica/index.php",
    "https://www.minervamedica.it/en/journals/minerva-biotechnology-biomolecular-research/index.php",
    "https://www.minervamedica.it/en/journals/minerva-cardiology-angiology/",
    "https://www.minervamedica.it/en/journals/minerva-endocrinology/index.php",
    "https://www.minervamedica.it/en/journals/gastroenterology/index.php",
    "http://www.minervamedica.it/en/journals/minerva-medica/index.php",
    "https://www.minervamedica.it/en/journals/minerva-pediatrics/index.php",
    "https://www.minervamedica.it/en/journals/minerva-surgery/index.php",
    "https://www.minervamedica.it/en/journals/minerva-urology-nephrology/index.php",
    "http://www.minervamedica.it/en/journals/panminerva-medica/index.php",
    "http://www.minervamedica.it/en/journals/nuclear-med-molecular-imaging/index.php",
]

domain = "https://www.minervamedica.it"
BASE_PATH = "/media/com/C69A54D99A54C817/pwj/"
proxy_url = "https://api.scraperapi.com?api_key=3664a7c3***ecd461ad4c8bf0&url="

csv_header = ['Title', 'Source', 'Subject', 'Sub Category', 'Type', 'Authors', 'Published At', 'Published Year', 'Abstract', 'File Path', 'PDF URL', 'Article URL', 'Created At', 'Updated At']
csv_name = "minervamedica"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "EDIZIONI MINERVA MEDICA"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_url(url):
    with SgRequests() as s:
        try:
            res = s.get(f"{proxy_url}{url}", headers=_headers)
            if res.status_code != 200:
                raise Exception

            return res
        except Exception as error:
            print(error)

def create_dir(dir_name):
    dir_path = os.path.join(BASE_PATH, dir_name)
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

def filter_out_2019(tr):
    year = -1
    a = tr.a
    if 'Volume' in a.text:
        year = a.text.strip().split('-')[0].strip()

    try:
        return int(year)
    except:
        return int(year.split()[-1].strip())

def get_page_data(page_source):
    return page_source.select('div.representation.overview.search div.product-listing-with-inputs-content ul.details')

def fetchSingle(base_url, journal_title, issn):
    start_url = base_url
    print(f"[sci] [{csv_name}] {base_url}")
    res = bs(fetch_url(base_url).text, 'lxml')
    start_url = os.path.dirname(base_url) + '/' + res.find('a', text=re.compile(r"past issues", re.I))['href']

    past_issues = bs(fetch_url(start_url).text, 'lxml').select('div#pluto h4 a')
    for volume in past_issues:
        try:
            year = int(volume.text.split(';')[0].split()[-1].strip())
        except Exception as error:
            print(f'[year] {error}')
            continue
        print(f"[{start_url}] {year}")
        if year > 2018:
            volume_url = os.path.dirname(base_url) + '/' + volume['href']
            print(f'[{year}] [{volume_url}]')
            issues = bs(fetch_url(volume_url).text, 'lxml').select('div#pluto p.m2 > a')
            for issue in issues:
                issue_url = os.path.dirname(base_url) + '/' + issue['href']
                try:
                    page_source = bs(fetch_url(issue_url).text, 'lxml')
                except:
                    time.sleep(1)
                    continue

                papers = page_source.select("div#pluto p.rubrica")

                print(f"[{volume.text}] [{year}] [{issue_url}] {len(papers)}")
                for paper in papers:
                    parse_paper_info(paper, journal_title, issn, year)


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
        for result in executor.map(fetchSingle, list, journal_titles, eissns):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def export_csv():
    new_csv_data = []
    for data in csv_data:
        if data['File Path'] in failed_files:
            continue
        data.pop('download_url')
        data.pop('file_name')
        new_csv_data.append(data)

    print('[export csv] total: ', len(new_csv_data), ', failed files: ', len(failed_files))
    with open(f'../output/{csv_name}.csv', 'w', encoding='UTF8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_header)
        writer.writeheader()
        writer.writerows(new_csv_data)

def read_csv_data():
    print("[read_csv_data]")
    with open(f'../output/{csv_name}.csv', 'r') as data:
        for line in csv.DictReader(data):
            file_name = sanitize_filename(line['Title']) + '.pdf'
            line['file_name'] = file_name
            line['download_url'] = line['PDF URL']
            csv_data.append(line)

def fetch_pdfs(pdf_list, path_list, occurrence=max_workers):
    output = []
    total = len(pdf_list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(download, pdf_list, path_list):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def download(pdf_link, file_path):
    print('--- downloading --- ', file_path, pdf_link)
    file_path = os.path.join(BASE_PATH, file_path)
    if os.path.exists(file_path):
        return
    try:
        with httpx.Client() as s:
            new_res = s.get(f"{pdf_link}", headers=_headers, allow_redirects=True)
            with open(file_path, "wb") as download_file:
                with s.stream('GET', new_res.url) as response:
                    for chunk in response.iter_bytes():
                        download_file.write(chunk)
                        
    except Exception as err:
        print('[Download error]: ', err)
        failed_files.append(file_path)

    return True

def download_pdfs_via_thread():
    for data in csv_data:
        pdfs.append(data['download_url'])
        file_paths.append(data['File Path'])

    print('[start downloading] total: ', len(csv_data))
    fetch_pdfs(pdfs, file_paths)

def parse_paper_info(paper, journal_title, issn, published_year):
    if 'Open access' not in paper.text:
        return
    siblings = paper.find_next_siblings('p')
    pdf_url = siblings[-1].find('a', text=re.compile(r"PDF", re.I))
    if not pdf_url:
        return

    pdf_url = domain + pdf_url['href']
    article_url = domain +'en/' + siblings[-1].find('a', text=re.compile(r"HTML", re.I))['href'].replace('../', '')
    title = siblings[1].text.strip()
    authors = siblings[2].text.strip()
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

    
def start():
    fetch_all(links, journal_titles, eissns)
        
if __name__ == '__main__':
    # start()

    # export_csv()

    read_csv_data()

    download_pdfs_via_thread()