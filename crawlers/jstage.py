import requests
from bs4 import BeautifulSoup as bs
import pdb
import os
from pathvalidate import sanitize_filename
import math
from concurrent.futures import ThreadPoolExecutor
import csv
import shutil
import re
import time

max_workers = 1

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

journal_titles = [
    "JOURNAL OF FIBER SCIENCE AND TECHNOLOGY",
    "PLANKTON & BENTHOS RESEARCH",
    "YAKUGAKU ZASSHI-JOURNAL OF THE PHARMACEUTICAL SOCIETY OF JAPAN",
    "JOURNAL OF PESTICIDE SCIENCE",
    "MYCOSCIENCE",
    "JOURNAL OF THE METEOROLOGICAL SOCIETY OF JAPAN",
    "SOLA",
    "ACTA PHYTOTAXONOMICA ET GEOBOTANICA",
    "BREEDING SCIENCE",
    "CIRCULATION JOURNAL",
    "JOURNAL OF ADVANCED MECHANICAL DESIGN SYSTEMS AND MANUFACTURING",
    "HORTICULTURE JOURNAL",
    "ACTA HISTOCHEMICA ET CYTOCHEMICA",
    "CELL STRUCTURE AND FUNCTION",
    "TRANSACTIONS OF THE JAPAN SOCIETY FOR AERONAUTICAL AND SPACE SCIENCES",
    "JOURNAL OF PROSTHODONTIC RESEARCH",
    "JOURNAL OF THE JAPAN PETROLEUM INSTITUTE",
    "NEUROLOGIA MEDICO-CHIRURGICA",
    "JOURNAL OF THE JAPAN INSTITUTE OF METALS AND MATERIALS",
    "MATERIALS TRANSACTIONS",
    "JOURNAL OF ATHEROSCLEROSIS AND THROMBOSIS",
    "ENDOCRINE JOURNAL",
    "JOURNAL OF EPIDEMIOLOGY",
    "PROCEEDINGS OF THE JAPAN ACADEMY SERIES B-PHYSICAL AND BIOLOGICAL SCIENCES",
    "JOURNAL OF MINERALOGICAL AND PETROLOGICAL SCIENCES",
    "INTERNATIONAL HEART JOURNAL",
    "IEICE TRANSACTIONS ON ELECTRONICS",
    "IEICE TRANSACTIONS ON INFORMATION AND SYSTEMS",
    "GEOCHEMICAL JOURNAL",
    "ELECTROCHEMISTRY",
    "JOURNAL OF NUTRITIONAL SCIENCE AND VITAMINOLOGY",
    "BIOSCIENCE OF MICROBIOTA FOOD AND HEALTH",
    "BIOMEDICAL RESEARCH-TOKYO",
    "ANTHROPOLOGICAL SCIENCE",
    "JOURNAL OF PHOTOPOLYMER SCIENCE AND TECHNOLOGY",
]

eissns = [
    "2189-7654",
    "1882-627X",
    "1347-5231",
    "1349-0923",
    "1618-2545",
    "1349-6476",
    "2186-9057",
    "2189-7042",
    "1347-3735",
    "1347-4820",
    "1881-3054",
    "2189-0110",
    "1347-5800",
    "1347-3700",
    "0549-3811",
    "2212-4632",
    "1349-8029",
    "1349-273X",
    "1880-6880",
    "1347-5320",
    "1880-3873",
    "1348-4540",
    "0917-5040",
    "1349-2896",
    "1349-3825",
    "1349-3299",
    "1745-1361",
    "1745-1353",
    "1880-5973",
    "2186-2451",
    "1881-7742",
    "2186-3342",
    "1880-313X",
    "1348-8570",
    "1349-6336",
]

links = [
    "https://www.jstage.jst.go.jp/browse/fiberst",
    "https://www.jstage.jst.go.jp/browse/pbr",
    "https://www.jstage.jst.go.jp/browse/yakushi",
    "https://www.jstage.jst.go.jp/browse/jpestics",
    "https://www.jstage.jst.go.jp/browse/mycosci/-char/ja",
    "http://www.jstage.jst.go.jp/browse/sola",
    "http://www.jstage.jst.go.jp/browse/jmsj/-char/en/",
    "https://www.jstage.jst.go.jp/browse/apg",
    "http://www.jstage.jst.go.jp/browse/jsbbs",
    "http://www.jstage.jst.go.jp/browse/circj",
    "http://www.jstage.jst.go.jp/browse/jamdsm/",
    "http://www.jstage.jst.go.jp/browse/ahc/_vols/-char/en",
    "http://www.jstage.jst.go.jp/browse/hortj",
    "https://www.jstage.jst.go.jp/browse/csf/-char/ja/",
    "https://www.jstage.jst.go.jp/browse/nmc/list/-char/en",
    "https://www.jstage.jst.go.jp/browse/jpi/-char/ja/",
    "https://www.jstage.jst.go.jp/browse/jpr",
    "http://tjsass.jstage.jst.go.jp/",
    "https://www.jstage.jst.go.jp/browse/jinstmet/-char/en",
    "https://www.jstage.jst.go.jp/browse/matertrans/-char/en",
    "https://www.jstage.jst.go.jp/browse/jat",
    "https://www.jstage.jst.go.jp/browse/endocrj",
    "https://www.jstage.jst.go.jp/browse/jea/_pubinfo/-char/en",
    "http://www.jstage.jst.go.jp/browse/pjab",
    "https://www.jstage.jst.go.jp/browse/jmps",
    "http://www.jstage.jst.go.jp/browse/ihj/",
    "https://www.jstage.jst.go.jp/browse/transinf",
    "https://www.jstage.jst.go.jp/browse/transele",
    "https://www.jstage.jst.go.jp/browse/geochemj/55/6/_contents/-char/en",
    "https://www.jstage.jst.go.jp/browse/electrochemistry/",
    "https://www.jstage.jst.go.jp/browse/jnsv/-char/en/",
    "https://www.jstage.jst.go.jp/browse/bmfh/",
    "http://www.jstage.jst.go.jp/browse/biomedres",
    "http://www.jstage.jst.go.jp/browse/ase",
    "http://www.jstage.jst.go.jp/browse/photopolymer",
]

domain = "https://www.jstage.jst.go.jp"
BASE_PATH = "/media/com/C69A54D99A54C817/pwj/"
proxy_url = "https://api.scraperapi.com?api_key=3664a7c3***ecd461ad4c8bf0&url="

csv_header = ['Title', 'Source', 'Subject', 'Sub Category', 'Type', 'Authors', 'Published At', 'Published Year', 'Abstract', 'File Path', 'PDF URL', 'Article URL', 'Created At', 'Updated At']
csv_data = []
pdfs = []
file_paths = []
failed_files = []
csv_name = "jstage"

def fetch_url(url):
    with requests.Session() as s:
        return s.get(f"{url}", headers=_headers)

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
    return page_source.select('div#search-resultslist-wrap ul.search-resultslisting li')

def fetchSingle(base_url, journal_title, issn):
    start_url = base_url + '/list/-char/en'
    print(f"[sci] [iwaponline] {start_url}")
    res = bs(fetch_url(start_url).text, 'lxml')
    volumes = res.select('div#searchbrowse-leftsection div.facetsearch-subheader')

    for volume in volumes:
        v_text = volume.span.a.text.strip()
        year = int(v_text.split('(')[1].split(')')[0].strip())
        print(f"[{start_url}] {year}")
        if year > 2018:
            papers = get_page_data(res)
            issues = volume.select("ul li a")
            for issue in issues:
                issue_url = issue['href']
                res = bs(fetch_url(issue_url).text, 'lxml')
                papers += get_page_data(res)

                print(f"[{v_text}] [{year}] [{issue_url}] {len(papers)}")
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
    with open(f'output/{csv_name}.csv', 'w', encoding='UTF8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_header)
        writer.writeheader()
        writer.writerows(new_csv_data)

def read_csv_data():
    print("[read_csv_data]")
    with open(f'../output/{csv_name}.csv', 'r') as data:
        for line in csv.DictReader(data):
            file_name = sanitize_filename(line['Title']) + '.pdf'
            line['file_name'] = file_name
            line['download_url'] = line['PDF URL'].replace("https://www.jstage.jst.go.jphttps://www.jstage.jst.go.jp", "https://www.jstage.jst.go.jp")
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
    file_path = os.path.join(BASE_PATH, file_path)
    if os.path.exists(file_path):
        return
    print('--- downloading --- ', file_path, pdf_link)
    try:
        with requests.get(pdf_link, stream=True) as response:
            with open(file_path, "wb") as download_file:
                shutil.copyfileobj(response.raw, download_file)

        time.sleep(1)
    except Exception as err:
        print('[Download error]: ', err)
        failed_files.append(file_path)

    return True

def download_pdfs_via_thread():
    for data in csv_data:
        pdfs.append(data['download_url'])
        file_paths.append(data['File Path'].replace(".pdf", "")[:255] + ".pdf")

    print('[start downloading] total: ', len(csv_data))
    fetch_pdfs(pdfs, file_paths)

def parse_paper_info(paper, journal_title, issn, published_year):
    if not paper.select_one('span.freeaccess-tag-style'):
        return

    if not paper.select_one('div.lft'):
        return

    article_url = domain + paper.select_one('div.searchlist-title a')['href']
    title = paper.select_one('div.searchlist-title a').text.strip()
    pdf_url = domain + paper.select_one('div.lft a')['href']
    authors = ""
    if paper.select_one('div.searchlist-authortags'):
        authors = paper.select_one('div.searchlist-authortags').text.strip()
    abstract = paper.select_one('div.abstract').text.strip().split('\n\t')[0].strip()
    published_at = paper.select_one('div.searchlist-additional-info').text.split('J-STAGE:')[-1].strip()
    file_name = sanitize_filename(title) + '.pdf'
    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    type = ''
    if paper.select_one('span.original-tag-style'):
        type = paper.select_one('span.original-tag-style').text.strip()
    csv_data.append({
        'Title': title, 
        'Source': 'TECHNICAL ASSOC PHOTOPOLYMERS,JAPAN', 
        'Subject': journal_title, 
        'Sub Category': '', 
        'Type': type, 
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

    read_csv_data()

    download_pdfs_via_thread()

    # export_csv()