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

max_workers = 24

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

journal_titles = [
    "CHALCOGENIDE LETTERS",
    "DIGEST JOURNAL OF NANOMATERIALS AND BIOSTRUCTURES",
    "JOURNAL OF OVONIC RESEARCH",
]

eissns = [
    "1584-8663",
    "1842-3582",
    "1584-9953",
]

links = [
    "https://chalcogen.ro/index.php/journals/chalcogenide-letters?showall=1",
    "https://chalcogen.ro/index.php/journals/digest-journal-of-nanomaterials-and-biostructures?showall=1",
    "https://chalcogen.ro/index.php/journals/journal-of-ovonic-research?showall=1",
]

domain = "https://chalcogen.ro"
BASE_PATH = os.path.abspath(os.curdir)
proxy_url = "http://api.scraperapi.com?api_key=01fdbc140ea84935ef164c5c1d2797bd&url="

csv_header = ['Title', 'Source', 'Subject', 'Sub Category', 'Type', 'Authors', 'Published At', 'Published Year', 'Abstract', 'File Path', 'PDF URL', 'Article URL', 'Created At', 'Updated At']
csv_data = []
pdfs = []
file_paths = []
failed_files = []

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
    return page_source.select('div.representation.overview.search div.product-listing-with-inputs-content ul.details')

def fetchSingle(base_url, journal_title, issn):
    print(f"[sci] [chalcogen] {base_url}")
    res = bs(fetch_url(base_url).text, 'lxml')
    years = res.find_all('a', href=re.compile(r"/index.php/journals/"))

    for _year in years:
        if "Number " not in _year.text and "Volume " not in _year.text:
            continue
        try:
            year = int(_year.text.strip().split()[-1].strip())
        except:
            continue
        if year > 2018:
            issue_url = domain + _year['href']
            print(f"[{year}] {issue_url}")
            page_source = bs(fetch_url(issue_url).text, 'lxml')
            papers = page_source.select("div[itemprop='articleBody'] ul")

            for paper in papers[:-1]:
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
    with open('chalcogen.csv', 'w', encoding='UTF8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_header)
        writer.writeheader()
        writer.writerows(new_csv_data)


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
        with open(file_path, "wb") as download_file:
            with requests.get(pdf_link, stream=True) as response:
                shutil.copyfileobj(response.raw, download_file)
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
    if paper.select('li p'):
        title = paper.select_one('span b').text.strip()
        siblings = [p for p in paper.find_next_siblings('p') if p.text.strip()]
        if siblings and siblings[0].a:
            pdf_url = domain + siblings[0].a['href']
        else:
            pdf_url = domain + paper.select('p')[-1].a['href']
        authors = paper.select('p')[1].text.strip()
    else:
        title = paper.select_one('li a').text.strip()
        pdf_url = domain + paper.select_one('li a')['href']
        authors = title.split('"')[0].strip()
    article_url = pdf_url
    abstract = ''
    published_at = published_year
    file_name = sanitize_filename(title) + '.pdf'
    flength = len(file_name)
    if flength > 150: # If filenameis longer than 70, truncate filename
        offset = flength - (flength % 40 + 20) # modulus of file name + 20 to prevent file type truncation
        file_name = file_name[offset:]

    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    csv_data.append({
        'Title': title, 
        'Source': 'VIRTUAL CO PHYSICS SRL', 
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
    start()

    download_pdfs_via_thread()

    export_csv()