import requests
from bs4 import BeautifulSoup as bs
import pdb
import os
from pathvalidate import sanitize_filename
import math
from concurrent.futures import ThreadPoolExecutor
import csv
import shutil

max_workers = 32

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

domain = "http://www.techno-press.org"
base_url = "http://www.techno-press.org/"
BASE_PATH = os.path.abspath(os.curdir)
base = BASE_PATH + "/JARQ-JAPAN AGRICULTURAL RESEARCH QUARTERLY (2185-8896)/"

proxies = {
    "http://": "http://37.48.118.90:13042",
}

csv_header = ['Title', 'Source', 'Subject', 'Sub Category', 'Type', 'Authors', 'Published At', 'Published Year', 'Abstract', 'File Path', 'PDF URL', 'Article URL', 'Created At', 'Updated At']
csv_data = []
pdfs = []
file_paths = []
failed_files = []

def fetch_url(url):
    with requests.Session() as s:
        return s.get(url, headers=_headers)

def fetchSingle(journal):
    journal_title = journal.select_one('span#Journaltitle').text.strip()
    article_url = domain + journal['href']
    print('[{}]'.format(journal_title))
    cover = bs(fetch_url(article_url).text, 'lxml')
    issn = ''
    contents = [content for content in cover.select('div#subpage7 td.loginBox') if content.text.strip()]
    for content in contents:
        _text = content.text.strip()
        if 'ISSN' not in _text:
            continue
        issn = _text.split('ISSN')[-1].split('(')[0].replace(':', '').strip()
    tob = cover.select_one('div#subpage5 td.editorialb')
    for tr in tob.select('tr'):
        if not tr.text.strip():
            # empty row
            continue
        if filter_out_2019(tr):
            for volume in tr.select('a'):
                volume_url = domain + volume['href'].strip()
                _v = volume['href'].split('\n')
                if len(_v) > 1:
                    volume_url = domain + _v[0].strip() + _v[-1].strip() 
                paper_page = bs(fetch_url(volume_url).text, 'lxml')
                published_at = paper_page.select('div#main > table tr')[-1].td.text.strip().split(',')[-1].strip()
                print('[{}] [{}] [{}]'.format(article_url, volume_url, published_at))
                papers = paper_page.select('div.paper_info')
                for paper in papers:
                    parse_paper_info(paper, journal_title, published_at, issn, article_url)

def fetch_all(list, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(fetchSingle, list):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

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
    try:
        with open(file_path, "wb") as download_file:
            with requests.get(pdf_link, stream=True) as response:
                shutil.copyfileobj(response.raw, download_file)
    except Exception as err:
        print('[Download error]: ', err)
        failed_files.append(file_path)

    return True

def filter_out_2019(tr):
    is_above_2019 = True
    for td in tr.select('td'):
        if not td.text.strip():
            continue
        if 'Volume' in td.text:
            text_list = td.text.strip().split()
            year = ''
            for x, txt in enumerate(text_list):
                if 'Volume' in txt:
                    year = text_list[x-1]
                    break
            if int(year) < 2019:
                is_above_2019 = False
                break

    return is_above_2019

def export_csv():
    new_csv_data = []
    for data in csv_data:
        if data['File Path'] in failed_files:
            continue
        data.pop('download_url')
        data.pop('file_name')
        new_csv_data.append(data)

    print('[export csv] total: ', len(new_csv_data), ', failed files: ', len(failed_files))
    with open('techno.csv', 'w', encoding='UTF8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_header)
        writer.writeheader()
        writer.writerows(new_csv_data)

def create_dir(dir_name):
    dir_path = os.path.join(BASE_PATH, dir_name)
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

def download_pdfs_via_thread():
    for data in csv_data:
        pdfs.append(data['download_url'])
        file_paths.append(data['File Path'])

    print('[start downloading] total: ', len(csv_data))
    fetch_pdfs(pdfs, file_paths)

def parse_paper_info(paper, journal_title, published_at, issn, article_url):
    trs = [tr for tr in paper.select('table tr') if tr.text.strip()]
    title = trs[-2].a.text.strip()
    pdf_url = domain + '/' + trs[-2].a['href']
    authors = trs[-2].td.contents[-1].text.strip()
    abstract = paper.select_one('p.paper_view').contents[3].text.strip()
    download_url =  domain + '/' + trs[-1].select('a')[1]['href']
    published_year = published_at.split('(')[0].strip().split()[-1]
    file_name = sanitize_filename(title + '.pdf')
    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    csv_data.append({
        'Title': title, 
        'Source': 'Techno-Press', 
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
        'download_url': download_url,
        'file_name': file_name
    })

    
def start():
    journals = bs(fetch_url(base_url).text, 'lxml').select('div.journal a')

    fetch_all(journals)
        
if __name__ == '__main__':
    start()

    # download_pdfs_via_thread()

    export_csv()