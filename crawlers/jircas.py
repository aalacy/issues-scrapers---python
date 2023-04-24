import httpx
from bs4 import BeautifulSoup as bs
from sglogging import SgLogSetup
import pdb
import os
from pathvalidate import sanitize_filename
from tenacity import retry, stop_after_attempt, wait_fixed
import math
from concurrent.futures import ThreadPoolExecutor
max_workers = 32

logger = SgLogSetup().get_logger("")

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

domain = "https://www.jircas.go.jp"
base_url = "https://www.jircas.go.jp/ja/publication"
BASE_PATH = os.path.abspath(os.curdir)
base = BASE_PATH + "/JARQ-JAPAN AGRICULTURAL RESEARCH QUARTERLY (2185-8896)/"

proxies = {
    "http://": "http://37.48.118.90:13042",
}

def fetch_url(url):
    with httpx.Client() as client:
        return client.get(url, headers=_headers)

def fetchSingle(link):
    data = []
    url = domain + link.a['href']
    page = 0
    page_header = None
    while True:
        sp0 = bs(fetch_url(f"{url}?page={page}").text, 'lxml')
        res = sp0.select('div.region-content li.media span.file-link a')
        if not res:
            break
        page_header = sp0.select_one('h1.page-header').text.strip()
        print(f"[{page_header}] [{page}] {len(res)}")
        page += 1
        data += res
    return data, page_header

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
                    logger.debug(f"Concurrent Operation count = {count}")
                output.append(result)
    return output

def fetch_pdfs(list, page_header, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(download, list, len(list) * [page_header]):
            if result:
                count = count + 1
                if count % reminder == 0:
                    logger.debug(f"Concurrent Operation count = {count}")
                output.append(result)
    return output


@retry(wait=wait_fixed(2), stop=stop_after_attempt(7))
def download(article, page_header):
    pdf_link = article['href']
    _name = page_header + '_' + sanitize_filename(article.text.strip())
    name = base + _name
    if os.path.exists(name):
        return
    logger.info(f'--- downloading --- {_name}')
    with open(name, "wb") as download_file:
        with httpx.stream(method="GET", url=pdf_link) as response:
            for chunk in response.iter_bytes():
                download_file.write(chunk)

    return True

def start():
    articles = bs(fetch_url(base_url).text, 'lxml').select('div#views-bootstrap-publication-list-page-page div.thumbnail')
    
    for links, page_header in  fetch_all(articles):
        fetch_pdfs(links, page_header)

if __name__ == '__main__':
    start()
