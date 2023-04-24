import csv
from sgrequests import SgRequests
import requests
import httpx
import os
from pathvalidate import sanitize_filename
import math
from concurrent.futures import ThreadPoolExecutor
from seleniumwire import webdriver 
from selenium import webdriver as NormalWebdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from time import sleep
import shutil
from bs4 import BeautifulSoup as bs
import pdb

max_workers = 4

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

def _header1(file):
    return {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Host": "www.bloodtransfusion.it",
    "Referer": f"https://www.bloodtransfusion.it/plugins/generic/pdfJsViewer/pdf.js/web/viewer.html?file={file}"
}

BASE_PATH = "/home/com/SData/"
PROJ_PATH = os.path.abspath(os.curdir)
proxy_url = "https://api.scraperapi.com?api_key=3664a*****&url="

csv_header = ['Title', 'Source', 'Subject', 'Sub Category', 'Type', 'Authors', 'Published At', 'Published Year', 'Abstract', 'File Path', 'PDF URL', 'Article URL', 'Created At', 'Updated At']
black_years = ['2015', '2016', '2017', '2018']

pdfs = []
file_paths = []

proxies = {
    "http://": "http://37.48.118.4:13081",
    "https://": "http://5.79.66.2:13081"
}

def invalid_year(published_at):
    is_skip = False
    for year in black_years:
        if year in str(published_at):
            is_skip = True
            print(f'[invalid] {published_at}')
            break
    return is_skip

def fetch_url(url, headers=_headers, scraper_api=False, need_proxies=False):
    _url = url
    if scraper_api:
        _url = f"{proxy_url}{url}"
    if need_proxies:
        with requests.Session() as s:
            return s.get(_url, headers=headers, proxies=proxies)
    else:
        with SgRequests() as s:
            return s.get(_url, headers=headers)

def clear_dir(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)

def create_dir(dir_name, new=False):
    dir_path = os.path.join(BASE_PATH, dir_name)
    if new:
        clear_dir(dir_path)
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

def export_csv(csv_name, csv_data, failed_files=[]):
    new_csv_data = []
    for data in csv_data:
        if data['File Path'] in failed_files:
            continue
        data.pop('download_url')
        data.pop('file_name')
        new_csv_data.append(data)

    print('[export csv] total: ', len(new_csv_data), ', failed files: ', len(failed_files))
    with open(f'{PROJ_PATH}/output/{csv_name}.csv', 'w', encoding='UTF8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_header)
        writer.writeheader()
        writer.writerows(new_csv_data)


def read_csv_data(csv_name, csv_data):
    print(f"[read_csv_data] {csv_name}")
    with open(f'{PROJ_PATH}/output/{csv_name}.csv', 'r') as data:
        for line in csv.DictReader(data):
            if invalid_year(line['Published At']):
                print(f'[skip pdf] {line["PDF URL"]}')
                continue
            file_name = sanitize_filename(line['Title']) + '.pdf'
            line['file_name'] = file_name
            line['download_url'] = line['PDF URL'].replace('/view/', '/download/')
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

def download_pdfs_via_thread(csv_data):
    for data in csv_data:
        pdfs.append(data['download_url'])
        file_paths.append(data['File Path'].replace(".pdf", "")[:255] + ".pdf")

    print('[start downloading] total: ', len(csv_data))
    fetch_pdfs(pdfs, file_paths)

def _download_requests(file_path, pdf_link, stream=True):
    with requests.get(pdf_link, stream=stream, proxies=proxies, allow_redirects=True) as r:
        with open(file_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

def _validate_pdf_link(pdf_link):
    res = bs(requests.get(pdf_link, allow_redirects=True, proxies=proxies).text, 'lxml')
    return res.select_one('embed')

def _download_httpx(file_path, pdf_link):
    with httpx.Client() as s:
        with open(file_path, "wb") as download_file:
            with s.stream("GET", pdf_link, allow_redirects=True) as response:
                for chunk in response.iter_bytes():
                    download_file.write(chunk)

def download(pdf_link, file_path, base_path=BASE_PATH):
    file_path = os.path.join(base_path, file_path)
    if os.path.exists(file_path):
        return
    print('--- downloading --- ', file_path, pdf_link)
    try:
        # if _validate_pdf_link(pdf_link):
        # _download_requests(file_path, pdf_link)
        _download_httpx(file_path, pdf_link)
    except Exception as err:
        print('[Download error]: ', err)

def download_via_selenium(csv_name, csv_data, wait_iframe=False, root_path=BASE_PATH):
    for data in csv_data:
        pdfs.append(data['download_url'])
        file_paths.append(data['File Path'].replace(".pdf", "")[:255] + ".pdf")

    temp_path = f'/tmp/{csv_name}'
    create_dir(temp_path, new=True)
    driver = set_up_selenium_webdriver(temp_path)
    for x in range(len(pdfs)):
        file = pdfs[x]
        try:
            dir_path = root_path + file_paths[x].split('/')[0]
            print(dir_path)
            print(f'[download] {file}')
            driver.get(file)
            count = 0
            while True:
                sleep(1)
                count += 1
                files = os.listdir(temp_path)
                if len(files) > 0 and files[0].endswith('.pdf'):
                    break
                if count > 20:
                    print('[error] [10s >] download')
                    break

            # read a recently downloaded file in the temp folder
            for latest_file in os.listdir(temp_path):
                latest_file_path = os.path.join(temp_path, latest_file)
                stat = os.stat(latest_file_path)
                if stat.st_size > 0:
                    right_path = os.path.join(root_path, file_paths[x])
                    if not os.path.exists(right_path):
                        shutil.move(latest_file_path, right_path)
                break
            # move the file from temp to the right folder
        except Exception as error:
            print(error)
            continue
        if wait_iframe:
            while True:
                if 'iframe' in driver.page_source:
                    break
                sleep(1)
            pdf_link = driver.page_source.split('<iframe src=')[1].split('width=')[0].strip()[1:-1]
            driver.get(pdf_link)
            WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div#outerContainer")))
            sleep(1)
            a = ActionChains(driver)
            # perform the ctrl+c pressing action
            a.key_down(Keys.CONTROL).send_keys('s').key_up(Keys.CONTROL).perform()

def set_up_selenium_webdriver(root_path=BASE_PATH, proxy=False):
    options = Options()
    # options.headless = True
    options.add_argument("--disable-gpu")
    options.add_argument("--start-maximized") #open Browser in maximized mode
    options.add_argument("--no-sandbox") #bypass OS security model
    options.add_argument("--disable-dev-shm-usage") #overcome limited resource problems
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--ignore-certificate-errors")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-proxy-server")
    options.add_experimental_option('prefs', {
        "download.default_directory": root_path, 
        "download.prompt_for_download": False, #To auto download the file
        "download.directory_upgrade": True,
        "disable-popup-blocking": "true",
        "download.extensions_to_open": "applications/pdf",
        "plugins.plugins_list": [{"enabled": False,
                                         "name": "Chrome PDF Viewer"}],
        "plugins.always_open_pdf_externally": True, #It will not show PDF directly in chrome
        'profile.default_content_setting_values.automatic_downloads': True
    })
    if proxy:
        proxy_options = {
            'proxy': {
                "http": "http://5.79.66.2:13081",
                'no_proxy': 'localhost,127.0.0.1'
            }  
        }
        return webdriver.Chrome(options=options, executable_path=ChromeDriverManager().install(), seleniumwire_options=proxy_options)
    return webdriver.Chrome(options=options, executable_path=ChromeDriverManager().install())

def setup_chrome_profile(root_path=BASE_PATH):
    options = Options()
    options.add_argument('user-data-dir=/home/com/.config/google-chrome/Profile 2')
    options.add_argument('--profile-directory=Profile 2')
    options.add_argument("--disable-gpu")
    options.add_argument("--start-maximized") #open Browser in maximized mode
    options.add_argument("--no-sandbox") #bypass OS security model
    options.add_argument("--disable-dev-shm-usage") #overcome limited resource problems
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--ignore-certificate-errors")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-proxy-server")
    return NormalWebdriver.Chrome(options=options, executable_path=ChromeDriverManager().install())

if __name__ == '__main__':
    # BASE_PATH = os.path.abspath(os.curdir)
    # pdf_link = "https://pubs.acs.org/doi/pdf/10.1021/acsami.2c13017"
    # file_path = "test.pdf"
    # # download(pdf_link, file_path, BASE_PATH)
    # driver = set_up_selenium_webdriver()
    # driver.get(pdf_link)
    # pdb.set_trace()
    pass