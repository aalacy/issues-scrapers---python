# cloudflare
import requests
from bs4 import BeautifulSoup as bs
import pdb
import os
from pathvalidate import sanitize_filename
import csv
import shutil
from seleniumwire import webdriver 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

max_workers = 24

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

journal_titles = [
    "AMERICAN JOURNAL OF AUDIOLOGY",
    "AMERICAN JOURNAL OF SPEECH-LANGUAGE PATHOLOGY",
    "JOURNAL OF SPEECH LANGUAGE AND HEARING RESEARCH",
    "LANGUAGE SPEECH AND HEARING SERVICES IN SCHOOLS",
]

eissns = [
    "1558-9137",
    "1558-9110",
    "1558-9102",
    "1558-9129",
]

links = [
    "https://pubs.asha.org/loi/aja",
    "https://pubs.asha.org/loi/ajslp",
    "https://pubs.asha.org/loi/jslhr",
    "https://pubs.asha.org/loi/lshss",
]

domain = "https://pubs.asha.org"
BASE_PATH = os.path.abspath(os.curdir)
proxy_url = "https://api.scraperapi.com?api_key=3664a7c3***ecd461ad4c8bf0&url="

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

def export_csv():
    new_csv_data = []
    for data in csv_data:
        if data['File Path'] in failed_files:
            continue
        new_csv_data.append(data)

        print('[export csv] total: ', len(new_csv_data), ', failed files: ', len(failed_files))
        with open('lww.csv', 'w', encoding='UTF8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_header)
            writer.writeheader()
            writer.writerows(new_csv_data)

class Scrapy:
    driver = None

    def __init__(self):
        self.set_up_selenium_webdriver()

    def set_up_selenium_webdriver(self):
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
        self.driver = webdriver.Chrome(options=options, executable_path=ChromeDriverManager().install())

    def fetchSingle(self, base_url, journal_title, issn):
        print(f"[sci] [asha] [{issn}] {base_url}")
        self.driver.get(base_url)
        _10years = self.driver.find_element(By.CSS_SELECTOR, "div.loi-list__wrapper div.scroll ul.rlist")
        for _10year in _10years.find_elements(By.CSS_SELECTOR, "li a"):
            if int(_10year.text.replace('s', '').strip()) < 2010:
                continue
            self.driver.execute_script("arguments[0].click();", _10year)
        else:
            issue = res.select_one("section#wpCurrentIssue div.content-box-body h3 a")
            if not issue:
                print(f"[error] {base_url}")
                self.driver.get(base_url)
                time.sleep(2)
                self.parse_issue(journal_title, issn)
                return

            year = int(issue.text.split('Volume')[0].strip().split('/')[-1].split('-')[-2].strip().split()[-1].strip())
            self.fetch_issue(issue, journal_title, issn, year)
        
    def fetch_issue(self, issue, journal_title, issn, year):
        if year < 2019:
            return
        issue_link = domain + issue['href']
        self.driver.get(issue_link)
        time.sleep(2)

        self.parse_issue(journal_title, issn, year)
        
    def parse_issue(self, journal_title, issn, year=2022):
        while True:
            try:
                WebDriverWait(self.driver, 3).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.article-list article")))

                WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "article button")))
            except:
                print(f"[error] [No element to download within 3 secs] {issn}")
                break

            articles = self.driver.find_elements(By.CSS_SELECTOR, "div.article-list article")
            accept_all_cookie_btn = self.driver.find_elements(By.CSS_SELECTOR, "button#onetrust-accept-btn-handler")
            if accept_all_cookie_btn:
                self.driver.execute_script("arguments[0].click();", accept_all_cookie_btn[0])
                time.sleep(1)

            print(f"[{issn}] {len(articles)}")
            for article in articles:
                self.parse_paper_info(article, journal_title, issn, year)
      
            paginations = self.driver.find_elements(By.CSS_SELECTOR, "li a.element__nav.element__nav--next")

            if not paginations:
                break

            self.driver.execute_script("arguments[0].click();", paginations[0])
            time.sleep(2)
    

    def download(self, pdf_link, file_path):
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

    def parse_paper_info(self, paper, journal_title, issn, published_year):
        self.driver.execute_script("arguments[0].scrollIntoView();", paper)
        time.sleep(1)
        try:
            WebDriverWait(paper, 3).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button")))
        except:
            return
        if not paper.find_elements(By.CSS_SELECTOR, 'button.user-menu__link--download'):
            return

        title = paper.find_element(By.CSS_SELECTOR, 'h4 a').text.strip()
        pdf_btn = paper.find_element(By.CSS_SELECTOR,'button.user-menu__link--download')
       
        authors = paper.find_element(By.CSS_SELECTOR, 'div.js-few-authors').text.replace("More", "").strip()
        article_url = paper.find_element(By.CSS_SELECTOR, 'h4 a').get_attribute('href')
        abstract = ''
        published_at = paper.find_element(By.CSS_SELECTOR, 'p.featuredArticleCitation').text.split(',')[-1].strip()
        if not published_year:
            published_year = published_at.split()[-1].strip()

        if int(published_year) < 2019:
            return

        file_name = article_url.split('/')[-1].replace('.aspx', '.pdf')
        dir = journal_title + '(' + issn +')'
        create_dir(dir)
        file_path = os.path.join(dir, file_name)
        if not os.path.exists(file_path):
            ActionChains(self.driver).key_down(Keys.CONTROL).click(pdf_btn).key_up(Keys.CONTROL).perform()
            downloaded_path = os.path.join("/home/com/Downloads", file_name)
            used_time = 0
            while not os.path.exists(downloaded_path):
                print(f'[waiting] {downloaded_path} ---')
                used_time += 1
                time.sleep(1)
                if used_time > 10 and used_time % 10 == 0:
                    ActionChains(self.driver).key_down(Keys.CONTROL).click(pdf_btn).key_up(Keys.CONTROL).perform()

            shutil.move(downloaded_path, file_path)
            time.sleep(1)

        csv_data.append({
            'Title': title, 
            'Source': 'LIPPINCOTT WILLIAMS & WILKINS', 
            'Subject': journal_title, 
            'Sub Category': '', 
            'Type': '', 
            'Authors': authors, 
            'Published At': published_at, 
            'Published Year': published_year, 
            'Abstract': abstract,
            'File Path': file_path, 
            'PDF URL': article_url, 
            'Article URL': article_url, 
            'Created At': '',
            'Updated At': '',
        })

        
    def start(self):
        for x in range(len(links)):
            self.fetchSingle(links[x], journal_titles[x], eissns[x])
        
if __name__ == '__main__':
    scrapy = Scrapy()

    scrapy.start()

    export_csv()