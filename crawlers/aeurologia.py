import os
import math
from bs4 import BeautifulSoup as bs
from pathvalidate import sanitize_filename
import re
from selenium.webdriver.common.by import By
from time import sleep
import shutil

from util import (
    fetch_url, 
    create_dir, 
    clear_dir,
    export_csv, 
    read_csv_data, 
    download_pdfs_via_thread,
    invalid_year,
    set_up_selenium_webdriver
)


max_workers = 1

domain = "https://www.aeurologia.com"

csv_name = "aeurologia"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "INIESTARES, S.A."
BASE_PATH = "/home/com/SData/"
temp_path = f'/tmp/{csv_name}'
create_dir(temp_path, new=True)

journal_titles = [
    "ARCHIVOS ESPANOLES DE UROLOGIA",
]

eissns = [
    "1576-8260",
]

links = [
    "https://www.aeurologia.com/EN/archive_by_years",
]

subjects = [
    "Urology & Nephrology",
]

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "cookie": "MAID=J+Su1OqbVQWx5OdCfaIgfw==; _ga=GA1.2.526034038.1675187834; OptanonAlertBoxClosed=2023-01-31T17:57:15.447Z; __atssc=google%3B2; JSESSIONID=32c94b52-a5ae-4c93-b3a7-f26a52b41994; SERVER=lZreQKWihaaxplRZrnof02UQ0dzEEbI9; MACHINE_LAST_SEEN=2023-02-15T03%3A34%3A30.397-08%3A00; _gid=GA1.2.1039952512.1676460900; OptanonConsent=isIABGlobal=false&datestamp=Wed+Feb+15+2023+03%3A52%3A08+GMT-0800+(Pacific+Standard+Time)&version=6.8.0&hosts=&landingPath=NotLandingPage&groups=C0002%3A1%2CC0001%3A1%2CC0003%3A1%2CBG1%3A1&geolocation=US%3BCA&AwaitingReconsent=false; __atuvc=1%7C5%2C0%7C6%2C12%7C7; __atuvs=63ecc35faee10409006"
}

driver = None

class Scraper:
    root_path = None

    def filter_year(self, paper):
        year = paper.text.strip().split()[0]
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

    def download_and_move(self, paper, file_path, file_name):
        print(f'[download_and_move]', file_name)
        paper.find_element(By.CSS_SELECTOR, file_path).click()
        count = 0
        while True:
            sleep(1)
            count += 1
            files = os.listdir(temp_path)
            if len(files) > 0 and files[0].endswith('.pdf'):
                break
            if count > 100:
                print('[error] [100s >] download')
                break

        # read a recently downloaded file in the temp folder
        for latest_file in os.listdir(temp_path):
            latest_file_path = os.path.join(temp_path, latest_file)
            stat = os.stat(latest_file_path)
            if stat.st_size > 0:
                right_path = os.path.join(self.root_path, file_name)
                if not os.path.exists(right_path):
                    shutil.move(latest_file_path, right_path)
            break
        sleep(1)
        create_dir(temp_path, new=True)
        # move the file from temp to the right folder

    def parse_paper_info(self, paper, journal_title, issn, published_year, subject):
        if not paper.find_elements(By.CSS_SELECTOR, 'img[src$="open-access.png"]'):
            return

        if not paper.find_elements(By.CSS_SELECTOR, 'a.j-pdf'):
            return

        pdf_url = ''
        article_url = paper.find_element(By.CSS_SELECTOR, 'div.j-title-1 a').get_attribute('href')
        title = paper.find_element(By.CSS_SELECTOR, 'div.j-title-1 a').text.strip()
        authors = ""
        if paper.find_elements(By.CSS_SELECTOR, 'div.j-author'):
            authors = paper.find_element(By.CSS_SELECTOR, 'div.j-author').text.replace('\n', '').strip()
    
        abstract = ''
        published_at = published_year
        file_name = sanitize_filename(title)[:255] + '.pdf'
        dir = journal_title + '(' + issn +')'
        create_dir(dir)
        right_path = os.path.join(self.root_path, file_name)
        if not os.path.exists(right_path):
            self.download_and_move(paper, 'a.j-pdf', file_name)
            sleep(1)
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


    def parse_archive(self, base_url, journal_title, eissn, subject):
        start_url = base_url
        print(f"[sci] [{csv_name}] {start_url}")
        res = bs(fetch_url(start_url).text, 'lxml')
        volumes = res.select("div.item-content  div.gk_qi")
        dir = journal_title + '(' + eissn +')'
        self.root_path = os.path.join(BASE_PATH, dir)
        driver = set_up_selenium_webdriver(root_path=temp_path)
        for volume in volumes:
            year = self.filter_year(volume)
            if not year:
                continue
            volume_url = volume.a['href']
            if not volume_url.startswith('http'):
                volume_url = domain + volume_url
            driver.get(volume_url)
            papers = driver.find_elements(By.CSS_SELECTOR, "ul.article-list li")
            print(f'[{year}] {volume_url} {len(papers)} papers')
            for paper in papers:
                self.parse_paper_info(paper, journal_title, eissn, year, subject)
                
    def start(self):
        for x in range(len(links)):
            self.parse_archive(links[x], journal_titles[x], eissns[x], subjects[x])

if __name__ == '__main__':
    scraper = Scraper()
    scraper.start()

    export_csv(csv_name, csv_data)

    # read_csv_data(csv_name, csv_data)

    # download_pdfs_via_thread(csv_data)