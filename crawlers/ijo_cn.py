import os
import math
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
from pathvalidate import sanitize_filename
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver import ActionChains
import pdb
import time

from util import (
    fetch_url, 
    create_dir, 
    export_csv, 
    read_csv_data, 
    download_pdfs_via_thread,
    set_up_selenium_webdriver,
    download_via_selenium
)

max_workers = 10

domain = "http://ifo.lviv.ua"

csv_name = "lviv_ua"
csv_data = []
pdfs = []
file_paths = []
failed_files = []
PUBLISHER_NAME = "IJO PRESS"

journal_titles = [
    "INTERNATIONAL JOURNAL OF OPHTHALMOLOGY"
]

eissns = [
    "2227-4898"
]

links = [
    "http://ies.ijo.cn/gjyken/ch/reader/issue_list.aspx"
]

subjects = [
    "Ophthalmology"
]

def parse_paper_info(paper, journal_title, issn, subject, published_year):
    if not paper.text.strip():
        return
    if 'Abstract' in paper.text:
        return
    if not paper.find_elements(By.CSS_SELECTOR, 'a'):
        return
    title = paper.find_element(By.CSS_SELECTOR, 'a').text.strip()
    article_url = paper.find_element(By.CSS_SELECTOR, 'a').get_attribute('href')
    if 'view_abstract.aspx' not in article_url:
        return
    if not article_url.startswith("http"):
        article_url = "http://ies.ijo.cn/gjyken/ch/reader/" + article_url
    print(f'[paper] {article_url}')
    article = bs(fetch_url(article_url).text, 'lxml')
    if not article.find('a', href=re.compile(r"create_pdf.aspx")):
        return
    pdf_url = article.find('a', href=re.compile(r"create_pdf.aspx"))['href']
    if not pdf_url.startswith('http'):
        pdf_url ="http://ies.ijo.cn/gjyken/ch/reader/" + pdf_url
    authors = ""
    abstract = ''
    if article.find('b', text=re.compile(r"Abstract:")):
        abstract = article.find('b', text=re.compile(r"Abstract:")).find_parent('tr').find_next_sibling().text.strip()
    published_at = article.select_one('span#SendTime').text.split(':')[-1]
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
    
 
def parse_archive(base_url, journal_title, issn, subject):
    start_url = base_url
    print(f"[sci] [{csv_name}] {start_url}")
    driver = set_up_selenium_webdriver()
    driver.get(start_url)
    volumes = Select(driver.find_element(By.CSS_SELECTOR, 'select#year_id')).options
    for x in range(len(volumes)):
        volume = volumes[x]
        year = filter_year(volume)
        if not year:
            break
        volume_selector = Select(driver.find_element(By.ID, 'year_id'))
        time.sleep(1)
        volume_selector.select_by_value(str(year))
        time.sleep(1)
        numbers = Select(driver.find_element(By.ID, 'quarter_id')).options
        for y in range(len(numbers)):
            number = numbers[y]
            number_selector = Select(driver.find_element(By.ID, 'quarter_id'))
            time.sleep(1)
            number_selector.select_by_value(number.get_attribute('value'))
            driver.find_element(By.CSS_SELECTOR, 'input[value="Submit"]').click()
            time.sleep(2)
            papers = driver.find_elements(By.CSS_SELECTOR, "div.right table table tbody tr")
            fetch_all(papers, journal_title, issn, subject, year)
            numbers = Select(driver.find_element(By.ID, 'quarter_id')).options
            time.sleep(1)

        volumes = Select(driver.find_element(By.ID, 'year_id')).options
        time.sleep(1)


def filter_year(volume):
    year = volume.get_attribute('value').strip()
    if not year:
        print(f"[no year] {volume['href']}")
        return False
    try:
        year = int(year)
        if year < 2019:
            return False
    except Exception as error:
        print(f'[error] [year] {error}')
        if 'Advanced' not in year:
            return False

    return year


def fetch_all(list, journal_title, eissn, subject, published_year, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(parse_paper_info, list, [journal_title] * len(list), [eissn] * len(list), [subject] * len(list), [published_year] * len(list)):
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

    download_pdfs_via_thread(csv_data)

    # download_via_selenium(csv_data)