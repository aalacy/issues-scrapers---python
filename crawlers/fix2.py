import os
import csv
import httpx
import requests
import shutil
from time import sleep
from PyPDF2 import PdfReader

BASE_PATH = '/media/com/ubuntu_work/work/study/scrapers'
VALIDATE_FILE = '/home/com/Downloads/validate_file_sci-ssr-2.txt'
OUTPUT_CSV_FILE_PATH = 'output'
TARGET_PATH = '/media/com/Expansion/10F'

error_files = []

proxies = {
    "http://": "http://37.48.118.4:13081",
    "https://": "http://5.79.66.2:13081"
}


def _download_requests(file_path, pdf_link, stream=True):
    with requests.get(pdf_link, stream=stream, proxies=proxies, allow_redirects=True) as r:
        with open(file_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

def _download_httpx(file_path, pdf_link):
    with httpx.Client(proxies=proxies) as s:
        with open(file_path, "wb") as download_file:
            with s.stream("GET", pdf_link, follow_redirects=True) as response:
                for chunk in response.iter_bytes():
                    download_file.write(chunk)

def remove_file(file_path):
    if os.path.exists(file_path):
        stat = os.stat(file_path)
        if stat.st_size == 0 or stat.st_size < 200:
            print(f'[rm] {file_path}')
            os.remove(file_path)
            return True
    else:
        return True
    
    return False
            
def download(pdf_link, file_path, base_path=BASE_PATH):
    file_path = os.path.join(base_path, file_path)
    if os.path.exists(file_path):
        return remove_file(file_path)
    print('--- downloading --- ', file_path, pdf_link)
    try:
        # if _validate_pdf_link(pdf_link):
        _download_requests(file_path, pdf_link)
        # _download_httpx(file_path, pdf_link)
    except Exception as err:
        remove_file(file_path)
        print('[Download error]: ', err)

def del_files_from_validate():
    with open(VALIDATE_FILE, 'r') as f:
        for line in f.readlines():
            if '[del]' in line:
                continue
            if 'bmc-' in line:
                continue
            if not line.strip():
                continue
            line = line.strip()
            if not remove_file(line):
                with open(line, 'rb') as f1:
                    try:
                        pdf = PdfReader(f1)
                    except Exception as err:
                        print(f'[pdf] [error] {line} ', err)
                        os.remove(line)

def read_validate_file():
    with open(VALIDATE_FILE, 'r') as f:
        for line in f.readlines():
            if '[del]' in line:
                continue
            if 'bmc-' in line:
                continue
            if not line.strip():
                continue
            file_name = line.split('/')[-1]
            error_files.append(file_name.strip().replace('.pdf', ''))

def pickup_error_file(line):
    for file in error_files:
        if file['file_name'].strip().replace('.pdf', '') in line['File Path']:
            return file
        
    return False

def read_csv_files():
    output_path = os.path.join(BASE_PATH, OUTPUT_CSV_FILE_PATH)
    for csv_file in os.listdir(output_path):
        if 'thejns_cases' not in csv_file:
            continue
        with open(f'{BASE_PATH}/output/{csv_file}', 'r') as data:
            print(f"[csv] {csv_file}")
            for line in csv.DictReader(data):
                file_path = line['File Path'].split('/')[-1].strip().replace('.pdf', '')
                cnt = min(len(file_path), 20)
                if file_path[:255] not in error_files:
                    print(f'[skip] ', file_path)
                    continue
                
                pdf_link = line['PDF URL'].replace('/view/', '/download/').replace('https://www.jstage.jst.go.jphttps', 'https')
                file_path = line['File Path'].replace(':', '_').replace(".pdf", "")[:255] + ".pdf"
                download(pdf_link, file_path, TARGET_PATH)
                sleep(2)

def run():
    read_validate_file()

    read_csv_files()

if __name__ == "__main__":
    # run()
    del_files_from_validate()