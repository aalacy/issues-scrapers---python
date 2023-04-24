import requests
from sgrequests import SgRequests
from bs4 import BeautifulSoup as bs
import pdb
import os
from pathvalidate import sanitize_filename
import math
from concurrent.futures import ThreadPoolExecutor
import csv
import re

max_workers = 1

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

journal_titles = [
    "ACTA NEUROPSYCHIATRICA",
    "ACTA NUMERICA",
    "AERONAUTICAL JOURNAL",
    "AI EDAM-ARTIFICIAL INTELLIGENCE FOR ENGINEERING DESIGN ANALYSIS AND MANUFACTURING",
    "ANIMAL HEALTH RESEARCH REVIEWS",
    "ANNALS OF GLACIOLOGY",
    "ANTARCTIC SCIENCE",
    "ANZIAM JOURNAL",
    "ASTIN BULLETIN",
    "BEHAVIORAL AND BRAIN SCIENCES",
    "BIRD CONSERVATION INTERNATIONAL",
    "BJPSYCH OPEN",
    "BRAIN IMPAIRMENT",
    "BRITISH JOURNAL OF NUTRITION",
    "BRITISH JOURNAL OF PSYCHIATRY",
    "BULLETIN OF ENTOMOLOGICAL RESEARCH",
    "BULLETIN OF SYMBOLIC LOGIC",
    "BULLETIN OF THE AUSTRALIAN MATHEMATICAL SOCIETY",
    "CAMBRIDGE QUARTERLY OF HEALTHCARE ETHICS",
    "CANADIAN ENTOMOLOGIST",
    "CANADIAN JOURNAL OF MATHEMATICS-JOURNAL CANADIEN DE MATHEMATIQUES",
    "CANADIAN JOURNAL OF NEUROLOGICAL SCIENCES",
    "CANADIAN MATHEMATICAL BULLETIN-BULLETIN CANADIEN DE MATHEMATIQUES",
    "CARDIOLOGY IN THE YOUNG",
    "CNS SPECTRUMS",
    "COMBINATORICS PROBABILITY & COMPUTING",
    "COMPOSITIO MATHEMATICA",
    "DISASTER MEDICINE AND PUBLIC HEALTH PREPAREDNESS",
    "EARTH AND ENVIRONMENTAL SCIENCE TRANSACTIONS OF THE ROYAL SOCIETY OF EDINBURGH",
    "ECONOMETRIC THEORY",
    "ENVIRONMENTAL CONSERVATION",
    "EPIDEMIOLOGY AND INFECTION",
    "EPIDEMIOLOGY AND PSYCHIATRIC SCIENCES",
    "ERGODIC THEORY AND DYNAMICAL SYSTEMS",
    "EUROPEAN JOURNAL OF APPLIED MATHEMATICS",
    "EUROPEAN PSYCHIATRY",
    "EXPERIMENTAL AGRICULTURE",
    "EXPERT REVIEWS IN MOLECULAR MEDICINE",
    "FORUM OF MATHEMATICS PI",
    "FORUM OF MATHEMATICS SIGMA",
    "GEOLOGICAL MAGAZINE",
    "GLASGOW MATHEMATICAL JOURNAL",
    "GLOBAL MENTAL HEALTH",
    "INFECTION CONTROL AND HOSPITAL EPIDEMIOLOGY",
    "INTERNATIONAL JOURNAL OF ASTROBIOLOGY",
    "INTERNATIONAL JOURNAL OF MICROWAVE AND WIRELESS TECHNOLOGIES",
    "INTERNATIONAL JOURNAL OF TECHNOLOGY ASSESSMENT IN HEALTH CARE",
    "INTERNATIONAL PSYCHOGERIATRICS",
    "INVASIVE PLANT SCIENCE AND MANAGEMENT",
    "JOURNAL OF APPLIED PROBABILITY",
    "JOURNAL OF DEVELOPMENTAL ORIGINS OF HEALTH AND DISEASE",
    "JOURNAL OF FLUID MECHANICS",
    "JOURNAL OF FUNCTIONAL PROGRAMMING",
    "JOURNAL OF GLACIOLOGY",
    "JOURNAL OF HELMINTHOLOGY",
    "JOURNAL OF LARYNGOLOGY AND OTOLOGY",
    "JOURNAL OF LAW MEDICINE & ETHICS",
    "JOURNAL OF NAVIGATION",
    "JOURNAL OF PALEONTOLOGY",
    "JOURNAL OF PLASMA PHYSICS",
    "JOURNAL OF SYMBOLIC LOGIC",
    "JOURNAL OF THE AUSTRALIAN MATHEMATICAL SOCIETY",
    "JOURNAL OF THE INSTITUTE OF MATHEMATICS OF JUSSIEU",
    "JOURNAL OF THE INTERNATIONAL NEUROPSYCHOLOGICAL SOCIETY",
    "JOURNAL OF THE MARINE BIOLOGICAL ASSOCIATION OF THE UNITED KINGDOM",
    "JOURNAL OF TROPICAL ECOLOGY",
    "KNOWLEDGE ENGINEERING REVIEW",
    "LICHENOLOGIST",
    "MATHEMATICAL PROCEEDINGS OF THE CAMBRIDGE PHILOSOPHICAL SOCIETY",
    "MATHEMATICAL STRUCTURES IN COMPUTER SCIENCE",
    "MEDICAL HISTORY",
    "MICROSCOPY AND MICROANALYSIS",
    "NAGOYA MATHEMATICAL JOURNAL",
    "NATURAL LANGUAGE ENGINEERING",
    "NETHERLANDS JOURNAL OF GEOSCIENCES-GEOLOGIE EN MIJNBOUW",
    "NUTRITION RESEARCH REVIEWS",
    "ORYX",
    "PALEOBIOLOGY",
    "PARASITOLOGY",
    "PLANT GENETIC RESOURCES-CHARACTERIZATION AND UTILIZATION",
    "POLAR RECORD",
    "POWDER DIFFRACTION",
    "PREHOSPITAL AND DISASTER MEDICINE",
    "PRIMARY HEALTH CARE RESEARCH AND DEVELOPMENT",
    "PROBABILITY IN THE ENGINEERING AND INFORMATIONAL SCIENCES",
    "PROCEEDINGS OF THE EDINBURGH MATHEMATICAL SOCIETY",
    "PROCEEDINGS OF THE NUTRITION SOCIETY",
    "PROCEEDINGS OF THE ROYAL SOCIETY OF EDINBURGH SECTION A-MATHEMATICS",
    "PSYCHOLOGICAL MEDICINE",
    "PUBLICATIONS OF THE ASTRONOMICAL SOCIETY OF AUSTRALIA",
    "PUBLIC HEALTH NUTRITION",
    "QUARTERLY REVIEWS OF BIOPHYSICS",
    "QUATERNARY RESEARCH",
    "REVIEW OF SYMBOLIC LOGIC",
    "ROBOTICA",
    "SCIENCE IN CONTEXT",
    "SEED SCIENCE RESEARCH",
    "SPANISH JOURNAL OF PSYCHOLOGY",
    "THEORY AND PRACTICE OF LOGIC PROGRAMMING",
    "TWIN RESEARCH AND HUMAN GENETICS",
    "VISUAL NEUROSCIENCE",
    "WEED SCIENCE",
    "WEED TECHNOLOGY",
    "ZYGOTE",
    "PHILOSOPHY OF SCIENCE",
    "HIGH POWER LASER SCIENCE AND ENGINEERING",
    "CLAY MINERALS"
]

eissns = [
    "1601-5215",
    "1474-0508",
    "2059-6464",
    "1469-1760",
    "1475-2654",
    "1727-5644",
    "1365-2079",
    "1446-8735",
    "1783-1350",
    "1469-1825",
    "1474-0001",
    "2056-4724",
    "1839-5252",
    "1475-2662",
    "1472-1465",
    "1475-2670",
    "1943-5894",
    "1755-1633",
    "1469-2147",
    "1918-3240",
    "1496-4279",
    "2057-0155",
    "1496-4287",
    "1467-1107",
    "2165-6509",
    "1469-2163",
    "1570-5846",
    "1938-744X",
    "1755-6929",
    "1469-4360",
    "1469-4387",
    "1469-4409",
    "2045-7979",
    "1469-4417",
    "1469-4425",
    "1778-3585",
    "1469-4441",
    "1462-3994",
    "2050-5086",
    "2050-5094",
    "1469-5081",
    "1469-509X",
    "2054-4251",
    "1559-6834",
    "1475-3006",
    "1759-0795",
    "1471-6348",
    "1741-203X",
    "1939-747X",
    "1475-6072",
    "2040-1752",
    "1469-7645",
    "1469-7653",
    "1727-5652",
    "1475-2697",
    "1748-5460",
    "1748-720X",
    "1469-7785",
    "1937-2337",
    "1469-7807",
    "1943-5886",
    "1446-8107",
    "1475-3030",
    "1469-7661",
    "1469-7769",
    "1469-7831",
    "1469-8005",
    "1096-1135",
    "1469-8064",
    "1469-8072",
    "2048-8343",
    "1435-8115",
    "2152-6842",
    "1469-8110",
    "1573-9708",
    "1475-2700",
    "1365-3008",
    "1938-5331",
    "1469-8161",
    "1479-263X",
    "1475-3057",
    "1945-7413",
    "1945-1938",
    "1477-1128",
    "1469-8951",
    "1464-3839",
    "1475-2719",
    "1473-7124",
    "1469-8978",
    "1448-6083",
    "1475-2727",
    "1469-8994",
    "1096-0287",
    "1755-0211",
    "1469-8668",
    "1474-0664",
    "1475-2735",
    "1988-2904",
    "1475-3081",
    "1839-2628",
    "1469-8714",
    "1550-2759",
    "1550-2740",
    "1469-8730",
    "1539-767X",
    "2052-3289",
    "1471-8030"
]

links = [
    "http://journals.cambridge.org/action/displayJournal?jid=neu",
    "http://journals.cambridge.org/action/displayJournal?jid=ANU",
    "https://www.cambridge.org/core/journals/aeronautical-journal/all-issues",
    "http://journals.cambridge.org/action/displayJournal?jid=AIE",
    "http://journals.cambridge.org/action/displayJournal?jid=AHR",
    "http://journals.cambridge.org/action/displayJournal?jid=AOG",
    "http://journals.cambridge.org/action/displayJournal?jid=ANS",
    "http://journals.cambridge.org/action/displayJournal?jid=ANZ",
    "http://journals.cambridge.org/action/displayJournal?jid=ASB",
    "http://journals.cambridge.org/action/displayJournal?jid=BBS",
    "http://journals.cambridge.org/action/displayJournal?jid=BCI",
    "https://www.cambridge.org/core/journals/bjpsych-open",
    "http://journals.cambridge.org/action/displayJournal?jid=BIM",
    "http://journals.cambridge.org/action/displayJournal?jid=BJN",
    "http://bjp.rcpsych.org/",
    "http://journals.cambridge.org/action/displayJournal?jid=BER",
    "http://journals.cambridge.org/action/displayJournal?jid=BSL",
    "http://journals.cambridge.org/action/displayJournal?jid=BAZ",
    "http://journals.cambridge.org/action/displayJournal?jid=CQH",
    "http://journals.cambridge.org/action/displayjournal?jid=TCE",
    "https://www.cambridge.org/core/journals/canadian-journal-of-mathematics",
    "http://journals.cambridge.org/action/displayJournal?jid=CJN",
    "https://www.cambridge.org/core/journals/canadian-mathematical-bulletin",
    "http://journals.cambridge.org/action/displayJournal?jid=CTY",
    "http://journals.cambridge.org/action/displayJournal?jid=CNS",
    "http://journals.cambridge.org/action/displayJournal?jid=CPC",
    "http://journals.cambridge.org/action/displayJournal?jid=COM",
    "http://journals.cambridge.org/action/displayJournal?jid=DMP",
    "http://journals.cambridge.org/action/displayJournal?jid=TRE",
    "http://journals.cambridge.org/action/displayJournal?jid=ECT",
    "http://journals.cambridge.org/action/displayJournal?jid=ENC",
    "http://journals.cambridge.org/action/displayJournal?jid=HYG",
    "http://journals.cambridge.org/action/displayJournal?jid=EPS",
    "http://journals.cambridge.org/action/displayJournal?jid=ETS",
    "http://journals.cambridge.org/action/displayJournal?jid=EJM",
    "https://www.cambridge.org/core/journals/european-psychiatry",
    "https://www.cambridge.org/core/journals/experimental-agriculture",
    "http://journals.cambridge.org/action/displayJournal?jid=ERM",
    "http://journals.cambridge.org/action/displayJournal?jid=FMP",
    "http://journals.cambridge.org/action/displayJournal?jid=FMS",
    "https://www.cambridge.org/core/journals/experimental-agriculture/all-issues",
    "http://journals.cambridge.org/action/displayJournal?jid=GMJ",
    "http://journals.cambridge.org/action/displayJournal?jid=GMH",
    "https://www.cambridge.org/core/journals/infection-control-and-hospital-epidemiology",
    "http://journals.cambridge.org/action/displayJournal?jid=IJA",
    "http://journals.cambridge.org/MRF",
    "http://journals.cambridge.org/action/displayJournal?jid=THC",
    "https://www.cambridge.org/core/journals/international-psychogeriatrics",
    "https://www.cambridge.org/core/journals/invasive-plant-science-and-management",
    "http://journals.cambridge.org/action/displayJournal?jid=JPR",
    "http://journals.cambridge.org/action/displayJournal?jid=DOH",
    "http://journals.cambridge.org/action/displayJournal?jid=FLM",
    "http://journals.cambridge.org/action/displayJournal?jid=JFP",
    "http://journals.cambridge.org/action/displayJournal?jid=JOG",
    "http://journals.cambridge.org/action/displayJournal?jid=JHL",
    "http://journals.cambridge.org/action/displayJournal?jid=JLO",
    "https://www.cambridge.org/core/journals/journal-of-law-medicine-and-ethics",
    "http://journals.cambridge.org/action/displayJournal?jid=NAV",
    "http://journals.cambridge.org/JPA",
    "http://journals.cambridge.org/action/displayJournal?jid=PLA",
    "http://journals.cambridge.org/action/displayJournal?jid=JSL",
    "http://journals.cambridge.org/action/displayJournal?jid=JAZ",
    "http://journals.cambridge.org/action/displayJournal?jid=JMJ",
    "http://journals.cambridge.org/action/displayJournal?jid=INS",
    "http://journals.cambridge.org/action/displayJournal?jid=MBI",
    "http://journals.cambridge.org/action/displayJournal?jid=TRO",
    "http://journals.cambridge.org/action/displayJournal?jid=KER",
    "http://journals.cambridge.org/action/displayJournal?jid=LIC",
    "http://journals.cambridge.org/action/displayJournal?jid=PSP",
    "http://journals.cambridge.org/action/displayJournal?jid=MSC",
    "http://journals.cambridge.org/action/displayJournal?jid=MDH",
    "http://journals.cambridge.org/action/displayJournal?jid=MAM",
    "http://www.dukeupress.edu/Catalog/ViewProduct.php/viewby_journal/productid_45639",
    "http://journals.cambridge.org/action/displayJournal?jid=NLE",
    "http://journals.cambridge.org/action/displayJournal?jid=NJG",
    "http://journals.cambridge.org/action/displayJournal?jid=NRR",
    "http://journals.cambridge.org/action/displayJournal?jid=ORX",
    "http://journals.cambridge.org/action/displayJournal?jid=PAB",
    "http://journals.cambridge.org/action/displayJournal?jid=PAR",
    "http://journals.cambridge.org/action/displayJournal?jid=PGR",
    "http://journals.cambridge.org/action/displayJournal?jid=POL",
    "http://journals.cambridge.org/action/displayJournal?jid=PDJ",
    "http://journals.cambridge.org/action/displayJournal?jid=PDM",
    "http://journals.cambridge.org/action/displayJournal?jid=PHC",
    "http://journals.cambridge.org/action/displayJournal?jid=PES",
    "http://journals.cambridge.org/action/displayJournal?jid=PEM",
    "http://journals.cambridge.org/action/displayJournal?jid=PNS",
    "http://journals.cambridge.org/action/displayJournal?jid=prm",
    "http://journals.cambridge.org/action/displayJournal?jid=PSM",
    "http://journals.cambridge.org/action/displayJournal?jid=PAS",
    "http://journals.cambridge.org/action/displayJournal?jid=PHN",
    "http://journals.cambridge.org/action/displayJournal?jid=QRB",
    "https://www.cambridge.org/core/journals/quaternary-research/all-issues",
    "http://journals.cambridge.org/action/displayJournal?jid=RSL",
    "http://journals.cambridge.org/action/displayJournal?jid=ROB",
    "http://journals.cambridge.org/action/displayJournal?jid=SIC",
    "http://journals.cambridge.org/action/displayJournal?jid=SSR",
    "http://journals.cambridge.org/action/displayJournal?jid=SJP",
    "http://journals.cambridge.org/action/displayJournal?jid=TLP",
    "http://journals.cambridge.org/THG",
    "http://journals.cambridge.org/action/displayJournal?jid=VNS",
    "https://www.cambridge.org/core/journals/weed-science",
    "https://www.cambridge.org/core/journals/weed-technology",
    "http://journals.cambridge.org/action/displayJournal?jid=ZYG",
    "https://www.cambridge.org/core/journals/philosophy-of-science",
    "http://journals.cambridge.org/action/displayJournal?jid=HPL",
    "https://www.cambridge.org/core/journals/clay-minerals"
]

domain = "https://www.cambridge.org"
BASE_PATH = "/media/com/C69A54D99A54C817/pwj/"
proxy_url = "http://api.scraperapi.com?api_key=3664a7c3***ecd461ad4c8bf0&url="

csv_header = ['Title', 'Source', 'Subject', 'Sub Category', 'Type', 'Authors', 'Published At', 'Published Year', 'Abstract', 'File Path', 'PDF URL', 'Article URL', 'Created At', 'Updated At']
csv_data = []
pdfs = []
file_paths = []
failed_files = []
csv_name = "cambridge"

def fetch_url(url, proxy=True):
    with requests.Session() as s:
        try:
            _url = url
            if proxy:
                _url = f"{proxy_url}{url}"
            return requests.get(_url, headers=_headers)
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
    if 'all-issues' not in base_url:
        start_url = fetch_url(base_url, False).url.__str__() + '/all-issues'

    print(f"[sci] {start_url}")
    volumes = bs(fetch_url(start_url.split('&url=')[-1]).text, 'lxml').select('div.journal-all-issues div.content ul.accordion.level.second > li.accordion-navigation')

    failed_urls = []
    for volume in volumes:
        year = filter_out_2019(volume)
        if year > 2018:
            issues = volume.select('div a')
            for issue in issues:
                if 'panel' in issue['href']:
                    continue
                page_url = domain + issue['href']
                res = fetch_url(page_url)
                if res.status_code == 404:
                    failed_urls.append(page_url)
                    continue
                page_source = bs(res.text, 'lxml')
                paginations = page_source.select('div.pagination-centered ul.pagination')
                papers = []
                if paginations:
                    for page_num in paginations[0].select('li a'):
                        _page = page_num.text.strip()
                        if not _page.isdigit():
                            continue
                        if _page == '1':
                            papers = get_page_data(page_source)
                            continue
                        _page_url = page_url + page_num['href']
                        papers += get_page_data(bs(fetch_url(_page_url).text, 'lxml'))
                else:
                    papers = get_page_data(page_source)

                print(f"[{year}] [{page_url}] {len(papers)}")
                for paper in papers:
                    parse_paper_info(paper, journal_title, issn, page_url)

    # write log file
    with open('./logs/cambridge.txt', 'w') as f:
        f.writelines(failed_urls)
                    

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
    file_path = os.path.join(BASE_PATH, file_path)
    if os.path.exists(file_path):
        return
    print('--- downloading --- ', file_path, pdf_link)
    try:
        with SgRequests() as s:
            new_res = s.get(f"{pdf_link}", headers=_headers)
            with open(file_path, "wb") as download_file:
                with s.stream(new_res.url) as response:
                    for chunk in response.iter_bytes():
                        download_file.write(chunk)

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

def parse_paper_info(paper, journal_title, issn, article_url):
    if not paper.select_one('li.open-access'):
        return

    if not paper.find('a', href=re.compile(r"pdf$")):
        print(article_url)
        return

    title = paper.select_one('li.title').text.strip()
    pdf_url = domain + paper.find('a', href=re.compile(r"pdf$"))['href']
    authors = paper.select_one('li.author').text.strip()
    _abstract = paper.select_one('div.abstract')
    abstract = ''
    if _abstract:
        abstract = _abstract.text.strip().replace('\n\n', '')
    published_at = paper.select_one('li.published span.date').text.strip()
    published_year = published_at.split()[-1].strip()
    file_name = sanitize_filename(title) + '.pdf'
    dir = journal_title + '(' + issn +')'
    create_dir(dir)
    file_path = os.path.join(dir, file_name)
    csv_data.append({
        'Title': title, 
        'Source': 'CAMBRIDGE UNIV PRESS', 
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