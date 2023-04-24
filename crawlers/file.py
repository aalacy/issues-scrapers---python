import os
from urllib.parse import urlparse
import tldextract
from PyPDF2 import PdfReader
import math
from concurrent.futures import ThreadPoolExecutor

BASE_PATH = "/home/com/SData/"
max_workers = 30
LOG_FILE = "validate_file.txt"

links = [
    "https://projecteuclid.org",
    "https://www.imrpress.com",
    "http://imstat.org (same as https://projecteuclid.org)",
    "http://bjp.rcpsych.org/",
    "http://www.dukeupress.edu",
    "https://comptes-rendus.academie-sciences.fr",
    "http://www.journal.csj.jp",
    "https://www.osapublishing.org/prj/browse.cfm",
    "http://www.bjoms.com",
    "http://www.cshperspectives.org/",
    "http://perspectivesinmedicine.cshlp.org/",
    "http://genesdev.cshlp.org/",
    "https://journal.copdfoundation.org/",
    "http://colombiamedica.univalle.edu.co",
    "https://hrcak.srce.hr",
    "https://www.mdedge.com",
    "https://dergipark.org.tr",
    "https://pubs.geoscienceworld.org",
    "http://www.geochemicalperspectives.org",
    "http://err.ersjournals.com",
    "https://sciendo.com",
    "https://riviste.fupress.net",
    "http://folia.paru.cas.cz",
    "http://www.futuremedicine.com",
    "https://dergipark.org.tr",
    "https://pubs.geoscienceworld.org",
    "https://lyellcollection.org",
    "http://neurology.org",
    "http://op.niscair.res.in",
    "http://periodicos.uem.br",
    "http://revistas.univalle.edu.co",
    "http://sciencepress.mnhn.fr",
    "http://www.app.pan.pl",
    "http://www.biosciencetrends.com/",
    "http://www.bjbms.org/",
    "http://www.bjoms.com",
    "http://www.bloodjournal.org",
    "http://www.bloodtransfusion.it/",
    "http://www.cshperspectives.org/",
    "http://www.doiserbia.nb.rs",
    "http://www.dukeupress.edu",
    "http://www.geochemicalperspectives.org",
    "http://www.geochemicalperspectivesletters.org/",
    "http://www.global-sci.com",
    "http://www.healio.com",
    "http://www.ifo.lviv.ua",
    "http://www.ijo.cn",
    "http://www.int-res.com",
    "http://www.journal.csj.jp",
    "http://www.mitpressjournals.org",
    "http://www.mjhid.org/",
    "http://www.osapublishing.org",
    "http://www.plosgenetics.org",
    "http://www.revistas.unal.edu.co",
    "http://www.sgecm.org.tw/ijge/",
    "http://www.slackjournals.com",
    "http://journals.pan.pl/opelre/",
    "http://journals.plos.org/plosbiology/",
    "http://www.spandidos-publications.com",
    "http://www.techscience.com",
    "http://www.termedia.pl",
    "http://www.unal.edu.co",
    "http://www.vef.hr",
    "https://ascopubs.org",
    "https://comptes-rendus.academie-sciences.fr",
    "https://econtent.hogrefe.com",
    "https://genome.cshlp.org/",
    "https://hrcak.srce.hr/acta-adriatica",
    "https://dergipark.org.tr",
    "https://hrcak.srce.hr",
    "https://imstat.org",
    "https://journal.copdfoundation.org/",
    "https://journals.prous.com",
    "https://journals.tubitak.gov.tr",
    "https://lyellcollection.org",
    "https://muse.jhu.edu",
    "https://openres.ersjournals.com/",
    "https://portlandpress.com/biochemj",
    "https://revistas.unal.edu.co",
    "https://www.ejgo.net/",
    "https://www.jomh.org/",
    "https://www.mattioli1885journals.com",
    "https://www.mdedge.com",
    "https://www.osapublishing.org",
    "https://www.tappi.org",
    "https://www.termedia.pl",
    "https://www.tsoc.org.tw",
    "https://www.vetline.de",
    "https://www.wachholtz-verlag.de",
    "https://www.wjmh.org/",
    "https://www.xiahepublishing.com",
]

def clean_zero_files():
    for folder, subfolders, files in os.walk(BASE_PATH):
        for subfolder in subfolders:
            for subfile in os.listdir(os.path.join(BASE_PATH, subfolder)):
                abs_path = os.path.join(BASE_PATH, subfolder, subfile)
                if os.path.isdir(abs_path):
                    continue
                if subfile.endswith(".pdf"):
                    stat = os.stat(abs_path)
                    if stat.st_size == 0 or stat.st_size < 100:
                        print(f'[rm] {abs_path}')
                        os.remove(abs_path)

def extract_domain():
    domains = []
    for link in links:
        domain = tldextract.extract(link)
        domains.append(f"{domain.domain}.{domain.suffix}")
    
    domains.sort()
    print(domains)

def write_file(data):
    with open(LOG_FILE, 'a+') as f:
        f.write(data + '\n')

def check_file(fullfile, parent_path, should_remove=False):
    full_path = os.path.join(parent_path, fullfile)
    if os.path.isdir(full_path):
        return
    with open(full_path, 'rb') as f:
        try:
            pdf = PdfReader(f)
            info = pdf.metadata
            if info:
                # print("OK " + full_path + "\n################")
                return True
            else:
                print("Error Info" + full_path + "\n################")
                if should_remove:
                    os.remove(full_path)
                write_file(full_path)
                return False
        except:
            print("Error Read" + full_path + "\n################")
            write_file(full_path)
            if should_remove:
                os.remove(full_path)
            return False

def validate_files(list, parent_path, occurrence=max_workers):
    output = []
    total = len(list)
    reminder = math.floor(total / 50)
    if reminder < occurrence:
        reminder = occurrence

    count = 0
    with ThreadPoolExecutor(
        max_workers=occurrence, thread_name_prefix="fetcher"
    ) as executor:
        for result in executor.map(check_file, list, [parent_path] * len(list)):
            if result:
                count = count + 1
                if count % reminder == 0:
                    print("Concurrent Operation count = ", count)
                output.append(result)
    return output

def search_files(dirpath):
    pwdpath = os.path.dirname(os.path.realpath(__file__))
    print("running path : %s" %pwdpath )
    if os.access(dirpath, os.R_OK):
        print("Path %s validation OK \n" %dirpath)
        for folder, subfolders, files in os.walk(dirpath):
            # if files:
            #     validate_files(files, BASE_PATH)
            for subfolder in subfolders:
                abs_path = os.path.join(BASE_PATH, subfolder)
                if os.path.isdir(abs_path):
                    pdf_list = os.listdir(abs_path)
                    if pdf_list:
                        if os.path.isdir(os.path.join(abs_path, pdf_list[0])):
                            for sub_path in pdf_list:
                                abs_sub_path = os.path.join(abs_path, sub_path)
                                pdf_sub_list = os.listdir(abs_sub_path)
                                validate_files(pdf_sub_list, parent_path=abs_sub_path)
                        else:
                            validate_files(pdf_list, parent_path=abs_path)
    else:
        print("Path is not valid")

if __name__ == "__main__":
    # clean_zero_files()

    # extract_domain()

    search_files(BASE_PATH)