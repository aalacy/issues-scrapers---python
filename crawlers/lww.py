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

_headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/12.0 Mobile/15A372 Safari/604.1",
}

journal_titles = [
    "ADVANCES IN NURSING SCIENCE",
    "ADVANCES IN SKIN & WOUND CARE",
    "AIDS",
    "ALZHEIMER DISEASE & ASSOCIATED DISORDERS",
    "AMERICAN JOURNAL OF CLINICAL ONCOLOGY-CANCER CLINICAL TRIALS",
    "AMERICAN JOURNAL OF DERMATOPATHOLOGY",
    "AMERICAN JOURNAL OF FORENSIC MEDICINE AND PATHOLOGY",
    "AMERICAN JOURNAL OF GASTROENTEROLOGY",
    "AMERICAN JOURNAL OF MEDICAL QUALITY",
    "AMERICAN JOURNAL OF NURSING",
    "AMERICAN JOURNAL OF PHYSICAL MEDICINE & REHABILITATION",
    "AMERICAN JOURNAL OF SURGICAL PATHOLOGY",
    "AMERICAN JOURNAL OF THERAPEUTICS",
    "ANESTHESIOLOGY",
    "ANNALS OF PLASTIC SURGERY",
    "ANNALS OF SURGERY",
    "ANTI-CANCER DRUGS",
    "APPLIED IMMUNOHISTOCHEMISTRY & MOLECULAR MORPHOLOGY",
    "ASAIO JOURNAL",
    "BEHAVIOURAL PHARMACOLOGY",
    "BLOOD COAGULATION & FIBRINOLYSIS",
    "BLOOD PRESSURE MONITORING",
    "CANCER NURSING",
    "CARDIOLOGY IN REVIEW",
    "CHINESE MEDICAL JOURNAL",
    "CIN-COMPUTERS INFORMATICS NURSING",
    "CLINICAL AND TRANSLATIONAL GASTROENTEROLOGY",
    "CLINICAL DYSMORPHOLOGY",
    "CLINICAL JOURNAL OF PAIN",
    "CLINICAL JOURNAL OF SPORT MEDICINE",
    "CLINICAL NEUROPHARMACOLOGY",
    "CLINICAL NUCLEAR MEDICINE",
    "CLINICAL NURSE SPECIALIST",
    "CLINICAL OBSTETRICS AND GYNECOLOGY",
    "CLINICAL ORTHOPAEDICS AND RELATED RESEARCH",
    "CLINICAL SPINE SURGERY",
    "COGNITIVE AND BEHAVIORAL NEUROLOGY",
    "CORNEA",
    "CORONARY ARTERY DISEASE",
    "CRITICAL CARE MEDICINE",
    "CURRENT OPINION IN ALLERGY AND CLINICAL IMMUNOLOGY",
    "CURRENT OPINION IN ANESTHESIOLOGY",
    "CURRENT OPINION IN CARDIOLOGY",
    "CURRENT OPINION IN CLINICAL NUTRITION AND METABOLIC CARE",
    "CURRENT OPINION IN CRITICAL CARE",
    "CURRENT OPINION IN ENDOCRINOLOGY DIABETES AND OBESITY",
    "CURRENT OPINION IN GASTROENTEROLOGY",
    "CURRENT OPINION IN HEMATOLOGY",
    "CURRENT OPINION IN HIV AND AIDS",
    "CURRENT OPINION IN INFECTIOUS DISEASES",
    "CURRENT OPINION IN LIPIDOLOGY",
    "CURRENT OPINION IN NEPHROLOGY AND HYPERTENSION",
    "CURRENT OPINION IN NEUROLOGY",
    "CURRENT OPINION IN OBSTETRICS & GYNECOLOGY",
    "CURRENT OPINION IN ONCOLOGY",
    "CURRENT OPINION IN OPHTHALMOLOGY",
    "CURRENT OPINION IN ORGAN TRANSPLANTATION",
    "CURRENT OPINION IN OTOLARYNGOLOGY & HEAD AND NECK SURGERY",
    "CURRENT OPINION IN PEDIATRICS",
    "CURRENT OPINION IN PSYCHIATRY",
    "CURRENT OPINION IN PULMONARY MEDICINE",
    "CURRENT OPINION IN RHEUMATOLOGY",
    "CURRENT OPINION IN SUPPORTIVE AND PALLIATIVE CARE",
    "CURRENT OPINION IN UROLOGY",
    "CURRENT SPORTS MEDICINE REPORTS",
    "DISEASES OF THE COLON & RECTUM",
    "EAR AND HEARING",
    "EPIDEMIOLOGY",
    "EUROPEAN JOURNAL OF ANAESTHESIOLOGY",
    "EUROPEAN JOURNAL OF CANCER PREVENTION",
    "EUROPEAN JOURNAL OF EMERGENCY MEDICINE",
    "EUROPEAN JOURNAL OF GASTROENTEROLOGY & HEPATOLOGY",
    "EXERCISE AND SPORT SCIENCES REVIEWS",
    "EYE & CONTACT LENS-SCIENCE AND CLINICAL PRACTICE",
    "GASTROENTEROLOGY NURSING",
    "HARVARD REVIEW OF PSYCHIATRY",
    "HEALTH PHYSICS",
    "HEMASPHERE",
    "HOLISTIC NURSING PRACTICE",
    "IMPLANT DENTISTRY",
    "INTERNATIONAL CLINICAL PSYCHOPHARMACOLOGY",
    "INTERNATIONAL JOURNAL OF GYNECOLOGICAL PATHOLOGY",
    "INTERNATIONAL JOURNAL OF REHABILITATION RESEARCH",
    "INVESTIGATIVE RADIOLOGY",
    "JAIDS-JOURNAL OF ACQUIRED IMMUNE DEFICIENCY SYNDROMES",
    "JANAC-JOURNAL OF THE ASSOCIATION OF NURSES IN AIDS CARE",
    "JBI EVIDENCE IMPLEMENTATION",
    "JCR-JOURNAL OF CLINICAL RHEUMATOLOGY",
    "JOURNAL FOR HEALTHCARE QUALITY",
    "JOURNAL OF ADDICTION MEDICINE",
    "JOURNAL OF ADDICTIONS NURSING",
    "JOURNAL OF BONE AND JOINT SURGERY-AMERICAN VOLUME",
    "JOURNAL OF CARDIOPULMONARY REHABILITATION AND PREVENTION",
    "JOURNAL OF CARDIOVASCULAR MEDICINE",
    "JOURNAL OF CARDIOVASCULAR NURSING",
    "JOURNAL OF CARDIOVASCULAR PHARMACOLOGY",
    "JOURNAL OF CATARACT AND REFRACTIVE SURGERY",
    "JOURNAL OF CLINICAL GASTROENTEROLOGY",
    "JOURNAL OF CLINICAL NEUROPHYSIOLOGY",
    "JOURNAL OF CLINICAL PSYCHOPHARMACOLOGY",
    "JOURNAL OF COMPUTER ASSISTED TOMOGRAPHY",
    "JOURNAL OF CONTINUING EDUCATION IN THE HEALTH PROFESSIONS",
    "JOURNAL OF CRANIOFACIAL SURGERY",
    "JOURNAL OF DEVELOPMENTAL AND BEHAVIORAL PEDIATRICS",
    "JOURNAL OF ECT",
    "JOURNAL OF FORENSIC NURSING",
    "JOURNAL OF GERIATRIC PHYSICAL THERAPY",
    "JOURNAL OF GLAUCOMA",
    "JOURNAL OF HEAD TRAUMA REHABILITATION",
    "JOURNAL OF HOSPICE & PALLIATIVE NURSING",
    "JOURNAL OF HYPERTENSION",
    "JOURNAL OF IMMUNOTHERAPY",
    "JOURNAL OF LOWER GENITAL TRACT DISEASE",
    "JOURNAL OF NERVOUS AND MENTAL DISEASE",
    "JOURNAL OF NEUROLOGIC PHYSICAL THERAPY",
    "JOURNAL OF NEURO-OPHTHALMOLOGY",
    "JOURNAL OF NEUROSCIENCE NURSING",
    "JOURNAL OF NEUROSURGICAL ANESTHESIOLOGY",
    "JOURNAL OF NURSING ADMINISTRATION",
    "JOURNAL OF NURSING CARE QUALITY",
    "JOURNAL OF OCCUPATIONAL AND ENVIRONMENTAL MEDICINE",
    "JOURNAL OF ORTHOPAEDIC TRAUMA",
    "JOURNAL OF PEDIATRIC GASTROENTEROLOGY AND NUTRITION",
    "JOURNAL OF PEDIATRIC HEMATOLOGY ONCOLOGY",
    "JOURNAL OF PEDIATRIC ORTHOPAEDICS",
    "JOURNAL OF PEDIATRIC ORTHOPAEDICS-PART B",
    "JOURNAL OF PERINATAL & NEONATAL NURSING",
    "JOURNAL OF PSYCHIATRIC PRACTICE",
    "JOURNAL OF STRENGTH AND CONDITIONING RESEARCH",
    "JOURNAL OF THE AMERICAN ASSOCIATION OF NURSE PRACTITIONERS",
    "JOURNAL OF THE CHINESE MEDICAL ASSOCIATION",
    "JOURNAL OF THORACIC IMAGING",
    "JOURNAL OF TRAUMA NURSING",
    "JOURNAL OF WOUND OSTOMY AND CONTINENCE NURSING",
    "MCN-THE AMERICAN JOURNAL OF MATERNAL-CHILD NURSING",
    "MEDICAL CARE",
    "MEDICINE",
    "MEDICINE & SCIENCE IN SPORTS & EXERCISE",
    "MELANOMA RESEARCH",
    "MENOPAUSE-THE JOURNAL OF THE NORTH AMERICAN MENOPAUSE SOCIETY",
    "NEUROLOGIST",
    "NEUROREPORT",
    "NUCLEAR MEDICINE COMMUNICATIONS",
    "NURSE EDUCATOR",
    "NURSING RESEARCH",
    "OBSTETRICAL & GYNECOLOGICAL SURVEY",
    "OBSTETRICS AND GYNECOLOGY",
    "OPHTHALMIC PLASTIC AND RECONSTRUCTIVE SURGERY",
    "OPTOMETRY AND VISION SCIENCE",
    "ORTHOPAEDIC NURSING",
    "OTOLOGY & NEUROTOLOGY",
    "PAIN",
    "PANCREAS",
    "PEDIATRIC CRITICAL CARE MEDICINE",
    "PEDIATRIC EMERGENCY CARE",
    "PEDIATRIC INFECTIOUS DISEASE JOURNAL",
    "PEDIATRIC PHYSICAL THERAPY",
    "PHARMACOGENETICS AND GENOMICS",
    "PLASTIC AND RECONSTRUCTIVE SURGERY",
    "PSYCHIATRIC GENETICS",
    "QUALITY MANAGEMENT IN HEALTH CARE",
    "REHABILITATION NURSING",
    "RETINA-THE JOURNAL OF RETINAL AND VITREOUS DISEASES",
    "SEXUALLY TRANSMITTED DISEASES",
    "SHOCK",
    "SIMULATION IN HEALTHCARE-JOURNAL OF THE SOCIETY FOR SIMULATION IN HEALTHCARE",
    "SOIL SCIENCE",
    "SOUTHERN MEDICAL JOURNAL",
    "SPINE",
    "SPORTS MEDICINE AND ARTHROSCOPY REVIEW",
    "STRENGTH AND CONDITIONING JOURNAL",
    "SURGICAL LAPAROSCOPY ENDOSCOPY & PERCUTANEOUS TECHNIQUES",
    "THERAPEUTIC DRUG MONITORING",
    "TOPICS IN CLINICAL NUTRITION",
    "TRANSPLANTATION",
    "ULTRASOUND QUARTERLY",
    "NEUROSURGERY",
    "OPERATIVE NEUROSURGERY",
]

eissns = [
    "1550-5014",
    "1538-8654",
    "1473-5571",
    "0893-0341",
    "1537-453X",
    "1533-0311",
    "1533-404X",
    "1572-0241",
    "1555-824X",
    "1538-7488",
    "1537-7385",
    "1532-0979",
    "1536-3686",
    "1528-1175",
    "1536-3708",
    "1528-1140",
    "1473-5741",
    "1533-4058",
    "1538-943X",
    "1473-5849",
    "1473-5733",
    "1473-5725",
    "1538-9804",
    "1538-4683",
    "2542-5641",
    "1538-9774",
    "2155-384X",
    "1473-5717",
    "1536-5409",
    "1536-3724",
    "1537-162X",
    "1536-0229",
    "1538-9782",
    "1532-5520",
    "1528-1132",
    "2380-0186",
    "1543-3641",
    "1536-4798",
    "1473-5830",
    "1530-0293",
    "1473-6322",
    "1473-6500",
    "1531-7080",
    "1473-6519",
    "1531-7072",
    "1752-2978",
    "1531-7056",
    "1531-7048",
    "1746-6318",
    "1473-6527",
    "1473-6535",
    "1473-6543",
    "1473-6551",
    "1473-656X",
    "1531-703X",
    "1531-7021",
    "1531-7013",
    "1531-6998",
    "1531-698X",
    "1473-6578",
    "1531-6971",
    "1531-6963",
    "1751-4266",
    "1473-6586",
    "1537-8918",
    "1530-0358",
    "1538-4667",
    "1531-5487",
    "1365-2346",
    "1473-5709",
    "1473-5695",
    "1473-5687",
    "1538-3008",
    "1542-233X",
    "1538-9766",
    "1465-7309",
    "1538-5159",
    "2572-9241",
    "1550-5138",
    "1056-6163",
    "1473-5857",
    "1538-7151",
    "1473-5660",
    "1536-0210",
    "1077-9450",
    "1552-6917",
    "2691-3321",
    "1536-7355",
    "1945-1474",
    "1935-3227",
    "1548-7148",
    "1535-1386",
    "1932-751X",
    "1558-2035",
    "1550-5049",
    "1533-4023",
    "1873-4502",
    "1539-2031",
    "1537-1603",
    "1533-712X",
    "1532-3145",
    "1554-558X",
    "1536-3732",
    "1536-7312",
    "1533-4112",
    "1939-3938",
    "2152-0895",
    "1536-481X",
    "1550-509X",
    "1539-0705",
    "1473-5598",
    "1537-4513",
    "1526-0976",
    "1539-736X",
    "1557-0584",
    "1536-5166",
    "1945-2810",
    "1537-1921",
    "1539-0721",
    "1550-5065",
    "1536-5948",
    "1531-2291",
    "1536-4801",
    "1536-3678",
    "1539-2570",
    "1473-5865",
    "1550-5073",
    "1538-1145",
    "1533-4287",
    "2327-6924",
    "1728-7731",
    "1536-0237",
    "1932-3883",
    "1528-3976",
    "1539-0683",
    "1537-1948",
    "1536-5964",
    "1530-0315",
    "1473-5636",
    "1530-0374",
    "2331-2637",
    "1473-558X",
    "1473-5628",
    "1538-9855",
    "1538-9847",
    "1533-9866",
    "0029-7844",
    "1537-2677",
    "1538-9235",
    "1542-538X",
    "1537-4505",
    "1872-6623",
    "1536-4828",
    "1947-3893",
    "1535-1815",
    "1532-0987",
    "1538-005X",
    "1744-6880",
    "1529-4242",
    "1473-5873",
    "1550-5154",
    "2048-7940",
    "1539-2864",
    "1537-4521",
    "1540-0514",
    "1559-713X",
    "1538-9243",
    "1541-8243",
    "1528-1159",
    "1538-1951",
    "1533-4295",
    "1534-4908",
    "1536-3694",
    "1550-5146",
    "1534-6080",
    "1536-0253",
    "1524-4040",
    "2332-4260",
]

links = [
    "http://journals.lww.com/advancesinnursingscience/pages/default.aspx",
    "http://journals.lww.com/aswcjournal/pages/default.aspx",
    "http://journals.lww.com/aidsonline/pages/default.aspx",
    "http://journals.lww.com/alzheimerjournal/pages/default.aspx",
    "http://journals.lww.com/amjclinicaloncology/pages/default.aspx",
    "http://journals.lww.com/amjdermatopathology/pages/default.aspx",
    "http://journals.lww.com/amjforensicmedicine/pages/default.aspx",
    "https://journals.lww.com/ajg/pages/issuelist.aspx",
    "https://journals.lww.com/ajmqonline/Pages/default.aspx",
    "http://journals.lww.com/ajnonline/pages/default.aspx",
    "http://journals.lww.com/ajpmr/pages/default.aspx",
    "http://journals.lww.com/ajsp/pages/default.aspx",
    "http://journals.lww.com/americantherapeutics/pages/default.aspx",
    "http://journals.lww.com/anesthesiology/pages/default.aspx",
    "http://journals.lww.com/annalsplasticsurgery/pages/default.aspx",
    "http://journals.lww.com/annalsofsurgery/pages/default.aspx",
    "http://journals.lww.com/anti-cancerdrugs/pages/default.aspx",
    "http://journals.lww.com/appliedimmunohist/pages/default.aspx",
    "http://journals.lww.com/asaiojournal/pages/default.aspx",
    "http://journals.lww.com/behaviouralpharm/pages/default.aspx",
    "http://journals.lww.com/bloodcoagulation/pages/default.aspx",
    "http://journals.lww.com/bpmonitoring/pages/default.aspx",
    "http://journals.lww.com/cancernursingonline/pages/default.aspx",
    "http://journals.lww.com/cardiologyinreview/pages/default.aspx",
    "https://journals.lww.com/cmj/pages/default.aspx",
    "http://journals.lww.com/cinjournal/pages/default.aspx",
    "https://journals.lww.com/ctg/pages/default.aspx",
    "http://journals.lww.com/clindysmorphol/pages/default.aspx",
    "http://journals.lww.com/clinicalpain/pages/default.aspx",
    "http://journals.lww.com/cjsportsmed/pages/default.aspx",
    "http://journals.lww.com/clinicalneuropharm/pages/default.aspx",
    "http://journals.lww.com/nuclearmed/pages/default.aspx",
    "http://journals.lww.com/cns-journal/pages/default.aspx",
    "http://journals.lww.com/clinicalobgyn/pages/default.aspx",
    "https://shop.lww.com/Clinical-Orthopaedics-and-Related-Research-/p/0009-921X",
    "http://journals.lww.com/jspinaldisorders/pages/default.aspx",
    "http://journals.lww.com/cogbehavneurol/pages/default.aspx",
    "http://journals.lww.com/corneajrnl/pages/default.aspx",
    "http://journals.lww.com/coronary-artery/pages/default.aspx",
    "http://journals.lww.com/ccmjournal/pages/default.aspx",
    "http://journals.lww.com/co-allergy/pages/default.aspx",
    "http://journals.lww.com/co-anesthesiology/pages/default.aspx",
    "http://journals.lww.com/co-cardiology/pages/default.aspx",
    "http://journals.lww.com/co-clinicalnutrition/pages/default.aspx",
    "http://journals.lww.com/co-criticalcare/pages/default.aspx",
    "http://journals.lww.com/co-endocrinology/pages/default.aspx",
    "http://journals.lww.com/co-gastroenterology/pages/default.aspx",
    "http://journals.lww.com/co-hematology/pages/default.aspx",
    "http://journals.lww.com/co-hivandaids/pages/default.aspx",
    "http://journals.lww.com/co-infectiousdiseases/pages/default.aspx",
    "http://journals.lww.com/co-lipidology/pages/default.aspx",
    "http://journals.lww.com/co-nephrolhypertens/pages/default.aspx",
    "http://journals.lww.com/co-neurology/pages/default.aspx",
    "http://journals.lww.com/co-obgyn/pages/default.aspx",
    "http://journals.lww.com/co-oncology/pages/default.aspx",
    "http://journals.lww.com/co-ophthalmology/pages/default.aspx",
    "http://journals.lww.com/co-transplantation/pages/default.aspx",
    "http://journals.lww.com/co-otolaryngology/pages/default.aspx",
    "http://journals.lww.com/co-pediatrics/pages/default.aspx",
    "http://journals.lww.com/co-psychiatry/pages/default.aspx",
    "http://journals.lww.com/co-pulmonarymedicine/pages/default.aspx",
    "http://journals.lww.com/co-rheumatology/pages/default.aspx",
    "https://journals.lww.com/co-supportiveandpalliativecare/pages/default.aspx",
    "http://journals.lww.com/co-urology/pages/default.aspx",
    "http://journals.lww.com/acsm-csmr/pages/default.aspx",
    "http://journals.lww.com/dcrjournal/pages/default.aspx",
    "http://journals.lww.com/ear-hearing/pages/default.aspx",
    "http://journals.lww.com/epidem/pages/default.aspx",
    "http://journals.lww.com/ejanaesthesiology/pages/default.aspx",
    "http://journals.lww.com/eurjcancerprev/pages/default.aspx",
    "http://journals.lww.com/euro-emergencymed/pages/default.aspx",
    "http://journals.lww.com/eurojgh/pages/default.aspx",
    "http://journals.lww.com/acsm-essr/pages/default.aspx",
    "http://journals.lww.com/claojournal/pages/default.aspx",
    "http://journals.lww.com/gastroenterologynursing/pages/default.aspx",
    "http://www.lww.com/webapp/wcs/stores/servlet/product_Harvard-Review-of-Psychiatry_11851_-1_12551_Prod-10673229",
    "http://journals.lww.com/health-physics/pages/default.aspx",
    "https://journals.lww.com/hemasphere/pages/default.aspx",
    "http://journals.lww.com/hnpjournal/pages/default.aspx",
    "http://journals.lww.com/implantdent/pages/default.aspx",
    "http://journals.lww.com/intclinpsychopharm/pages/default.aspx",
    "http://journals.lww.com/intjgynpathology/pages/default.aspx",
    "http://journals.lww.com/intjrehabilres/pages/default.aspx",
    "http://journals.lww.com/investigativeradiology/pages/default.aspx",
    "http://journals.lww.com/jaids/pages/default.aspx",
    "https://journals.lww.com/janac/pages/default.aspx",
    "https://journals.lww.com/ijebh/pages/default.aspx",
    "http://journals.lww.com/jclinrheum/pages/default.aspx",
    "http://journals.lww.com/jhqonline/pages/default.aspx",
    "http://journals.lww.com/journaladdictionmedicine/pages/default.aspx",
    "http://journals.lww.com/jan/Pages/default.aspx",
    "https://shop.lww.com/The-Journal-of-Bone---Joint-Surgery/p/0021-9355",
    "http://journals.lww.com/jcrjournal/pages/default.aspx",
    "http://journals.lww.com/jcardiovascularmedicine/pages/default.aspx",
    "http://journals.lww.com/jcnjournal/pages/default.aspx",
    "http://journals.lww.com/cardiovascularpharm/pages/default.aspx",
    "https://journals.lww.com/jcrs/pages/issuelist.aspx",
    "http://journals.lww.com/jcge/pages/default.aspx",
    "http://journals.lww.com/clinicalneurophys/pages/default.aspx",
    "http://journals.lww.com/psychopharmacology/pages/default.aspx",
    "http://journals.lww.com/jcat/pages/default.aspx",
    "http://journals.lww.com/jcehp/Pages/default.aspx",
    "http://journals.lww.com/jcraniofacialsurgery/pages/default.aspx",
    "http://journals.lww.com/jrnldbp/pages/default.aspx",
    "http://journals.lww.com/ectjournal/pages/default.aspx",
    "http://journals.lww.com/forensicnursing/pages/default.aspx",
    "http://journals.lww.com/jgpt/pages/default.aspx",
    "http://journals.lww.com/glaucomajournal/pages/default.aspx",
    "http://journals.lww.com/headtraumarehab/pages/default.aspx",
    "http://journals.lww.com/jhpn/pages/default.aspx",
    "http://journals.lww.com/jhypertension/pages/default.aspx",
    "http://journals.lww.com/immunotherapy-journal/pages/default.aspx",
    "http://journals.lww.com/jlgtd/pages/default.aspx",
    "http://journals.lww.com/jonmd/pages/default.aspx",
    "http://journals.lww.com/jnpt/pages/default.aspx",
    "http://journals.lww.com/jneuro-ophthalmology/pages/default.aspx",
    "http://journals.lww.com/jnnonline/pages/default.aspx",
    "http://journals.lww.com/jnsa/pages/default.aspx",
    "http://journals.lww.com/jonajournal/pages/default.aspx",
    "http://journals.lww.com/jncqjournal/pages/default.aspx",
    "http://journals.lww.com/joem/pages/default.aspx",
    "http://journals.lww.com/jorthotrauma/pages/default.aspx",
    "http://journals.lww.com/jpgn/pages/default.aspx",
    "http://journals.lww.com/jpho-online/pages/default.aspx",
    "http://journals.lww.com/pedorthopaedics/pages/default.aspx",
    "http://journals.lww.com/jpo-b/pages/default.aspx",
    "http://journals.lww.com/jpnnjournal/pages/default.aspx",
    "http://journals.lww.com/practicalpsychiatry/pages/default.aspx",
    "http://journals.lww.com/nsca-jscr/pages/default.aspx",
    "https://journals.lww.com/jaanp/pages/aboutthejournal.aspx",
    "https://journals.lww.com/jcma/pages/default.aspx",
    "http://journals.lww.com/thoracicimaging/pages/default.aspx",
    "http://journals.lww.com/journaloftraumanursing/pages/default.aspx",
    "http://journals.lww.com/jwocnonline/pages/default.aspx",
    "http://journals.lww.com/mcnjournal/pages/default.aspx",
    "http://journals.lww.com/lww-medicalcare/pages/default.aspx",
    "http://journals.lww.com/md-journal/pages/default.aspx",
    "http://journals.lww.com/acsm-msse/pages/default.aspx",
    "http://journals.lww.com/melanomaresearch/pages/default.aspx",
    "http://journals.lww.com/menopausejournal/pages/default.aspx",
    "http://journals.lww.com/theneurologist/pages/default.aspx",
    "http://journals.lww.com/neuroreport/pages/default.aspx",
    "http://journals.lww.com/nuclearmedicinecomm/pages/default.aspx",
    "http://journals.lww.com/nurseeducatoronline/pages/default.aspx",
    "http://journals.lww.com/nursingresearchonline/pages/default.aspx",
    "http://journals.lww.com/obgynsurvey/pages/default.aspx",
    "http://journals.lww.com/greenjournal/pages/default.aspx",
    "http://journals.lww.com/op-rs/pages/default.aspx",
    "http://journals.lww.com/optvissci/pages/default.aspx",
    "http://journals.lww.com/orthopaedicnursing/pages/default.aspx",
    "http://journals.lww.com/otology-neurotology/pages/default.aspx",
    "http://journals.lww.com/pain/pages/default.aspx",
    "http://journals.lww.com/pancreasjournal/pages/default.aspx",
    "http://journals.lww.com/pccmjournal/pages/default.aspx",
    "http://journals.lww.com/pec-online/pages/default.aspx",
    "http://journals.lww.com/pidj/pages/default.aspx",
    "http://journals.lww.com/pedpt/pages/default.aspx",
    "http://journals.lww.com/jpharmacogenetics/pages/default.aspx",
    "http://journals.lww.com/plasreconsurg/pages/default.aspx",
    "http://journals.lww.com/psychgenetics/pages/default.aspx",
    "http://journals.lww.com/gmhcjournal/pages/default.aspx",
    "https://journals.lww.com/rehabnursingjournal/Pages/currenttoc.aspx",
    "http://journals.lww.com/retinajournal/pages/default.aspx",
    "http://journals.lww.com/stdjournal/pages/default.aspx",
    "http://journals.lww.com/shockjournal/pages/default.aspx",
    "http://journals.lww.com/simulationinhealthcare/pages/default.aspx",
    "http://journals.lww.com/soilsci/pages/default.aspx",
    "http://journals.lww.com/smajournalonline/pages/default.aspx",
    "http://journals.lww.com/spinejournal/pages/default.aspx",
    "http://journals.lww.com/sportsmedarthro/pages/default.aspx",
    "http://journals.lww.com/nsca-scj/pages/default.aspx",
    "http://journals.lww.com/surgical-laparoscopy/pages/default.aspx",
    "http://journals.lww.com/drug-monitoring/pages/default.aspx",
    "http://journals.lww.com/topicsinclinicalnutrition/pages/default.aspx",
    "http://journals.lww.com/transplantjournal/pages/default.aspx",
    "http://journals.lww.com/ultrasound-quarterly/pages/default.aspx",
    "https://journals.lww.com/neurosurgery/Pages/issuelist.aspx",
    "https://journals.lww.com/onsonline/Pages/issuelist.aspx",
]

domain = "https://journals.lww.com"
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
        options.add_argument('--proxy-server=%s' % "5.79.66.2:13080")
        options.add_argument("--start-maximized") #open Browser in maximized mode
        options.add_argument("--no-sandbox") #bypass OS security model
        options.add_argument("--disable-dev-shm-usage") #overcome limited resource problems
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--ignore-certificate-errors")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-blink-features=AutomationControlled")
        # options.add_argument("--no-proxy-server")
        self.driver = webdriver.Chrome(options=options, executable_path=ChromeDriverManager().install())

    def fetchSingle(self, base_url, journal_title, issn):
        print(f"[sci] [chalcogen] [{issn}] {base_url}")
        res = bs(fetch_url(base_url).text, 'lxml')
        issues = res.select("div.AVDetailViewListItem div.AVIssueLink a")
        if issues:
            for issue in issues:
                year = int(issue.text.split(',')[1].strip().split()[-1].strip())
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

    # export_csv()