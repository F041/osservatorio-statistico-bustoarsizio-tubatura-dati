import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import time
import os
import base64
import binascii
from urllib.parse import urljoin, urlparse, unquote
import logging
import re
import csv

# --- CONFIGURAZIONE ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
BASE_URL = "https://bustoarsizio.trasparenza-valutazione-merito.it"
# Pagina caricata nell'iframe dalla voce di menu "Pagamenti dell'amministrazione > Dati sui pagamenti"
# (data-mainurl di #menu-item-header-35127). Da ottobre 2026 Firefox/Selenium riceve la home a pochi KB/s
# e va in timeout, mentre via HTTP la lista e la paginazione funzionano: niente più browser.
LIST_PAGE_URL = f"{BASE_URL}/web/trasparenza/papca-g/-/papca/igrid/36236"
MAX_LIST_PAGES = 5

# --- Percorsi  ---
# Ottieni la directory dello script corrente (src)
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
# Vai alla directory genitore (radice del progetto)
PROJECT_ROOT = os.path.dirname(SRC_DIR)
# Definisci la cartella dei dati relativa alla radice
DATA_FOLDER = os.path.join(PROJECT_ROOT, "data")
DOWNLOAD_DIR = os.path.join(DATA_FOLDER, "downloaded_files")
OUTPUT_CSV = os.path.join(DATA_FOLDER, "found_excel_links_iframe.csv")

# Crea le directory se non esistono (compatibile anche in ambiente CI)
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- Altre Costanti ---
def is_relevant_object(text): text_upper = text.upper(); return "PAGAMENTI" in text_upper
ALLOWED_EXTENSIONS = ('.xlsx', '.xls', '.ods')
DOWNLOAD_LINK_SELECTOR = "a[href*='downloadAllegato']"
ATOB_URL_RE = re.compile(r"atob\('([A-Za-z0-9+/=]+)'\)")
DETAIL_LINK_TITLE = "Apri Dettaglio"
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'})
# Il server chiude spesso le connessioni (RemoteDisconnected / read timeout): ritenta con backoff
session.mount("https://", HTTPAdapter(max_retries=Retry(total=4, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET", "HEAD"))))
# ────────────────────────────────────────────────────────────────────────────────────

# --- Funzioni (Download, Estrazione HTML) - INVARIATE ---
def sanitize_filename(fn: str) -> str: fn = unquote(fn); fn = re.sub(r"^(?:filename\*=UTF-8'')?", '', fn, flags=re.IGNORECASE); fn = re.sub(r'[\\/*?:"<>|]', '_', fn); fn = fn.strip(); return fn[:150]
def find_excel_link_in_detail(detail_url):
    time.sleep(0.3); logging.info(f"Accesso a pagina dettaglio (requests): {detail_url}")
    try: response = session.get(detail_url, timeout=30); response.raise_for_status(); html_content = response.text
    except requests.exceptions.RequestException as e: logging.error(f"Errore accesso dettaglio {detail_url}: {e}"); return None
    soup = BeautifulSoup(html_content, 'lxml'); link_tag = soup.select_one(DOWNLOAD_LINK_SELECTOR)
    if link_tag and link_tag.has_attr('href'): excel_url = urljoin(BASE_URL, link_tag['href']); logging.info(f"Trovato link download ({DOWNLOAD_LINK_SELECTOR}): {excel_url}"); return excel_url
    # Dal 2026 il portale nasconde l'URL: href="javascript:void(0)" + onclick="window.open(atob('<base64>'))"
    for onclick_tag in soup.select("a[onclick*='atob(']"):
        match = ATOB_URL_RE.search(onclick_tag['onclick'])
        if not match: continue
        try: decoded_url = base64.b64decode(match.group(1)).decode('utf-8')
        except (binascii.Error, UnicodeDecodeError): continue
        if 'downloadAllegato' in decoded_url: excel_url = urljoin(BASE_URL, decoded_url); logging.info(f"Trovato link download (onclick atob): {excel_url}"); return excel_url
    for link_tag_fallback in soup.find_all('a', href=True):
         href = link_tag_fallback['href']
         if any((href.lower().endswith(ext + '?p_auth=') or href.lower().endswith(ext)) for ext in ALLOWED_EXTENSIONS + ('.zip', '.pdf', '.doc', '.p7m')): excel_url = urljoin(BASE_URL, href); logging.info(f"Trovato link download (fallback generico): {excel_url}"); return excel_url
    logging.warning(f"Nessun link allegato trovato in: {detail_url}"); return None
def download_file(url: str, publication_object: str, data_id: str):
    try:
        logging.debug(f"Eseguo HEAD request per: {url}"); head_response = session.head(url, allow_redirects=True, timeout=20); head_response.raise_for_status(); final_url = head_response.url
        filename = None; match = None; content_disposition = head_response.headers.get("Content-Disposition", ""); logging.debug(f"Content-Disposition: '{content_disposition}'")
        if "filename*" in content_disposition: match = re.search(r"filename\*=([^';\s]+)'([^']*)'([^;]+)", content_disposition);
        if match: filename = sanitize_filename(match.group(3))
        match = None
        if not filename and "filename=" in content_disposition: match = re.search(r'filename="?([^";]+)"?', content_disposition);
        if match: 
            try: filename = sanitize_filename(match.group(1).encode('latin-1').decode('utf-8')); 
            except: filename = sanitize_filename(match.group(1))
        if not filename: logging.debug("Nome file non da CD. Tento da URL."); path_part = urlparse(final_url).path; filename = sanitize_filename(os.path.basename(path_part) or f"download_{data_id}")
        if not filename or filename == "." or not os.path.splitext(filename)[1] : safe_object_name = "".join(c if c.isalnum() else "_" for c in publication_object[:50]).strip('_'); original_ext = os.path.splitext(urlparse(url).path)[1]; original_ext=original_ext if original_ext and len(original_ext)<=5 else ".tmp"; filename = f"pagamenti_{data_id}_{safe_object_name}{original_ext}"; logging.warning(f"Generato nome file di fallback: {filename}")
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS: logging.info(f"⏭️  File '{filename}' saltato (estensione '{file_ext}' non consentita)."); return None
        # Usa la costante DOWNLOAD_DIR definita correttamente
        dest_path = os.path.join(DOWNLOAD_DIR, filename)
        if os.path.exists(dest_path): logging.info(f"✔️ File già presente: {filename}"); return dest_path
        logging.info(f"⬇️ Scarico '{filename}' da {final_url}")
        with session.get(final_url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(dest_path, "wb") as f: bytes_downloaded = 0; start_time = time.time(); [f.write(chunk) for chunk in resp.iter_content(chunk_size=8192) if (bytes_downloaded := bytes_downloaded + len(chunk))]; duration = time.time() - start_time; speed_kbps = (bytes_downloaded / 1024 / duration) if duration > 0 else 0; logging.info(f"   → Salvato: {dest_path} ({bytes_downloaded/1024:.1f} KB in {duration:.2f}s, {speed_kbps:.1f} KB/s)")
        return dest_path
    except UnboundLocalError as e: logging.error(f"!!! UnboundLocalError download {url}: {e}."); return None
    except requests.exceptions.RequestException as e: logging.error(f"Errore rete download/HEAD {url}: {e}"); return None
    except Exception as e: logging.error(f"Errore generico download {url}: {e}"); return None
def extract_data_from_html(html_content):
    soup = BeautifulSoup(html_content, 'lxml'); relevant_items = []
    table = soup.find('table', class_='master-detail-list-table')
    if not table: logging.error("Tabella ('master-detail-list-table') non trovata nell'HTML (iframe?)."); return relevant_items
    rows = table.find_all('tr', class_='master-detail-list-line'); logging.info(f"BS4: Trovate {len(rows)} righe.")
    items_added_count = 0
    for row in rows:
        object_cell = row.find('td', class_='oggetto'); action_cell = row.find('td', class_='actions')
        if object_cell and action_cell:
            object_text = object_cell.get_text(strip=True)
            # Filtro keyword applicato
            if is_relevant_object(object_text):
                logging.info(f" Oggetto RILEVANTE trovato: '{object_text[:60]}...'")
                detail_link_tag = action_cell.find('a', title=DETAIL_LINK_TITLE)
                if detail_link_tag and detail_link_tag.has_attr('href'):
                    detail_url = urljoin(BASE_URL, detail_link_tag['href']); item_info = {'object': object_text, 'detail_url': detail_url, 'data_id': row.get('data-id', detail_url)}; relevant_items.append(item_info); items_added_count += 1
                else: logging.warning(f" Riga rilevante '{object_text[:60]}...' ma senza link dettaglio.")
            # else: logging.debug(f" Oggetto scartato (no keyword): '{object_text[:60]}...'")
        # else: logging.debug(" Riga saltata (mancano celle oggetto/azioni).")
    logging.info(f"BS4: Estratti {items_added_count} atti rilevanti da questo HTML.")
    return relevant_items


# --- CICLO PRINCIPALE: LISTA ATTI VIA HTTP ---
def run_scraper_main():
    all_items_found = []; processed_detail_urls = set(); excel_links_found = []
    downloaded_files_summary = [] # Inizializzazione per riepilogo

    # La paginazione è lato server e legata al cookie di sessione: "Avanti" è un link GET (paginationAction=NEXT)
    logging.info("--- Inizio Paginazione & Estrazione (HTTP) ---"); page_url = LIST_PAGE_URL; page_count = 1
    while page_url and page_count <= MAX_LIST_PAGES:
        logging.info(f"--- Processo Pagina Lista {page_count}: {page_url} ---")
        try: response = session.get(page_url, timeout=60); response.raise_for_status(); current_html = response.text
        except requests.exceptions.RequestException as e: logging.error(f"Errore accesso lista pagina {page_count}: {e}"); break
        items_on_page = extract_data_from_html(current_html)

        # Aggiungi elementi trovati
        newly_added_count = 0
        for item in items_on_page:
            detail_url = item['detail_url']
            if detail_url not in processed_detail_urls: processed_detail_urls.add(detail_url); all_items_found.append(item); newly_added_count += 1
        logging.info(f"Aggiunti {newly_added_count} nuovi atti da pagina {page_count}. Totale: {len(all_items_found)}")

        # Paginazione: link "Avanti" con href reale se attivo, "javascript:void(0)" se disabilitato
        next_link = BeautifulSoup(current_html, 'lxml').find('a', string=re.compile(r'^\s*Avanti\s*$'))
        next_href = next_link.get('href', '') if next_link else ''
        if next_href.startswith('http') or next_href.startswith('/'): page_url = urljoin(BASE_URL, next_href); page_count += 1; time.sleep(0.5)
        else: logging.info("Link 'Avanti' assente o disabilitato: fine paginazione."); page_url = None
    if page_url: logging.warning(f"Raggiunto limite pagine lista {MAX_LIST_PAGES}.")
    logging.info(f"--- Fine Fase Lista: Trovati {len(all_items_found)} atti totali rilevanti. ---")

    # --- Download e Riepilogo ---
    logging.info(f"--- FASE DOWNLOAD: Scarico allegati ({', '.join(ALLOWED_EXTENSIONS)}) ---")
    processed_urls=set()
    if not all_items_found: logging.warning("Nessun atto da processare.")
    else:
        for i, item_info in enumerate(all_items_found):
            logging.info(f"[{i+1}/{len(all_items_found)}] Processo Download: '{item_info['object'][:80]}...'")
            excel_url = find_excel_link_in_detail(item_info['detail_url'])
            if excel_url:
                if excel_url not in excel_links_found: excel_links_found.append(excel_url)
                processed_urls.add(excel_url)
                item_id_for_download = item_info['data_id'] if item_info['data_id'] != item_info['detail_url'] else item_info['detail_url'].split('/')[-1]
                time.sleep(0.5 + (i % 10 * 0.05))
                downloaded_path = download_file(excel_url, item_info['object'], item_id_for_download)
                if downloaded_path: downloaded_files_summary.append({'object': item_info['object'], 'source_url': item_info['detail_url'], 'excel_url': excel_url, 'local_path': downloaded_path, 'data_id': item_info['data_id'] })

    logging.info("--- FASE RIEPILOGO ---")
    logging.info(f"Scraping completato. File consentiti scaricati: {len(downloaded_files_summary)}. Link allegato trovati: {len(excel_links_found)}.")
    logging.info(f"Atti unici processati: {len(processed_detail_urls)}.")
    if excel_links_found:
         try:
             # Usa la costante OUTPUT_CSV definita correttamente
             with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
                 writer = csv.writer(f); writer.writerow(["URL_Allegato_Trovato"])
                 for u in excel_links_found:
                     try: writer.writerow([u])
                     except Exception as e_write: logging.error(f"Errore scrittura riga CSV per URL {u}: {e_write}")
             logging.info(f"📄 Salvati {len(excel_links_found)} link allegato potenziali in '{OUTPUT_CSV}'")
         except IOError as e: logging.error(f"Errore salvataggio CSV '{OUTPUT_CSV}': {e}")
    return downloaded_files_summary, excel_links_found

# --- Esecuzione ---
if __name__ == "__main__":
    start_time_script = time.time(); downloaded_summary, links_found = run_scraper_main(); end_time_script = time.time()
    print("\n" + "="*40); print("--- RIEPILOGO SCRAPING COMPLETATO ---"); print("="*40)
    # Usa la costante DOWNLOAD_DIR definita correttamente
    if downloaded_summary: print(f"Sono stati scaricati {len(downloaded_summary)} file ({', '.join(ALLOWED_EXTENSIONS)}) nella cartella '{DOWNLOAD_DIR}':");
    for i, file_info in enumerate(downloaded_summary): print(f" {i+1}. {os.path.basename(file_info['local_path'])} (da: {file_info['object'][:60]}...)")
    else: print(f"Nessun file con estensione {ALLOWED_EXTENSIONS} è stato scaricato.")
    # Usa la costante OUTPUT_CSV definita correttamente
    print(f"\nTrovati {len(links_found)} link allegato totali (vedi '{OUTPUT_CSV}')."); print(f"Controlla i log per file saltati (⏭️) o errori.");
    total_duration = end_time_script - start_time_script; print(f"Tempo totale di esecuzione: {total_duration:.2f} secondi."); print("="*40)
