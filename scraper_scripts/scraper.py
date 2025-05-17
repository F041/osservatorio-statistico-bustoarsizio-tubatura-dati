import requests
from bs4 import BeautifulSoup
import time
import os
from urllib.parse import urljoin, urlparse, unquote
import logging
import re
import csv
import math

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementClickInterceptedException
from webdriver_manager.firefox import GeckoDriverManager

# --- CONFIGURAZIONE ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
BASE_URL = "https://bustoarsizio.trasparenza-valutazione-merito.it"
MAIN_PAGE_URL = "https://bustoarsizio.trasparenza-valutazione-merito.it/"
MENU_PAGAMENTI_XPATH = "//*[@id='menu-item-header-35125']"
SUBMENU_DATI_PAGAMENTI_SELECTOR_ID = "menu-item-header-35127"
IFRAME_SELECTOR_ID = "corrente-iframe"

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
DETAIL_LINK_TITLE = "Apri Dettaglio"
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'})
# ────────────────────────────────────────────────────────────────────────────────────

# --- Funzioni (Download, Estrazione HTML) - INVARIATE ---
def sanitize_filename(fn: str) -> str: fn = unquote(fn); fn = re.sub(r"^(?:filename\*=UTF-8'')?", '', fn, flags=re.IGNORECASE); fn = re.sub(r'[\\/*?:"<>|]', '_', fn); fn = fn.strip(); return fn[:150]
def find_excel_link_in_detail(detail_url):
    time.sleep(0.3); logging.info(f"Accesso a pagina dettaglio (requests): {detail_url}")
    try: response = session.get(detail_url, timeout=30); response.raise_for_status(); html_content = response.text
    except requests.exceptions.RequestException as e: logging.error(f"Errore accesso dettaglio {detail_url}: {e}"); return None
    soup = BeautifulSoup(html_content, 'lxml'); link_tag = soup.select_one(DOWNLOAD_LINK_SELECTOR)
    if link_tag and link_tag.has_attr('href'): excel_url = urljoin(BASE_URL, link_tag['href']); logging.info(f"Trovato link download ({DOWNLOAD_LINK_SELECTOR}): {excel_url}"); return excel_url
    else:
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


# --- CICLO PRINCIPALE CON IFRAME SENZA CLICK INTERNI ---
def run_scraper_main():
    all_items_found = []; processed_detail_urls = set(); excel_links_found = []
    downloaded_files_summary = [] # Inizializzazione per riepilogo
    driver = None
    try:
        logging.info("Inizializzazione WebDriver Firefox..."); service = FirefoxService(GeckoDriverManager().install())
        options = FirefoxOptions(); options.add_argument("--headless"); # Modalità headless per CI
        options.add_argument("--disable-gpu"); options.add_argument("--window-size=1920,1080");
        driver = webdriver.Firefox(service=service, options=options); wait = WebDriverWait(driver, 30)

        # 1. Vai alla pagina principale
        logging.info(f"Navigazione alla pagina principale: {MAIN_PAGE_URL}"); driver.get(MAIN_PAGE_URL)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body"))); logging.info("Pagina principale caricata.")

        # 2. Clicca sul menu Pagamenti per aprire il POPUP ROSSO
        logging.info(f"Tentativo click JS menu pagamenti ({MENU_PAGAMENTI_XPATH})...")
        menu_pagamenti = wait.until(EC.presence_of_element_located((By.XPATH, MENU_PAGAMENTI_XPATH)))
        driver.execute_script("arguments[0].click();", menu_pagamenti)
        logging.info("Click JS menu pagamenti eseguito.")

        # 3. Clicca sulla voce "Dati sui pagamenti" DENTRO IL POPUP ROSSO
        logging.info(f"Tentativo click voce 'Dati sui pagamenti' nel popup (ID: {SUBMENU_DATI_PAGAMENTI_SELECTOR_ID})...")
        submenu_popup_pagamenti = wait.until(EC.element_to_be_clickable((By.ID, SUBMENU_DATI_PAGAMENTI_SELECTOR_ID)))
        try: submenu_popup_pagamenti.click()
        except ElementClickInterceptedException: logging.warning("Click normale intercettato sottomenu, uso JS..."); driver.execute_script("arguments[0].click();", submenu_popup_pagamenti)
        logging.info("Click voce 'Dati sui pagamenti' nel popup OK.")

        # 4. Attendi e passa all'iframe
        logging.info("Attesa caricamento iframe..."); iframe_locator = (By.ID, IFRAME_SELECTOR_ID)
        wait.until(EC.frame_to_be_available_and_switch_to_it(iframe_locator))
        logging.info(f"Passato al contesto dell'iframe (#{IFRAME_SELECTOR_ID}).")
        time.sleep(2)

        # --- ORA SIAMO DENTRO L'IFRAME ---
        # 5. RIMOZIONE CLICK INTERNI: Assumiamo che la vista sia già corretta
        logging.info("Assumendo vista iframe corretta, estraggo direttamente...")

        # 6. Estrazione/Paginazione DENTRO l'iframe
        logging.info("--- Inizio Paginazione & Estrazione (Iframe) ---"); page_count = 1; max_pages_selenium = 5 # Messo limite basso per testare paginazione se serve
        while page_count <= max_pages_selenium:
             logging.info(f"--- Processo Pagina Selenium {page_count} (Iframe) ---")
             try:
                 # Attendi la tabella
                 table_locator_css = "table.master-detail-list-table"; table_row_locator_css = f"{table_locator_css} tr.master-detail-list-line"
                 wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, table_locator_css))); logging.debug(f"Tabella visibile (Pag {page_count})")
                 current_html = driver.page_source; items_on_page = extract_data_from_html(current_html)                

                 # Aggiungi elementi trovati
                 newly_added_count = 0
                 for item in items_on_page:
                     detail_url = item['detail_url']
                     if detail_url not in processed_detail_urls: processed_detail_urls.add(detail_url); all_items_found.append(item); newly_added_count += 1
                 logging.info(f"Aggiunti {newly_added_count} nuovi atti da pagina {page_count}. Totale: {len(all_items_found)}")

                 # Paginazione (se necessaria)
                 try:
                     next_button_locator = (By.LINK_TEXT, "Avanti"); next_button = driver.find_element(*next_button_locator)
                     parent_li = next_button.find_element(By.XPATH, "./.."); is_disabled = 'disabled' in parent_li.get_attribute('class')
                     if not is_disabled:
                         logging.info("Bottone 'Avanti' (iframe) attivo. Click..."); old_table_row = None
                         try: old_table_row = driver.find_element(By.CSS_SELECTOR, table_row_locator_css)
                         except NoSuchElementException: pass
                         logging.debug("Uso JS Click per 'Avanti'"); driver.execute_script("arguments[0].click();", next_button)
                         logging.info("Click 'Avanti' (iframe) OK. Attesa...");
                         if old_table_row:
                             try: wait.until(EC.staleness_of(old_table_row)); wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR,"table.master-detail-list-table"))); logging.info("Tabella iframe aggiornata.")
                             except TimeoutException: logging.warning("Timeout attesa aggiornamento iframe."); break
                         else: time.sleep(3)
                         page_count += 1
                     else: logging.info("Bottone 'Avanti' (iframe) disabilitato."); break
                 except NoSuchElementException: logging.info("Bottone 'Avanti' (iframe) non trovato."); break # Fine paginazione

             except TimeoutException: logging.error("Timeout attesa elementi iframe."); break
             except Exception as e: logging.error(f"Errore loop iframe: {e}"); break
        if page_count > max_pages_selenium: logging.warning(f"Raggiunto limite pagine Selenium {max_pages_selenium}.")

    finally:
        if driver: logging.info("Chiusura WebDriver Firefox..."); driver.quit()
    logging.info(f"--- Fine Fase Selenium: Trovati {len(all_items_found)} atti totali rilevanti. ---")

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
