
import pandas as pd
from pathlib import Path
import logging
import sys # Per uscire in caso di errori critici

# --- Configurazione Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Percorsi ---
try:
    PROJECT_ROOT = Path(__file__).parent.parent.resolve()
    DOWNLOAD_DIR = PROJECT_ROOT / "data" / "downloaded_files"
    PROCESSED_CSV = PROJECT_ROOT / "data" / "processed_data" / "processed_pagamenti.csv" # Percorso corretto
    ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".ods"}
except NameError:
    # Se __file__ non è definito (es. eseguito in un notebook interattivo senza salvare)
    PROJECT_ROOT = Path('.').resolve() # Usa la directory corrente
    DOWNLOAD_DIR = PROJECT_ROOT / "data" / "downloaded_files"
    PROCESSED_CSV = PROJECT_ROOT / "data" / "processed_data" / "processed_pagamenti.csv"
    ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".ods"}
    logging.warning(f"__file__ non definito, PROJECT_ROOT impostato su: {PROJECT_ROOT}")


# --- Funzione Helper per Trovare File ---
def find_data_files(directory: Path, extensions: set) -> list[Path]:
    """Trova tutti i file con le estensioni consentite nella directory."""
    if not directory.is_dir():
        logging.error(f"Directory non trovata: {directory}")
        return []
    files = [f for f in directory.iterdir() if f.is_file() and f.suffix.lower() in extensions]
    logging.info(f"Trovati {len(files)} file dati in {directory} con estensioni {extensions}")
    return files

# --- Funzione Principale di Verifica ---
def verify_row_counts():
    """
    Verifica la corrispondenza (approssimativa) del numero di righe
    tra i file originali e il file CSV processato.
    """
    logging.info("--- Inizio Verifica Conteggio Righe ---")

    # 1. Leggi il file CSV processato e calcola i conteggi per file
    if not PROCESSED_CSV.exists():
        logging.error(f"File processato non trovato: {PROCESSED_CSV}. Impossibile verificare.")
        sys.exit(1) # Esce dallo script con codice di errore

    try:
        # Leggi solo le colonne necessarie per efficienza, se possibile
        # Assumiamo che 'NomeFileOrigine' sia sempre presente
        df_proc = pd.read_csv(PROCESSED_CSV, usecols=['NomeFileOrigine'])
        logging.info(f"File processato '{PROCESSED_CSV.name}' caricato.")
    except Exception as e:
        logging.error(f"Errore durante la lettura di {PROCESSED_CSV}: {e}", exc_info=True)
        sys.exit(1)

    if 'NomeFileOrigine' not in df_proc.columns:
         logging.error(f"Colonna 'NomeFileOrigine' mancante in {PROCESSED_CSV}. Impossibile verificare.")
         sys.exit(1)

    processed_counts = df_proc.groupby('NomeFileOrigine').size()
    logging.info(f"Calcolati conteggi per {len(processed_counts)} file dal CSV processato.")
    # print("\nConteggi dal file processato:")
    # print(processed_counts)

    # 2. Trova e leggi i file originali per ottenere i conteggi grezzi
    raw_files = find_data_files(DOWNLOAD_DIR, ALLOWED_EXTENSIONS)
    if not raw_files:
        logging.error(f"Nessun file dati originale trovato in {DOWNLOAD_DIR}. Verifica interrotta.")
        sys.exit(1)

    raw_counts = {}
    logging.info("Inizio lettura file originali per conteggio grezzo...")

    # Definisci le keyword qui, come nell'ETL
    expected_keywords = {'numero', 'anno', 'data', 'importo', 'nominativo', 'descrizione'}

    for file_path in raw_files:
        raw_count = 0 # Inizializza conteggio per questo file
        df_raw = None
        try:
            # --- INIZIO LOGICA HEADER VERIFICA (simile a ETL) ---
            header_found_at = None # Dove abbiamo trovato l'header (0 o 1)

            # Tentativo 1: Leggi con header=0
            try:
                if file_path.suffix.lower() == '.ods':
                    df_raw_h0 = pd.read_excel(file_path, header=0, engine='odf')
                else:
                    try: df_raw_h0 = pd.read_excel(file_path, header=0)
                    except Exception: df_raw_h0 = pd.read_excel(file_path, header=0, engine='openpyxl')

                if df_raw_h0 is not None and not df_raw_h0.empty:
                    actual_headers_lower = {str(col).lower().strip() for col in df_raw_h0.columns}
                    keywords_found = sum(any(key in hdr for hdr in actual_headers_lower) for key in expected_keywords)
                    if keywords_found >= 3:
                        header_found_at = 0
                        df_raw = df_raw_h0 # Usiamo questo DataFrame
                        logging.debug(f"  -> {file_path.name}: Header valido trovato alla riga 0.")
            except Exception as e_h0:
                 logging.debug(f"  -> {file_path.name}: Errore lettura con header=0: {e_h0}")
                 pass # Ignora errori qui, proveremo l'altro metodo

            # Tentativo 2: Leggi con header=1 (SOLO se header non trovato a 0)
            if header_found_at is None:
                 try:
                      if file_path.suffix.lower() == '.ods':
                           df_raw_h1 = pd.read_excel(file_path, header=1, engine='odf')
                      else:
                           try: df_raw_h1 = pd.read_excel(file_path, header=1)
                           except Exception: df_raw_h1 = pd.read_excel(file_path, header=1, engine='openpyxl')

                      # Verifica semplice: se ha colonne, assumiamo sia l'header corretto per questi file
                      if df_raw_h1 is not None and not df_raw_h1.empty and len(df_raw_h1.columns) > 0:
                            # NON verifichiamo le keyword qui, assumiamo che se header=0 fallisce, header=1 sia quello giusto
                            header_found_at = 1
                            df_raw = df_raw_h1 # Usiamo questo DataFrame
                            logging.debug(f"  -> {file_path.name}: Assunto header valido alla riga 1.")

                 except Exception as e_h1:
                      # Potrebbe fallire per file corti, gestito dopo
                      logging.debug(f"  -> {file_path.name}: Errore/Impossibile leggere con header=1: {e_h1}")
                      pass

            # --- FINE LOGICA HEADER VERIFICA ---

            # Calcola il conteggio BASATO sul DataFrame letto correttamente
            if df_raw is not None:
                raw_count = len(df_raw)
            else:
                 # Se entrambi i tentativi falliscono (es. file troppo corto per header=1)
                 logging.warning(f"  -> {file_path.name}: Impossibile determinare header/conteggio grezzo. Impostato a 0.")
                 raw_count = 0

            raw_counts[file_path.name] = raw_count
            logging.debug(f"  -> {file_path.name}: Conteggio grezzo finale = {raw_count} (header trovato a: {header_found_at})")

        except Exception as e:
            logging.error(f"  -> Errore lettura/conteggio file originale {file_path.name}: {e}. File saltato.")
            # Non aggiungiamo a raw_counts se fallisce gravemente

    logging.info(f"Calcolati conteggi grezzi per {len(raw_counts)} file originali.")

    # 3. Confronta i set di file e i conteggi
    processed_filenames = set(processed_counts.index)
    raw_filenames = set(raw_counts.keys())

    files_solo_in_proc = processed_filenames - raw_filenames
    files_solo_in_raw = raw_filenames - processed_filenames
    files_comuni = processed_filenames.intersection(raw_filenames)

    if files_solo_in_proc:
        logging.warning(f"ATTENZIONE: File presenti nel CSV processato ma non trovati/letti nella cartella originale: {files_solo_in_proc}")
    if files_solo_in_raw:
        logging.warning(f"ATTENZIONE: File trovati/letti nella cartella originale ma mancanti nel CSV processato (errore ETL?): {files_solo_in_raw}")

    logging.info(f"\n--- Confronto Conteggi Righe (per {len(files_comuni)} file comuni) ---")
    discrepanze = 0
    problemi_gravi = 0

    # Colonne per report tabellare
    print(f"{'Nome File':<50} {'Righe Grezze':>15} {'Righe Processate':>18} {'Differenza':>12} {'Stato'}")
    print("-" * 100)

    for filename in sorted(list(files_comuni)):
        raw_count = raw_counts.get(filename, -1) # Usa get per sicurezza
        proc_count = processed_counts.get(filename, -1)
        differenza = raw_count - proc_count

        status = "OK"
        if differenza < 0:
            status = "ERRORE (Proc > Raw!)"
            discrepanze += 1
            problemi_gravi += 1
        elif proc_count == 0 and raw_count > 0:
             status = "WARN (0 righe proc!)"
             discrepanze += 1
        elif differenza > raw_count * 0.1 and differenza > 10 : # Esempio: se differenza > 10% e > 10 righe
             status = "WARN (Diff > 10%)"
             discrepanze += 1
        elif differenza > 0:
            status = "OK (ETL drop)" # Differenza positiva attesa per dropna

        print(f"{filename:<50} {raw_count:>15} {proc_count:>18} {differenza:>12} {status}")


    print("-" * 100)
    logging.info(f"--- Fine Confronto Conteggi ---")
    if problemi_gravi > 0:
         logging.error(f"RISULTATO VERIFICA: {problemi_gravi} problemi gravi rilevati (Proc > Raw o 0 righe proc).")
    elif discrepanze > 0:
         logging.warning(f"RISULTATO VERIFICA: {discrepanze} discrepanze rilevate (differenze significative o file mancanti). Controllare i WARNING.")
    else:
         logging.info("RISULTATO VERIFICA: OK. I conteggi sembrano coerenti.")

    # Restituisce True se non ci sono problemi gravi, False altrimenti
    return problemi_gravi == 0

def count_importo_zero():
    """
    Conta quante righe nel file processed_pagamenti.csv hanno ImportoEuro esattamente pari a 0.
    """
    if not PROCESSED_CSV.exists():
        logging.error(f"File processato non trovato: {PROCESSED_CSV}. Impossibile contare.")
        return None
    try:
        df = pd.read_csv(PROCESSED_CSV, usecols=["ImportoEuro"])
        # Gestione robusta: considera sia 0 numerico che stringa '0' o '0.0'
        count_zero = df["ImportoEuro"].apply(lambda x: float(str(x).replace(",",".").replace("€","").strip()) if str(x).strip() not in ["", "nan", "None"] else None).fillna(999999999).eq(0).sum()
        print(f"Numero di righe con ImportoEuro = 0: {count_zero}")
        return count_zero
    except Exception as e:
        logging.error(f"Errore durante il conteggio ImportoEuro=0: {e}")
        return None

# --- Esecuzione ---
if __name__ == "__main__":
    logging.info("--- Avvio verifica ETL ---")
    esito = verify_row_counts()
    if not esito:
        logging.error("Verifica conteggio righe fallita. Uscita anticipata.")
        sys.exit(1)
    else:
        logging.info("Verifica conteggio righe superata. Ora controllo ImportoEuro zero...")
        try:
            count_importo_zero()
        except Exception as e:
            logging.error(f"Errore durante il conteggio ImportoEuro zero: {e}")
            sys.exit(1)
    logging.info("--- Verifica ETL completata ---")
