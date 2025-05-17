# Inizio di src/etl_processor.py
import pandas as pd
from pathlib import Path
import logging

# Configurazione logging (simile allo scraper)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Percorsi
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
DOWNLOAD_DIR = PROJECT_ROOT / "data" / "downloaded_files"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed_data" 
OUTPUT_PARQUET = OUTPUT_DIR / "processed_pagamenti.parquet" 
# File di output (commentato o meno)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".ods"}

# --- DEFINIZIONI FUORI DAL CICLO ---
# Mappa dalle chiavi (minuscole, pulite) che CERCHIAMO negli header promossi
# ai nomi STANDARD che vogliamo usare
standard_columns_map = {
    'numero': 'NumeroMandato',
    'anno': 'Anno',
    'descrizione': 'DescrizioneMandato',
    'data': 'DataMandato',
    'cig': 'CIG',
    'nominativo': 'Beneficiario',
    'importo': 'ImportoEuro'
}

# Lista ORDINATA di TUTTE le colonne che vogliamo nel DataFrame finale
final_column_order = [
    'NumeroMandato',
    'Anno',
    'DataMandato',
    'CIG',
    'Beneficiario',
    'ImportoEuro',
    'DescrizioneMandato',
    'NomeFileOrigine'
]
# --- FINE DEFINIZIONI ---

def find_data_files(directory: Path) -> list[Path]:
    """Trova tutti i file con le estensioni consentite nella directory."""
    files = [f for f in directory.iterdir() if f.is_file() and f.suffix.lower() in ALLOWED_EXTENSIONS]
    logging.info(f"Trovati {len(files)} file dati in {directory}")
    return files

data_files = find_data_files(DOWNLOAD_DIR)
if not data_files:
    logging.warning("Nessun file dati trovato da processare. Uscita.")
    exit()

all_dataframes = [] # Lista per contenere i DataFrame puliti di ogni file

# --- CICLO PRINCIPALE SUI FILE ---

for file_path in data_files:
    logging.info(f"--- Processo il file: {file_path.name} ---")
    df = None
    header_correctly_identified = False # Flag per sapere se abbiamo un header valido

    # --- 1. LETTURA FILE (con logica header flessibile) ---
    try:
        # Tentativo 1: Leggi con header=0 (prima riga)
        logging.debug(f"  -> Tentativo lettura con header=0")
        df_read = None # Usiamo una variabile temporanea
        if file_path.suffix.lower() == '.ods':
            df_read = pd.read_excel(file_path, header=0, engine='odf')
        else:
             try: df_read = pd.read_excel(file_path, header=0)
             except Exception: df_read = pd.read_excel(file_path, header=0, engine='openpyxl')

        # Verifica se l'header trovato è 'sensato'
        expected_keywords = {'numero', 'anno', 'data', 'importo', 'nominativo', 'descrizione'}
        if df_read is not None and not df_read.empty:
            actual_headers_lower = {str(col).lower().strip() for col in df_read.columns}
            keywords_found = sum(any(key in hdr for hdr in actual_headers_lower) for key in expected_keywords)

            if keywords_found >= 3: # Soglia: almeno 3 keyword
                logging.info(f"  -> Header valido trovato alla riga 0 (lettura standard). Colonne: {df_read.columns.tolist()}")
                df = df_read # Assegna il DataFrame letto a quello principale
                header_correctly_identified = True # Imposta il flag
            else:
                logging.info(f"  -> Header riga 0 non sembra valido ({keywords_found} keyword trovate). Ritento lettura senza header e promozione riga dati 0.")
        else:
             logging.info(f"  -> Lettura con header=0 ha prodotto DataFrame vuoto o None.")


        # Tentativo 2 (SOLO se il Tentativo 1 non ha identificato l'header)
        if not header_correctly_identified:
            df_read_no_header = None
            if file_path.suffix.lower() == '.ods':
                 df_read_no_header = pd.read_excel(file_path, header=None, engine='odf')
            else:
                 try: df_read_no_header = pd.read_excel(file_path, header=None)
                 except Exception: df_read_no_header = pd.read_excel(file_path, header=None, engine='openpyxl')

            if df_read_no_header is not None and not df_read_no_header.empty and len(df_read_no_header) >= 2:
                 # L'HEADER VERO dovrebbe essere alla SECONDA riga letta (indice 1)
                 potential_header_row = df_read_no_header.iloc[1].astype(str).str.strip()
                 logging.info(f"  -> DEBUG: Riga 1 (potenziale header) RAW: {potential_header_row.tolist()}")

                 # Verifica se QUESTA riga (indice 1) è un header sensato
                 potential_header_values_lower = set(potential_header_row.tolist())
                 potential_header_values_lower = {str(val).lower().strip() for val in potential_header_values_lower}
                 logging.info(f"  -> DEBUG: Riga 1 (potenziale header) lower/strip: {potential_header_values_lower}")

                 keywords_found_row1 = len(expected_keywords.intersection(potential_header_values_lower))
                 logging.debug(f"  -> Controllo Riga 1: Valori trovati={potential_header_values_lower}, Keywords attese={expected_keywords}, Intersezione={keywords_found_row1}")

                 if keywords_found_row1 >= 3: # Stessa soglia
                     # Se la riga 1 è l'header, i DATI iniziano dalla riga 2
                     if len(df_read_no_header) > 2:
                         df = df_read_no_header[2:].reset_index(drop=True)
                         df.columns = potential_header_row # Usa la riga 1 come header
                         logging.info(f"  -> Usata Riga 1 (letta senza header) come header valido. Colonne: {df.columns.tolist()}")
                         header_correctly_identified = True # Imposta il flag
                     else:
                          logging.warning(f"  -> Riga 1 sembra un header valido, ma non ci sono dati dopo (solo 2 righe nel file).")
                          df = pd.DataFrame() # File vuoto
                 else:
                     logging.warning(f"  -> La riga 1 (letta senza header) non sembra un header valido ({keywords_found_row1} keyword trovate).")
                     df = pd.DataFrame()
            elif df_read_no_header is not None and not df_read_no_header.empty:
                 logging.warning(f"  -> File letto senza header ha meno di 2 righe. Impossibile identificare header.")
                 df = pd.DataFrame()
            else:
                 logging.warning(f"  -> Lettura con header=None ha prodotto DataFrame vuoto o None.")
                 df = pd.DataFrame()

    except Exception as e:
        logging.error(f"  -> ERRORE LETTURA file {file_path.name}: {e}", exc_info=True)
        df = pd.DataFrame() # Assicura che df sia vuoto in caso di errore lettura
        # continue # Potresti voler saltare, ma vediamo se la pulizia gestisce df vuoto

    # --- 2. PULIZIA E STANDARDIZZAZIONE ---
    try:
        # --- 2a. Controllo DataFrame Vuoto o Header non Identificato ---
        # Ora controlliamo ANCHE il flag 'header_correctly_identified'
        if df is None or df.empty or not header_correctly_identified:
            if not header_correctly_identified and (df is not None and not df.empty):
                 logging.warning(f"  -> Nessun header valido identificato. Salto pulizia.")
            else:
                 logging.warning(f"  -> DataFrame vuoto o lettura fallita. Salto pulizia.")
            continue # Salta al prossimo file
        

        # --- 2b. Rinominare Colonne Trovate ---
        current_columns_lower = {col.lower().strip(): col for col in df.columns}
        rename_map = {}
        matched_original_cols = set() # Per tracciare colonne già mappate

        for std_key, std_val in standard_columns_map.items():
            found_match_for_std_key = False
            # Cerca corrispondenza (più specifica: prima esatta, poi contenimento)
            original_col_to_map = None
            if std_key in current_columns_lower: # Corrispondenza esatta (dopo lower/strip)
                original_col_to_map = current_columns_lower[std_key]
            else: # Se non esatta, prova contenimento
                 for current_lower, current_original in current_columns_lower.items():
                      if std_key in current_lower and current_original not in matched_original_cols:
                          original_col_to_map = current_original
                          break # Prendi la prima corrispondenza non ancora usata

            if original_col_to_map and original_col_to_map not in matched_original_cols:
                 rename_map[original_col_to_map] = std_val
                 matched_original_cols.add(original_col_to_map)
                 found_match_for_std_key = True
            elif original_col_to_map and original_col_to_map in matched_original_cols:
                 logging.warning(f"  -> Colonna originale '{original_col_to_map}' già mappata, non può essere usata per '{std_key}'.")


        # Log colonne non mappate
        unmapped_cols = [orig for orig in df.columns if orig not in rename_map]
        if unmapped_cols:
            logging.debug(f"  -> Colonne originali non mappate a standard: {unmapped_cols}")

        df.rename(columns=rename_map, inplace=True)
        logging.info(f"  -> Colonne dopo rename: {df.columns.tolist()}")

        # --- 2c. Assicurare Struttura Finale e Aggiungere Info Origine ---
        df['NomeFileOrigine'] = file_path.name
        # Forza la struttura finale: seleziona, riordina, aggiunge colonne mancanti con NA
        df = df.reindex(columns=final_column_order)
        logging.info(f"  -> Struttura finale applicata. Colonne: {df.columns.tolist()}. Shape: {df.shape}")

        # --- 2d. Conversione Tipi (sulla struttura finale garantita) ---
        conversion_errors = []

        if 'ImportoEuro' in df.columns:
            logging.info(f"  -> Inizio pulizia e conversione 'ImportoEuro'...") # Log aggiunto

            # Applica una funzione di pulizia più robusta
            def safe_parse_float(value):
                if value is None:
                    logging.debug(f"DEBUG IMPORTO: valore None trovato")
                    return None
                if isinstance(value, (int, float)):
                    logging.debug(f"DEBUG IMPORTO: valore già numerico {value}")
                    return float(value) # Già numerico
                if not isinstance(value, str):
                    value = str(value) # Converti a stringa
                cleaned_text = value.replace(".", "").replace(",", ".").replace("€", "").strip()
                if cleaned_text == "":
                    logging.debug(f"DEBUG IMPORTO: stringa vuota trovata nell'importo originario: '{value}'")
                    return None
                try:
                    return float(cleaned_text)
                except Exception as e_conv:
                    logging.warning(f"DEBUG IMPORTO: errore conversione '{value}' -> '{cleaned_text}': {e_conv}")
                    return None
            df['ImportoEuro'] = df['ImportoEuro'].apply(safe_parse_float)

            # Controlla quanti NaN sono stati introdotti
            new_non_numeric = df['ImportoEuro'].isna().sum()
            # Calcola quanti erano NaN *prima* dell'apply (più complesso ora)
            # Potremmo contare quanti None ha restituito la funzione, ma per ora logghiamo solo il totale NaN
            if new_non_numeric > 0:
                 conversion_errors.append(f"ImportoEuro ({new_non_numeric} valori non convertiti in numero)")

            # Logga il tipo risultante PRIMA di salvare
            logging.info(f"  -> Tipo Dati 'ImportoEuro' dopo conversione: {df['ImportoEuro'].dtype}")

        # --- 2e. Rimozione Righe Inutili ---
        initial_rows = len(df)
        key_cols_for_dropna = ['NumeroMandato', 'ImportoEuro', 'Beneficiario']
        existing_key_cols = [col for col in key_cols_for_dropna if col in df.columns]

        if existing_key_cols:
            # Rimuovi solo se TUTTE le colonne chiave sono NaN/NaT/Vuote
            # Questo è meno aggressivo che rimuovere se ANCHE UNA SOLA è NaN
            # df.dropna(subset=existing_key_cols, how='all', inplace=True) # Opzione 1: Rimuovi se TUTTE sono NA
            df.dropna(subset=existing_key_cols, how='any', inplace=True) # Opzione 2 (come prima): Rimuovi se ALMENO UNA è NA
            rows_dropped = initial_rows - len(df)
            if rows_dropped > 0:
                logging.info(f"  -> Rimosse {rows_dropped} righe con NA in almeno una delle colonne chiave: {existing_key_cols}.")


        # --- Fine Pulizia per questo file ---
        if not df.empty:
             all_dataframes.append(df)
             logging.info(f"  -> DataFrame pulito aggiunto. Shape finale per questo file: {df.shape}")
        else:
             logging.warning(f"  -> DataFrame vuoto dopo pulizia/dropna. Non aggiunto.")

    except Exception as e:
        logging.error(f"  -> ERRORE PULIZIA file {file_path.name}: {e}", exc_info=True)

# --- FINE CICLO ---

# --- 3. UNIONE E SALVATAGGIO FINALE ---
if all_dataframes:
    logging.info(f"--- Unione di {len(all_dataframes)} DataFrame processati ---")
    try:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        logging.info(f"DataFrame finale creato. Shape totale: {final_df.shape}")

        # Ispezione finale (opzionale ma utile)
        logging.info("Info sul DataFrame finale:")
        final_df.info(verbose=True, show_counts=True) # Mostra info dettagliate

        # Controllo valori unici per colonne chiave (opzionale)
        logging.info(f"Valori unici Anno: {final_df['Anno'].unique().tolist()}")
        # logging.info(f"Valori CIG non vuoti trovati: {final_df[final_df['CIG'] != '']['CIG'].nunique()}")

        # Salvataggio in Parquet (consigliato per efficienza)
        #logging.info(f"Salvataggio DataFrame finale in: {OUTPUT_PARQUET}")
        #final_df.to_parquet(OUTPUT_PARQUET, index=False)
        #logging.info("Salvataggio completato.")

        # Salvataggio anche in CSV (opzionale, per ispezione facile)
        OUTPUT_CSV = OUTPUT_DIR / "processed_pagamenti.csv"
        logging.info(f"Salvataggio DataFrame finale in: {OUTPUT_CSV}")
        final_df.to_csv(OUTPUT_CSV, 
                        index=False, 
                        #decimal=',' ,
                        encoding='utf-8-sig') # utf-8-sig per Excel compatibility
        logging.info("Salvataggio CSV completato.")

    except Exception as e:
        logging.error(f"Errore durante l'unione o il salvataggio finale: {e}", exc_info=True)
else:
    logging.warning("Nessun DataFrame processato con successo. Nessun file finale creato.")

logging.info("--- Script ETL completato ---")