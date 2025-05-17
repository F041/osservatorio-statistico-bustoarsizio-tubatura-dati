# src/load_to_sqlite.py
import pandas as pd
import sqlite3
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
PROCESSED_CSV = PROJECT_ROOT / "data" / "processed_data" / "processed_pagamenti.csv"
DB_DIR = PROJECT_ROOT / "data" / "database"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "busto_pagamenti.db"
TABLE_NAME = "pagamenti"

logger.info("Leggendo il file CSV processato...")
try:
    # Leggi CSV, leggi ImportoEuro come stringa per pulizia manuale
    df = pd.read_csv(
        PROCESSED_CSV,
        parse_dates=['DataMandato'],
        dtype={'Anno': 'str', 'CIG': 'str', 'Beneficiario': 'str',
               'DescrizioneMandato': 'str', 'NumeroMandato': 'str', 'ImportoEuro': 'str'} # Leggi come stringa
    )

    # --- CORREZIONE LOGICA PARSING IMPORTO (per formato XXX.YY) ---
    logger.info("Pulizia e conversione colonna ImportoEuro...")
    # Rimuovi solo € e spazi. NON TOCCARE IL PUNTO. NON TOCCARE LA VIRGOLA (non dovrebbe esserci).
    df['ImportoEuro'] = df['ImportoEuro'].astype(str).str.replace('€', '', regex=False).str.strip()
    # Converti a numerico (float/REAL). Il punto è già il separatore decimale corretto.
    df['ImportoEuro'] = pd.to_numeric(df['ImportoEuro'], errors='coerce')
    # Controlla quanti valori non sono stati convertiti (diventati NaN)
    null_import_count = df['ImportoEuro'].isnull().sum()
    if null_import_count > 0:
         logger.warning(f"{null_import_count} valori in ImportoEuro non sono stati convertiti correttamente in numero.")

    # Converti Anno in intero nullable
    df['Anno'] = pd.to_numeric(df['Anno'], errors='coerce').astype('Int64')
    # Converti NumeroMandato in intero nullable
    df['NumeroMandato'] = pd.to_numeric(df['NumeroMandato'], errors='coerce').astype('Int64')

    logger.info("Tipi di dato dopo la conversione in Pandas:")
    df.info() # Verifica che ImportoEuro sia float64

except Exception as e:
    logger.error(f"Errore lettura/conversione CSV {PROCESSED_CSV}: {e}", exc_info=True)
    exit()

# --- Scrittura DB (come prima, usando stringhe per i tipi) ---
logger.info(f"Connessione al database SQLite: {DB_PATH}")
conn = None
try:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    logger.info(f"Scrittura dati nella tabella '{TABLE_NAME}' (sostituzione)...")
    dtype_sqlite_strings = {
        'NumeroMandato': 'INTEGER', 'Anno': 'INTEGER', 'DataMandato': 'TIMESTAMP',
        'CIG': 'TEXT', 'Beneficiario': 'TEXT', 'ImportoEuro': 'REAL', # Conferma REAL
        'DescrizioneMandato': 'TEXT', 'NomeFileOrigine': 'TEXT'
    }
    df.to_sql( TABLE_NAME, conn, if_exists='replace', index=False,
               dtype=dtype_sqlite_strings, chunksize=1000, method='multi')
    logger.info(f"Dati scritti con successo.")

    # --- Verifica Schema e Conteggio (come prima) ---
    cursor.execute(f"PRAGMA table_info({TABLE_NAME});")
    logger.info(f"Schema tabella '{TABLE_NAME}' creata:")
    for col in cursor.fetchall(): logger.info(f"  - Colonna: {col[1]}, Tipo SQLite: {col[2]}")
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    count = cursor.fetchone()[0]; logger.info(f"Verifica: la tabella contiene {count} righe.")

except Exception as e: logger.error(f"Errore scrittura DB: {e}", exc_info=True)
finally:
    if conn: conn.commit(); conn.close(); logger.info("Connessione DB chiusa.")

logger.info("--- Script caricamento SQLite completato ---")