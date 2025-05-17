# Pipeline Dati Statistici Busto Arsizio (`busto-arsizio-data-pipeline`)

## A Cosa Serve Questo Progetto? (Spiegato Semplice)

Immaginate di avere un compito importante: ogni mese, dovete andare a raccogliere dei dati specifici dal sito del Comune di Busto Arsizio, pulirli un po' e poi salvare questi dati aggiornati in un "archivio" digitale (un file chiamato database). Questo archivio serve poi ad un altro progetto (chiamato `esplora-pagamenti-busto-arsizio`) per mostrare questi dati in modo facile da consultare.

Fare tutto questo a mano ogni mese può essere noioso e si rischia di dimenticarsene.

Questo progetto, `busto-arsizio-data-pipeline`, fa esattamente questo lavoro in **automatico**:
1.  **Raccoglie i dati**: Va sul sito del Comune e scarica le informazioni necessarie (come un piccolo robot).
2.  **Li sistema**: Pulisce e organizza questi dati.
3.  **Crea l'archivio aggiornato**: Salva i dati puliti in un file database (chiamato `busto_pagamenti.db`).
4.  **Pubblica l'archivio**: Mette questo file database aggiornato a disposizione su GitHub (in una sezione chiamata "Releases"), pronto per essere usato dall'altro progetto.

Tutto questo avviene automaticamente una volta al mese, grazie a un sistema chiamato "GitHub Actions".

## Come Funziona (in Breve)?

Questo progetto contiene una serie di istruzioni (script) che dicono al computer cosa fare, passo dopo passo:
1.  `scraper.py`: Si occupa di "raschiare" (scaricare) i dati grezzi dal sito web.
2.  `etl_processor.py`: Prende i dati grezzi, li trasforma e li pulisce, preparandoli per l'archivio.
3.  `verify_etl.py`: Fa un controllo per assicurarsi che i dati siano stati processati correttamente.
4.  `db_handler.py`: Prende i dati puliti e li carica nel file database `busto_pagamenti.db`.

Una volta al mese (o quando viene avviato manualmente), il sistema GitHub Actions esegue questi script in ordine e, se tutto va bene, crea una nuova pubblicazione ("Release") con il database aggiornato.

## Per Chi Sviluppa o Vuole Capire di Più

*   **Tecnologie Usate**: Principalmente Python, con librerie come `requests`, `BeautifulSoup`, `selenium` per lo scraping, e `pandas` per la manipolazione dei dati.
*   **Automazione**: GitHub Actions orchestra l'esecuzione mensile degli script e la creazione delle release.
*   **Output**: L'artefatto principale è un file database SQLite (`busto_pagamenti.db` o simile) che viene allegato a ogni release.
*   **Consumatore**: Il database prodotto è pensato per essere utilizzato dal repository `F041/esplora-pagamenti-busto-arsizio`.

## Come Contribuire o Segnalare Problemi

Se notate problemi o avete suggerimenti, potete aprire una "Issue" in questo repository.

---