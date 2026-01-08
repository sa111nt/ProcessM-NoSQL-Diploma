#!/bin/bash
# ^^^ SHEBANG: Mówi systemowi Linux, że ten plik ma być wykonany przez powłokę Bash.
# WAŻNE: Plik musi mieć format końców linii LF (Unix), a nie CRLF (Windows)!

# Definiujemy adres URL do CouchDB z loginem i hasłem.
# Dzięki temu nie musimy wpisywać "http://admin:admin..." w każdej linijce.
COUCH_URL="http://admin:admin@127.0.0.1:5984"

echo "⏳ Waiting for CouchDB to start..."

# --- PĘTLA OCZEKUJĄCA NA START BAZY ---
# Docker uruchamia ten skrypt równolegle ze startem CouchDB.
# Musimy poczekać, aż baza będzie gotowa na przyjęcie połączeń.
# curl -s : tryb cichy (silent), nie pokazuj paska postępu.
# -o /dev/null : ignoruj treść odpowiedzi (nie interesuje nas HTML, tylko kod statusu).
# -w "%{http_code}" : wypisz TYLKO kod HTTP (np. 200, 404, 500).
until [ "$(curl -s -o /dev/null -w "%{http_code}" $COUCH_URL)" -eq "200" ]; do
  # Jeśli kod nie jest 200, czekamy 1 sekundę i sprawdzamy znowu.
  sleep 1
done

echo "✅ CouchDB is up! Configuring system..."

# --- TWORZENIE BAZ SYSTEMOWYCH ---
# CouchDB wymaga tych baz do działania (użytkownicy, replikacje).
# Domyślnie tworzy je leniwie, ale wymuszenie ich teraz zapobiega błędom w logach.
# "|| true" oznacza: "Jeśli komenda się nie uda (bo baza już istnieje), nie przerywaj skryptu".
curl -X PUT $COUCH_URL/_users || true
curl -X PUT $COUCH_URL/_replicator || true
curl -X PUT $COUCH_URL/_global_changes || true

echo "🚀 Applying AGGRESSIVE performance tuning..."

# --- SEKJA OPTYMALIZACJI (TUNING) ---
# To tutaj dzieje się magia przyspieszająca import z minut do sekund.

# 1. Delayed Commits (Opóźniony zapis)
# Domyślnie: false (CouchDB zapisuje na dysk po każdym requescie - bezpieczne, ale wolne).
# Zmiana na "true": CouchDB trzyma dane w RAM i zrzuca na dysk co parę sekund.
# ZYSK: Drastyczne zmniejszenie operacji I/O (Input/Output) dysku.
curl -X PUT $COUCH_URL/_node/_local/_config/couchdb/delayed_commits -d '"true"'

# 2. Zwiększenie limitu wielkości requestu HTTP
# Domyślnie limit jest mały. Przy imporcie "bulk" wysyłamy duże paczki JSON.
# Ustawiamy na ~4GB (4294967296 bajtów), żeby uniknąć błędu "413 Request Entity Too Large".
curl -X PUT $COUCH_URL/_node/_local/_config/httpd/max_http_request_size -d '"4294967296"'

# 3. Zmniejszenie poziomu logowania
# Domyślnie: "info" (loguje każde zapytanie HTTP).
# Zmiana na "warn": Loguje tylko błędy i ostrzeżenia.
# ZYSK: Procesor nie traci czasu na wypisywanie tekstu do konsoli/pliku, a Docker nie puchnie od logów.
curl -X PUT $COUCH_URL/_node/_local/_config/log/level -d '"warn"'

# --- USTAWIENIA KLASTRA (Dla jednego węzła) ---

# 4. Liczba replik (n)
# Domyślnie: 3 (CouchDB próbuje trzymać 3 kopie każdego dokumentu dla bezpieczeństwa).
# Zmiana na "1": Mamy tylko jeden serwer (kontener), więc 3 kopie to strata miejsca i CPU.
# ZYSK: 3x mniej operacji zapisu dla procesora.
curl -X PUT $COUCH_URL/_node/_local/_config/cluster/n -d '"1"'

# 5. Liczba shardów (q)
# Shardy to kawałki, na które dzielona jest baza. Pozwala to na wielowątkowość.
# Ustawienie "4" jest optymalne dla procesorów 4-8 rdzeniowych.
# Zbyt mało (1) = nie wykorzystasz wszystkich rdzeni. Zbyt dużo (16) = narzut na zarządzanie.
curl -X PUT $COUCH_URL/_node/_local/_config/cluster/q -d '"4"'

echo "CouchDB configuration complete! Ready for import."