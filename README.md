# PQL Process Mining Engine

Silnik Process Mining oparty na bazie dokumentowej NoSQL (CouchDB), obsługujący zapytania w języku Process Query Language (PQL). Projekt zawiera wbudowany serwer REST API oraz przeglądarkowy interfejs graficzny (GUI).

## 1. Uruchomienie bazy CouchDB (Docker)

1. Przejdź do folderu `docker/`:
    ```bash
    cd docker
    ```

2. Uruchom kontener CouchDB w tle:
    ```bash
    docker-compose up -d
    ```

3. Sprawdź, czy kontener działa poprawnie:
    ```bash
    docker ps
    ```

4. Otwórz panel zarządzania CouchDB (Fauxton) w przeglądarce:
   - **URL:** http://localhost:5984/_utils/#login
   - **Login:** `admin`
   - **Hasło:** `admin`

---

## 2. Tworzenie i optymalizacja bazy danych

Aby zapewnić optymalną wydajność dla wielkich logów zdarzeń, należy wstępnie skonfigurować bazę.

1. Wejdź do terminala kontenera CouchDB:
    ```bash
    docker exec -it processm_couchdb bash
    ```

2. Utwórz wymagane bazy systemowe (`_users`, `_replicator`, `_global_changes`):
    ```bash
    curl -X PUT [http://admin:admin@127.0.0.1:5984/_users](http://admin:admin@127.0.0.1:5984/_users) -H "Content-Type: application/json" -d '{}'
    curl -X PUT [http://admin:admin@127.0.0.1:5984/_replicator](http://admin:admin@127.0.0.1:5984/_replicator) -H "Content-Type: application/json" -d '{}'
    curl -X PUT [http://admin:admin@127.0.0.1:5984/_global_changes](http://admin:admin@127.0.0.1:5984/_global_changes) -H "Content-Type: application/json" -d '{}'
    ```

3. Zastosuj optymalizacje dla dużych zapytań (np. wyłączenie opóźnionych commitów i zwiększenie limitu HTTP):
    ```bash
    curl -X PUT [http://admin:admin@127.0.0.1:5984/_node/_local/_config/couchdb/delayed_commits](http://admin:admin@127.0.0.1:5984/_node/_local/_config/couchdb/delayed_commits) -d '"true"'
    curl -X PUT [http://admin:admin@127.0.0.1:5984/_node/_local/_config/httpd/max_http_request_size](http://admin:admin@127.0.0.1:5984/_node/_local/_config/httpd/max_http_request_size) -d '"4294967296"'
    ```

4. *(Opcjonalnie)* Usunięcie głównej bazy logów (reset danych):
    ```bash
    curl -X DELETE "http://localhost:5984/event_logs" -u "admin:admin"
    ```

---

## 3. Uruchomienie aplikacji (Serwer PQL)

System działa jako mikroserwis Spring Boot. Uruchamia on silnik tłumaczący PQL, łączy się z bazą CouchDB i wystawia interfejs webowy.

1. W środowisku IDE (np. IntelliJ IDEA) uruchom główną klasę aplikacji:
   `PqlServerApplication.kt`
   *(Alternatywnie z terminala: `./gradlew bootRun`)*
2. Po pojawieniu się w konsoli komunikatu `🚀 Serwer PQL REST API uruchomiony na porcie 8080!`, aplikacja jest gotowa do pracy.

---

## 4. Instrukcja obsługi: Przeglądarkowe GUI

Aplikacja nie wymaga korzystania z konsoli – posiada wbudowany interfejs webowy do pełnej obsługi cyklu Process Miningu.

1. Otwórz przeglądarkę i wejdź na adres: **http://localhost:8080/**
2. **Import logu:** W sekcji nr 1 wybierz plik z dysku (`.xes`, `.gz`, `.zip`) i kliknij "Importuj Log". System automatycznie zbuduje w bazie płaską strukturę JSON oraz założy wydajne indeksy B-Tree.
3. **Zapytania PQL:** W sekcji nr 2 wpisz w pole tekstowe natywne zapytanie PQL, np.:
   ```pql
   select e:name, max(e:total) where l:name='JournalReview' group by e:name