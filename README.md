## Uruchomienie Dockera z CouchDB

1. Przejdź do folderu `docker/`:
    ```bash
    cd docker
    ```

2. Uruchom kontener CouchDB:
    ```bash
    docker-compose up -d
    ```

3. Sprawdź, czy kontener działa:
    ```bash
    docker ps
    ```

4. Otwórz CouchDB w przeglądarce:
    ```
    http://localhost:5984/
    ```
   Login: `admin`  
   Hasło: `admin`

## Tworzenie baz danych

1. Wejdź do kontenera CouchDB
    ```bash
   docker exec -it docker-couchdb-1 bash
    ```
2. Utwórz bazy danych `_users`, `_replicator` oraz `_global_changes`:
    ```bash
    curl -X PUT http://admin:admin@127.0.0.1:5984/_users -H "Content-Type: application/json" -d '{}'
    curl -X PUT http://admin:admin@127.0.0.1:5984/_replicator -H "Content-Type: application/json" -d '{}'
    curl -X PUT http://admin:admin@127.0.0.1:5984/_global_changes -H "Content-Type: application/json" -d '{}'
    ```