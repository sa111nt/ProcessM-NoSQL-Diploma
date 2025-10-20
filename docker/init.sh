#!/bin/sh
# wait-for-couchdb.sh
echo "Waiting for CouchDB to start..."
until curl -s http://admin:admin@127.0.0.1:5984/; do
  sleep 1
done

echo "Creating system databases..."
curl -X PUT http://admin:admin@127.0.0.1:5984/_users
curl -X PUT http://admin:admin@127.0.0.1:5984/_replicator
curl -X PUT http://admin:admin@127.0.0.1:5984/_global_changes