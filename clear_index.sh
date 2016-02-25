#!/bin/bash

if [ $1 ]
    then
        curl http://localhost:8983/solr/$1/update --data '<delete><query>*:*</query></delete>' -H 'Content-type:text/xml; charset=utf-8'
        wait ${!}
        curl http://localhost:8983/solr/$1/update --data '<commit />' -H 'Content-type:text/xml; charset=utf-8'
        wait ${!}
        echo "Solr index for core $1 cleared."
    else
        echo "No Solr core specified."
        echo "Example usage: clear_index.sh core1"
fi
