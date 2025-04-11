#!/usr/bin/env bash

CONTEXT_NAME=framework
EVENT_STORE_VERSION=17.102.3-SNAPSHOT

#fail script on error
set -e

LIQUIBASE_COMMAND=update
#LIQUIBASE_COMMAND=dropAll

function runEventBufferLiquibase() {
    echo "running EventBufferLiquibase"
    java -jar target/event-buffer-liquibase-${EVENT_STORE_VERSION}.jar --url=jdbc:postgresql://localhost:5432/${CONTEXT_NAME}viewstore --username=${CONTEXT_NAME} --password=${CONTEXT_NAME} --logLevel=info ${LIQUIBASE_COMMAND}
    echo "Finished EventBufferLiquibase"
}


runEventBufferLiquibase