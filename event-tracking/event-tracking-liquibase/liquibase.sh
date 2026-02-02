#!/usr/bin/env bash

CONTEXT_NAME=framework
EVENT_STORE_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

LIQUIBASE_COMMAND=update
#LIQUIBASE_COMMAND=dropAll

#fail script on error
set -e

function runEventTrackingLiquibase() {
    echo "Running event repository Liquibase"
    java -jar target/event-tracking-liquibase-${EVENT_STORE_VERSION}.jar --url=jdbc:postgresql://localhost:5432/${CONTEXT_NAME}viewstore --username=${CONTEXT_NAME} --password=${CONTEXT_NAME} --logLevel=info ${LIQUIBASE_COMMAND}
    echo "Finished running event repository liquibase"
}


runEventTrackingLiquibase