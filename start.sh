#!/usr/bin/env bash
set -eo pipefail

export MAVEN_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8002"
mvn play2:run -Dhttp.port=8080
