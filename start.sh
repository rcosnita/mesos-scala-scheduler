#!/usr/bin/env bash
set -eo pipefail

export MAVEN_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8002"
export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos-1.6.1.dylib
mvn play2:run -Dhttp.port=8080
