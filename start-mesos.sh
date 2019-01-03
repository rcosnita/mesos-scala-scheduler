#!/usr/bin/env bash
set -eo pipefail

MESOS_LAYER=${1}

mkdir -p mesos/master || true
mkdir -p mesos/slave || true

start_master() {
    mesos-master --ip=127.0.0.1 --port=12000 --work_dir=$(pwd)/mesos/master
}

start_slave() {
    mesos-slave --master=127.0.0.1:12000 --port=12001 --work_dir=$(pwd)/mesos/slave
}

case ${MESOS_LAYER} in
    master)
        start_master
        ;;
    slave)
        start_slave
        ;;
    *)
        echo "The mesos layer argument is mandatory and must be set to **master** or **slave**"
        exit -1
        ;;
esac