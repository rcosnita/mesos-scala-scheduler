This project provides a very simple Mesos framework (written in Scala) which schedules shell commands
on a regular basis. The project is inspired from the excellent book: "Building Applications on Mesos" -> https://www.oreilly.com/library/view/building-applications-on/9781491926543/ch04.html.

# Getting started

The steps in this section were tested on Mac OS X.

```bash
git clone ... .
mkdir -p /usr/local/sbin || true
brew install mesos

cd mesos-dummy-scheduler
mvn clean install
./start-mesos.sh master # terminal 1
./start-mesos.sh slave # terminal 2
./start.sh # terminal 3
open http://localhost:12000 # terminal 3
curl -v -XPOST http://localhost:8080/start
```

# References

* [Building Applications on Mesos - by David Greenberg](https://www.oreilly.com/library/view/building-applications-on/9781491926543/ch04.html)