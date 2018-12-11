FROM openjdk:8-jre
MAINTAINER frank <info@scoop-software.de>
EXPOSE 5050

ADD build/distributions/pipelineFH.tar /opt/micro-service

WORKDIR /opt/micro-service/project1
CMD ["./bin/project1", "-fg"]