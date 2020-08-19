FROM openjdk:11-jre
MAINTAINER frank <info@software.de>
EXPOSE 5050

ADD build/distributions/project1.tar /opt/micro-service

WORKDIR /opt/micro-service/project1
CMD ["./bin/project1", "-fg"]