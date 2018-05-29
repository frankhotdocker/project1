FROM openjdk:8-jre
MAINTAINER frank <info@scoop-software.de>
EXPOSE 5050
COPY build/distributions/*.zip /opt/micro-service/
RUN unzip /opt/micro-service/*.zip -d /opt/micro-service

WORKDIR /opt/micro-service/project1
CMD ["./bin/project1", "-fg"]