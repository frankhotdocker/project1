FROM openjdk:8-jre-alpine
MAINTAINER Greg Whitaker <greg@netifi.com>
EXPOSE 5050
COPY build/distributions/ratpack-docker-shadow.zip /opt/ratpack-docker/
RUN unzip /opt/ratpack-docker/ratpack-docker-shadow.zip -d /opt/ratpack-docker
WORKDIR /opt/ratpack-docker/ratpack-docker-shadow
CMD ["./bin/ratpack-docker", "-fg"]