FROM maven:openjdk

COPY ./src /opt/app/src
COPY ./pom.xml /opt/app/pom.xml

WORKDIR /opt/app

RUN mvn clean package