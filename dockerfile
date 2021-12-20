FROM maven:3.8.4-jdk-11-slim
COPY . /opt/app
WORKDIR /opt/app
RUN mvn clean package
