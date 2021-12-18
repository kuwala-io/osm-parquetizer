FROM maven:3.8.4-jdk-11-slim
COPY ./pipelines/osm-poi/osm-parquetizer /opt/app
WORKDIR /opt/app
RUN mvn clean package
