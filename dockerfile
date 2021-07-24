FROM maven
COPY ./pipelines/osm-poi/osm-parquetizer /opt/app
WORKDIR /opt/app
RUN mvn clean package
