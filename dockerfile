FROM maven
COPY ./pipelines/osm-poi/osm-parquetizer /opt/app
WORKDIR /opt/app
RUN mvn clean package
ADD target/osm-parquetizer-1.0.1-SNAPSHOT.jar osm-parquetizer.jar

ENTRYPOINT [ 'java', '-jar', 'osm-parquetizer.jar' ]