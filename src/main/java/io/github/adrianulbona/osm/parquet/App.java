package io.github.adrianulbona.osm.parquet;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.pbf2.v0_6.PbfReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.iq80.leveldb.*;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import static java.util.Collections.unmodifiableList;
import static org.openstreetmap.osmosis.core.domain.v0_6.EntityType.Node;
import static org.openstreetmap.osmosis.core.domain.v0_6.EntityType.Relation;

/**
 * Created by adrian.bona on 27/03/16.
 */
public class App {

    public static void main(String[] args) {
        final MultiEntitySinkConfig config = new MultiEntitySinkConfig();
        final CmdLineParser cmdLineParser = new CmdLineParser(config);

        try {
            cmdLineParser.parseArgument(args);
            final PbfReader reader = new PbfReader(config.getSource().toFile(), config.threads);
            final MultiEntitySink sink = new MultiEntitySink(config);
            sink.addObserver(new MultiEntitySinkObserver());
            reader.setSink(sink);
            reader.run();

            System.exit(0);
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            System.out.print("Usage: java -jar osm-parquetizer.jar");
            System.out.println();
            cmdLineParser.printSingleLineUsage(System.out);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private static class MultiEntitySinkConfig implements MultiEntitySink.Config {

        @Option(name = "--pbf-path", usage = "the OSM PBF file to be parquetized")
        private Path source = null;

        @Option(name = "--output-path", usage = "the directory where to store the Parquet files")
        private Path destinationFolder = null;

        @Option(name = "--continent", usage = "the continent of the OSM PBF file")
        private String continent = null;

        @Option(name = "--country", usage = "the country of the OSM PBF file")
        private String country = null;

        @Option(name = "--country_region", usage = "the country region of the OSM PBF file")
        private String countryRegion = null;

        @Option(name = "--pbf-threads", usage = "if present number of threads for PbfReader")
        private int threads = 1;

        @Option(name = "--exclude-metadata", usage = "if present the metadata will not be parquetized")
        private boolean excludeMetadata = false;

        @Option(name = "--no-nodes", usage = "if present the nodes will be not parquetized")
        private boolean noNodes = false;

        @Option(name = "--no-ways", usage = "if present the ways will be not parquetized")
        private boolean noWays = false;

        @Option(name = "--no-relations", usage = "if present the relations will not be parquetized")
        private boolean noRelations = false;

        @Override
        public boolean getExcludeMetadata() {
            return this.excludeMetadata;
        }

        @Override
        public Path getSource() {
            if (this.source != null) {
                return this.source;
            }

            String source = "tmp/kuwala/osm_files";

            if (this.continent != null) {
                source += ("/" + this.continent);
            }

            if (this.country != null) {
                source += ("/" + this.country);
            }

            if (this.countryRegion != null) {
                source += ("/" + this.countryRegion);
            }

            source += "/pbf/geo_fabrik.osm.pbf";

            return Path.of(source);
        }

        @Override
        public Path getDestinationFolder() {
            if (this.destinationFolder != null) {
                return this.destinationFolder;
            }

            String destination = "tmp/kuwala/osm_files";

            if (this.continent != null) {
                destination += ("/" + this.continent);
            }

            if (this.country != null) {
                destination += ("/" + this.country);
            }

            if (this.countryRegion != null) {
                destination += ("/" + this.countryRegion);
            }

            destination += "/parquet/osm_parquetizer";

            return Path.of(destination);
        }

        @Override
        public List<EntityType> entitiesToBeParquetized() {
            final List<EntityType> entityTypes = new ArrayList<>();
            if (!noNodes) {
                entityTypes.add(Node);
            }
            if (!noWays) {
                entityTypes.add(EntityType.Way);
            }
            if (!noRelations) {
                entityTypes.add(Relation);
            }
            return unmodifiableList(entityTypes);
        }
    }


    private static class MultiEntitySinkObserver implements MultiEntitySink.Observer {

        private static final Logger LOGGER = LoggerFactory.getLogger(MultiEntitySinkObserver.class);
        private AtomicLong totalEntitiesCount;
        private DB db;

        public MultiEntitySinkObserver() {
            Options options = new Options();
            options.createIfMissing(true);

            try {
                this.db = factory.open(new File(".leveldb"), options);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void started() {
            totalEntitiesCount = new AtomicLong();
        }

        @Override
        public void processed(Entity entity) {
            if (entity.getClass().getName().equals("org.openstreetmap.osmosis.core.domain.v0_6.Node")) {
                db.put(
                    bytes(String.valueOf(entity.getId())),
                    bytes(((org.openstreetmap.osmosis.core.domain.v0_6.Node) entity).getLongitude() +
                        "," +
                        ((org.openstreetmap.osmosis.core.domain.v0_6.Node) entity).getLatitude())
                );
            } else if (entity.getClass().getName().equals("org.openstreetmap.osmosis.core.domain.v0_6.Way")) {
                try {
                    db.close();
                    // TODO: Save entry that was skipped because of switching the leveldb connection
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            final long count = totalEntitiesCount.incrementAndGet();
            if (count % 1000000 == 0) {
                LOGGER.info("Entities processed: " + count);
            }
        }

        @Override
        public void ended() {
            try {
                FileUtils.deleteDirectory(new File(".leveldb"));
            } catch (IOException e) {
                e.printStackTrace();
            }

            LOGGER.info("Total entities processed: " + totalEntitiesCount.get());
        }
    }
}
