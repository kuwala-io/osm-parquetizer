package io.github.adrianulbona.osm.parquet.convertor;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.iq80.leveldb.DB;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.slf4j.Logger;
import java.io.File;
import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.iq80.leveldb.*;

import static org.apache.parquet.schema.Type.Repetition.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import static java.util.stream.IntStream.range;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * Created by adrian.bona on 26/03/16.
 */
public class WayWriteSupport extends OsmEntityWriteSupport<Way> {

    private final PrimitiveType nodeIndexType;
    private final PrimitiveType nodeIdType;
    private final PrimitiveType latType;
    private final PrimitiveType longType;
    private final GroupType nodes;
    private DB db = null;

    public WayWriteSupport(boolean excludeMetadata) {
        super(excludeMetadata);
        nodeIndexType = new PrimitiveType(REQUIRED, INT32, "index");
        nodeIdType = new PrimitiveType(REQUIRED, INT64, "nodeId");
        latType = new PrimitiveType(OPTIONAL, DOUBLE, "latitude");
        longType = new PrimitiveType(OPTIONAL, DOUBLE, "longitude");
        nodes = new GroupType(REPEATED, "nodes", nodeIndexType, nodeIdType, latType, longType);
    }

    @Override
    protected MessageType getSchema() {
        final List<Type> attributes = new ArrayList<>(getCommonAttributes());
        attributes.add(nodes);
        return new MessageType("way", attributes);
    }

    @Override
    protected void writeSpecificFields(Way record, int nextAvailableIndex) {
        if (db == null) {
            Options options = new Options();
            options.createIfMissing(true);

            try {
                db = factory.open(new File(".leveldb"), options);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (OverlappingFileLockException ignored) {}
        }

        final List<WayNode> wayNodes = record.getWayNodes();
        final Map<Integer,WayNode> indexedNodes = new HashMap<>();
        range(0, wayNodes.size()).forEach(index -> indexedNodes.put(index, wayNodes.get(index)));

        if (!indexedNodes.isEmpty()) {
            recordConsumer.startField(nodes.getName(), nextAvailableIndex);

            indexedNodes.forEach((index, wayNode) -> {
                String[] coords = db != null ?
                        asString(db.get(bytes(String.valueOf(wayNode.getNodeId())))).split(",") :
                        null;
                recordConsumer.startGroup();

                // Node Index
                recordConsumer.startField(nodeIndexType.getName(), 0);
                recordConsumer.addInteger(index);
                recordConsumer.endField(nodeIndexType.getName(), 0);

                // Node Id
                recordConsumer.startField(nodeIdType.getName(), 1);
                recordConsumer.addLong(wayNode.getNodeId());
                recordConsumer.endField(nodeIdType.getName(), 1);

                if (coords != null) {
                    // Node latitude
                    recordConsumer.startField(latType.getName(), 2);
                    recordConsumer.addDouble(Double.parseDouble(coords[1]));
                    recordConsumer.endField(latType.getName(), 2);

                    // Node longitude
                    recordConsumer.startField(longType.getName(), 3);
                    recordConsumer.addDouble(Double.parseDouble(coords[0]));
                    recordConsumer.endField(longType.getName(), 3);
                }

                recordConsumer.endGroup();
            });

            recordConsumer.endField(nodes.getName(), nextAvailableIndex);
        }
    }
}
