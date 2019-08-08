package com.latticeengines.graphdb;

import javax.annotation.PostConstruct;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    @Value("${graphdb.contact.url}")
    private String graphContactPoint;

    @Value("${graphdb.contact.port}")
    private Integer graphContactPort;

    private static final String GREMLIN_DRIVER_SERIALIZER //
            = Serializers.GRAPHSON_V2D0.name();

    private ThreadLocal<Cluster> thCluster;

    @PostConstruct
    public void initializeBean() {
        thCluster = new ThreadLocal<>();
    }

    public Cluster initThreadLocalCluster() {
        Cluster existingCluster = thCluster.get();
        if (existingCluster != null) {
            thCluster.set(null);
            closeCluster(existingCluster);
        }
        thCluster.set(createNewCluster());
        return thCluster.get();
    }

    public void cleanupThreadLocalCluster() {
        Cluster existingCluster = thCluster.get();
        if (existingCluster != null) {
            thCluster.set(null);
            closeCluster(existingCluster);
        }
    }

    public Cluster initCluster() {
        if (thCluster.get() == null) {
            return createNewCluster();
        } else {
            return thCluster.get();
        }
    }

    private Cluster createNewCluster() {
        Cluster.Builder builder = Cluster.build();
        builder.addContactPoint(graphContactPoint);
        builder.port(graphContactPort);
        builder.serializer(GREMLIN_DRIVER_SERIALIZER);
        builder.minConnectionPoolSize(1);
        log.info("Initiated graph connection pool cluster");
        return builder.create();
    }

    public GraphTraversalSource initTraversalSource(Cluster cluster) {
        return EmptyGraph.instance().traversal().withRemote(DriverRemoteConnection.using(cluster));
    }

    public void closeCluster(Cluster cluster) {
        if (cluster != thCluster.get() && isNotBadCluster(cluster)) {
            cluster.close();
        }
    }

    public boolean isNotBadCluster(Cluster cluster) {
        return cluster != null && !cluster.isClosed() && !cluster.isClosing();
    }
}
