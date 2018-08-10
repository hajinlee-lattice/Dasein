package com.latticeengines.graph;

import javax.annotation.PostConstruct;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.graph.NameSpaceUtil;

@Component
public class GraphUtil {

    private static final String GREMLIN_DRIVER_SERIALIZER //
            = Serializers.GRAPHSON_V2D0.name();

    @Value("${graph.ns.env}")
    private String defaultEnv;

    @Value("${graph.ns.version}")
    private String defaultVersion;

    @Value("${graph.ns.is.postfix:true}")
    private Boolean isNSPostfix;

    @Value("${graph.exception.ignore}")
    private Boolean ignoreException;

    @Value("${graph.contact.url}")
    private String graphContactPoint;

    @Value("${graph.contact.port}")
    private Integer graphContactPort;

    private NameSpaceUtil nameSpaceUtil;

    @PostConstruct
    public void postConstruct() {
        nameSpaceUtil = new NameSpaceUtil(defaultEnv, defaultVersion);
    }

    public String getDefaultEnv() {
        return defaultEnv;
    }

    public void setDefaultEnv(String defaultEnv) {
        this.defaultEnv = defaultEnv;
    }

    public String getDefaultVersion() {
        return defaultVersion;
    }

    public void setDefaultVersion(String defaultVersion) {
        this.defaultVersion = defaultVersion;
    }

    public Boolean getIsNSPostfix() {
        return isNSPostfix;
    }

    public void setIsNSPostfix(Boolean isNSPostfix) {
        this.isNSPostfix = isNSPostfix;
    }

    public NameSpaceUtil getNameSpaceUtil() {
        return nameSpaceUtil;
    }

    public void setNameSpaceUtil(NameSpaceUtil nameSpaceUtil) {
        this.nameSpaceUtil = nameSpaceUtil;
    }

    public Boolean getIgnoreException() {
        return ignoreException;
    }

    public void setIgnoreException(Boolean ignoreException) {
        this.ignoreException = ignoreException;
    }

    public Cluster initCluster() {
        Cluster.Builder builder = Cluster.build();
        builder.addContactPoint(graphContactPoint);
        builder.port(graphContactPort);
        builder.serializer(GREMLIN_DRIVER_SERIALIZER);
        return builder.create();
    }

    public GraphTraversalSource initTraversalSource(Cluster cluster) {
        return EmptyGraph.instance().traversal().withRemote(DriverRemoteConnection.using(cluster));
    }
}
