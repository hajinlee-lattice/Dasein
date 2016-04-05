package com.latticeengines.propdata.core.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceConnection;
import com.latticeengines.propdata.core.service.ZkConfigurationService;

public class ZkConfigurationServiceImpl implements ZkConfigurationService {

    private static final Log log = LogFactory.getLog(ZkConfigurationServiceImpl.class);

    private Camille camille;
    private String podId;
    private static final String PROPDATA_SERVICE = "PropData";
    private static final String DATASOURCES = "DataSources";
    private static final String BOOTSTRAP_MODE = "BOOTSTRAP";
    private String mode;

    @Value("${propdata.source.db.json:source_dbs_dev.json}")
    private String sourceDbsJson;

    @Value("${propdata.target.db.json:target_dbs_dev.json}")
    private String targetDbsJson;

    public ZkConfigurationServiceImpl(String mode) {
        this.mode = mode;
    }

    @PostConstruct
    private void postConstruct() {
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();
        if (BOOTSTRAP_MODE.equals(mode)) {
            try {
                bootstrapCamille();
            } catch (Exception e) {
                throw new RuntimeException("Failed to bootstrap camille zk configuration.", e);
            }
        }
    }

    private void bootstrapCamille() throws Exception {
        log.info("Uploading source db connection pool to ZK using " + sourceDbsJson + " ...");
        String json = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("datasource/" + sourceDbsJson));
        Path poolPath = dbPoolPath(DataSourcePool.SourceDB);
        if (!camille.exists(poolPath) || !camille.get(poolPath).getData().equals(json)) {
            camille.upsert(poolPath, new Document(json), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }

        log.info("Uploading target db connection pool to ZK using " + targetDbsJson + " ...");
        json = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("datasource/" + targetDbsJson));
        poolPath = dbPoolPath(DataSourcePool.TargetDB);
        if (!camille.exists(poolPath) || !camille.get(poolPath).getData().equals(json)) {
            camille.upsert(poolPath, new Document(json), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }

    }

    @Override
    public List<DataSourceConnection> getConnectionsInPool(DataSourcePool pool) {
        ObjectMapper mapper = new ObjectMapper();
        Path poolPath = dbPoolPath(pool);
        try {
            List<DataSourceConnection> connections = new ArrayList<>();
            ArrayNode arrayNode = mapper.readValue(camille.get(poolPath).getData(), ArrayNode.class);
            for (JsonNode jsonNode : arrayNode) {
                connections.add(mapper.treeToValue(jsonNode, DataSourceConnection.class));
            }
            return connections;
        } catch (Exception e) {
            return null;
        }
    }


    private Path dbPoolPath(DataSourcePool pool) {
        Path propDataPath = PathBuilder.buildServicePath(podId, PROPDATA_SERVICE);
        return propDataPath.append(DATASOURCES).append(pool.name());
    }

}
