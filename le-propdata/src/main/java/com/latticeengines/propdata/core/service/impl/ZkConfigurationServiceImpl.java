package com.latticeengines.propdata.core.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.propdata.core.datasource.DataSourceConnection;
import com.latticeengines.propdata.core.datasource.DataSourcePool;
import com.latticeengines.propdata.core.service.ZkConfigurationService;
import com.latticeengines.propdata.core.source.Source;

@Component("zkConfigurationService")
public class ZkConfigurationServiceImpl implements ZkConfigurationService {

    private static final Log log = LogFactory.getLog(ZkConfigurationServiceImpl.class);

    private Camille camille;
    private String podId;
    private static final String PROPDATA_SERVICE = "PropData";
    private static final String SOURCES = "Sources";
    private static final String JOB_ENABLED = "JobEnabled";
    private static final String JOB_CRON = "JobCronExpression";

    private static final String DATASOURCES = "DataSources";

    private static final String MAX_REALTIME_MATCH_INPUT = "MaxRealTimeMatchInput";

    @Autowired
    List<Source> sources;

    @Value("${propdata.source.db.json:source_dbs_dev.json}")
    private String sourceDbsJson;

    @Value("${propdata.target.db.json:target_dbs_dev.json}")
    private String targetDbsJson;

    @PostConstruct
    private void postConstruct() throws Exception {
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();

        for (Source source: sources) {
            Path flagPath = jobFlagPath(source);
            if (!camille.exists(flagPath)) {
                camille.create(flagPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
        }

        log.info("Uploading source db connection pool to ZK using " + sourceDbsJson + " ...");
        String json = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("datasource/" + sourceDbsJson));
        Path poolPath = dbPoolPath(DataSourcePool.SourceDB);
        camille.upsert(poolPath, new Document(json), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        log.info("Uploading target db connection pool to ZK using " + targetDbsJson + " ...");
        json = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("datasource/" + targetDbsJson));
        poolPath = dbPoolPath(DataSourcePool.TargetDB);
        camille.upsert(poolPath, new Document(json), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        log.info("Read or initializing max real time input ...");
        if (!camille.exists(maxRealTimeInputPath())) {
            camille.create(maxRealTimeInputPath(), new Document("1000"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }

    }

    @Override
    public List<DataSourceConnection> getConnectionsInPool(DataSourcePool pool) {
        ObjectMapper mapper = new ObjectMapper();
        Path poolPath = dbPoolPath(pool);
        try {
            List<DataSourceConnection> connections = new ArrayList<>();
            ArrayNode arrayNode = mapper.readValue(camille.get(poolPath).getData(), ArrayNode.class);
            for (JsonNode jsonNode: arrayNode) {
                connections.add(mapper.treeToValue(jsonNode, DataSourceConnection.class));
            }
            return connections;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Integer maxRealTimeInput() {
        try {
            return Integer.valueOf(camille.get(maxRealTimeInputPath()).getData());
        } catch (Exception e) {
            log.warn("Failed to retrieve max real time input size from zk, using default value of 1000 instead");
            return 1000;
        }
    }

    @Override
    public boolean refreshJobEnabled(Source source) {
        Path flagPath = jobFlagPath(source);
        try {
            if (camille.exists(flagPath)) {
                return Boolean.valueOf(camille.get(flagPath).getData());
            } else {
                camille.create(flagPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }


    @Override
    public String refreshCronSchedule(Source source) {
        Path cronPath = jobCronPath(source);
        try {
            if (camille.exists(cronPath)) {
                return String.valueOf(camille.get(cronPath).getData());
            } else {
                camille.create(cronPath, new Document(source.getDefaultCronExpression()), ZooDefs.Ids.OPEN_ACL_UNSAFE);
                return source.getDefaultCronExpression();
            }
        } catch (Exception e) {
            return null;
        }
    }

    private Path dbPoolPath(DataSourcePool pool) {
        Path propDataPath = PathBuilder.buildServicePath(podId, PROPDATA_SERVICE);
        return propDataPath.append(DATASOURCES).append(pool.name());
    }

    private Path jobFlagPath(Source source) {
        Path propDataPath = PathBuilder.buildServicePath(podId, PROPDATA_SERVICE);
        Path sourcePath = propDataPath.append(SOURCES).append(source.getSourceName());
        return sourcePath.append(JOB_ENABLED);
    }

    private Path jobCronPath(Source source) {
        Path propDataPath = PathBuilder.buildServicePath(podId, PROPDATA_SERVICE);
        Path sourcePath = propDataPath.append(SOURCES).append(source.getSourceName());
        return sourcePath.append(JOB_CRON);
    }

    private Path maxRealTimeInputPath() {
        Path propDataPath = PathBuilder.buildServicePath(podId, PROPDATA_SERVICE);
        return propDataPath.append(MAX_REALTIME_MATCH_INPUT);
    }

}
