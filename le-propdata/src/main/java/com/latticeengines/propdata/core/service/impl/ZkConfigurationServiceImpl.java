package com.latticeengines.propdata.core.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceConnection;
import com.latticeengines.propdata.core.service.ZkConfigurationService;

@Component("zkConfigurationService")
public class ZkConfigurationServiceImpl implements ZkConfigurationService {

    private Camille camille;
    private String podId;
    private static final String PROPDATA_SERVICE = "PropData";
    private static final String DATASOURCES = "DataSources";
    private static final String STACKS = "Stacks";

    @Value("${propdata.source.db.json:source_dbs_dev.json}")
    private String sourceDbsJson;

    @Value("${propdata.target.db.json:target_dbs_dev.json}")
    private String targetDbsJson;

    @Value("${common.le.stack}")
    private String leStack;

    @PostConstruct
    private void postConstruct() {
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();
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
        if (StringUtils.isNotEmpty(leStack)) {
            propDataPath = propDataPath.append(STACKS).append(leStack);
        }
        return propDataPath.append(DATASOURCES).append(pool.name());
    }

}
