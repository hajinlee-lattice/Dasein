package com.latticeengines.datacloud.core.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.datacloud.core.datasource.DataSourceConnection;
import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.datacloud.DataSourcePool;

@Component("zkConfigurationService")
public class ZkConfigurationServiceImpl implements ZkConfigurationService {

    private static final Log log = LogFactory.getLog(ZkConfigurationServiceImpl.class);

    private Camille camille;
    private String podId;
    private static final String PROPDATA_SERVICE = "PropData";
    private static final String DATASOURCES = "DataSources";
    private static final String STACKS = "Stacks";
    private static final String MATCH_SERVICE = "Match";
    private static final String USE_REMOTE_DNB_GLOBAL = "UseRemoteDnB";

    @Value("${datacloud.source.db.json}")
    private String sourceDbsJson;

    @Value("${datacloud.target.db.json}")
    private String targetDbsJson;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${datacloud.dnb.use.remote.global}")
    private String useRemoteDnBGlobal;

    @PostConstruct
    private void postConstruct() {
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();
        try {
            bootstrapCamille();
        } catch (Exception e) {
            throw new RuntimeException("Failed to bootstrap camille zk configuration.", e);
        }
    }

    private void bootstrapCamille() throws Exception {
        String json = IOUtils.toString(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("datasource/" + sourceDbsJson));
        Path poolPath = dbPoolPath(DataSourcePool.SourceDB);
        if (!camille.exists(poolPath)) {
            log.info("Uploading source db connection pool to ZK using " + sourceDbsJson + " ...");
            camille.upsert(poolPath, new Document(json), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }
        // Flag of UseRemoteDnBGlobal
        Path useRemoteDnBPath = useRemoteDnBGlobalPath();
        if (!camille.exists(useRemoteDnBPath) || StringUtils.isBlank(camille.get(useRemoteDnBPath).getData())) {
            camille.upsert(useRemoteDnBPath, new Document(useRemoteDnBGlobal), ZooDefs.Ids.OPEN_ACL_UNSAFE);
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

    public boolean fuzzyMatchEnabled(CustomerSpace customerSpace) {
        try {
            FeatureFlagValueMap flags = FeatureFlagClient.getFlags(customerSpace);
            return (flags.containsKey(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName()) && Boolean.TRUE.equals(flags
                    .get(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName())));
        } catch (Exception e) {
            log.error("Error when retrieving " + LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName()
                    + " feature flags for " + customerSpace, e);
            return false;
        }
    }

    public boolean bypassDnBCache(CustomerSpace customerSpace) {
        try {
            FeatureFlagValueMap flags = FeatureFlagClient.getFlags(customerSpace);
            if (!flags.containsKey(LatticeFeatureFlag.BYPASS_DNB_CACHE.getName())) {
                return false;
            } else {
                return Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.BYPASS_DNB_CACHE.getName()));
            }
        } catch (Exception e) {
            log.error("Error when retrieving " + LatticeFeatureFlag.BYPASS_DNB_CACHE.getName() + " feature flags for "
                    + customerSpace, e);
            return false;
        }
    }

    private Path dbPoolPath(DataSourcePool pool) {
        Path propDataPath = PathBuilder.buildServicePath(podId, PROPDATA_SERVICE);
        if (StringUtils.isNotEmpty(leStack)) {
            propDataPath = propDataPath.append(STACKS).append(leStack);
        }
        return propDataPath.append(DATASOURCES).append(pool.name());
    }

    @Override
    public boolean isMatchDebugEnabled(CustomerSpace customerSpace) {
        try {
            FeatureFlagValueMap flags = FeatureFlagClient.getFlags(customerSpace);
            return (flags.containsKey(LatticeFeatureFlag.ENABLE_MATCH_DEBUG.getName()) && Boolean.TRUE.equals(flags
                    .get(LatticeFeatureFlag.ENABLE_MATCH_DEBUG.getName())));
        } catch (Exception e) {
            log.error("Error when retrieving " + LatticeFeatureFlag.ENABLE_MATCH_DEBUG.getName()
                    + " feature flags for " + customerSpace, e);
            return false;
        }
    }

    @Override
    public boolean useRemoteDnBGlobal() {
        Path useRemoteDnBPath = useRemoteDnBGlobalPath();
        try {
            if (!camille.exists(useRemoteDnBPath) || StringUtils.isBlank(camille.get(useRemoteDnBPath).getData())) {
                camille.upsert(useRemoteDnBPath, new Document(useRemoteDnBGlobal), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
            return Boolean.valueOf(camille.get(useRemoteDnBPath).getData()).booleanValue();
        } catch (Exception e) {
            log.error("Failed to get UseRemoteDnBGlobal flag", e);
            return Boolean.valueOf(useRemoteDnBGlobal);
        }
    }

    private Path matchServicePath() {
        return PathBuilder.buildServicePath(podId, PROPDATA_SERVICE).append(MATCH_SERVICE);
    }

    private Path useRemoteDnBGlobalPath() {
        return matchServicePath().append(USE_REMOTE_DNB_GLOBAL);
    }

}
