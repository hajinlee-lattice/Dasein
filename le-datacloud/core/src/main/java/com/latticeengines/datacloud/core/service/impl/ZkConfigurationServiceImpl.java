package com.latticeengines.datacloud.core.service.impl;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.datacloud.core.datasource.DataSourceConnection;
import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.DataSourcePool;

@Component("zkConfigurationService")
public class ZkConfigurationServiceImpl implements ZkConfigurationService {

    private static final Logger log = LoggerFactory.getLogger(ZkConfigurationServiceImpl.class);

    private Camille camille;
    private String podId;
    private static Boolean relaxPublicDomain;
    private static final String PROPDATA_SERVICE = "PropData";
    private static final String DATASOURCES = "DataSources";
    private static final String MATCH_SERVICE = "Match";
    private static final String USE_REMOTE_DNB_GLOBAL = "UseRemoteDnB";
    private static final String RELAX_PUBLIC_DOMAIN_CHECK = "RelaxPublicDomainCheck";

    @Value("${datacloud.source.db.json}")
    private String sourceDbsJson;

    @Value("${datacloud.target.db.json}")
    private String targetDbsJson;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${datacloud.dnb.use.remote.global}")
    private String useRemoteDnBGlobal;

    @Autowired
    private BatonService batonService;

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
                .getResourceAsStream("datasource/" + sourceDbsJson), Charset.defaultCharset());
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
            String data = camille.get(poolPath).getData();
            if (StringUtils.isBlank(data)) {
                return connections;
            }
            ArrayNode arrayNode = mapper.readValue(data, ArrayNode.class);
            for (JsonNode jsonNode : arrayNode) {
                connections.add(mapper.treeToValue(jsonNode, DataSourceConnection.class));
            }
            return connections;
        } catch (Exception e) {
            log.error("Failed to get Connections in Pool", e);
            return null;
        }
    }

    private Path dbPoolPath(DataSourcePool pool) {
        Path propDataPath = PathBuilder.buildServicePath(podId, PROPDATA_SERVICE);
        if (StringUtils.isNotEmpty(leStack)) {
            propDataPath = propDataPath.append(PathConstants.STACKS).append(leStack);
        }
        return propDataPath.append(DATASOURCES).append(pool.name());
    }

    @Override
    public boolean isMatchDebugEnabled(CustomerSpace customerSpace) {
        try {
            return batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_MATCH_DEBUG);
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
            return Boolean.valueOf(camille.get(useRemoteDnBPath).getData());
        } catch (Exception e) {
            log.error("Failed to get UseRemoteDnBGlobal flag", e);
            return Boolean.valueOf(useRemoteDnBGlobal);
        }
    }

    @Override
    public boolean isCDLTenant(CustomerSpace customerSpace) {
        try {
            return batonService.hasProduct(customerSpace, LatticeProduct.CG);
        } catch (Exception e) {
            log.error("Error when check CDL product for " + customerSpace, e);
            return false;
        }
    }

    @Override
    public boolean isPublicDomainCheckRelaxed() {
        if (relaxPublicDomain == null) {
            Path publicDomainPath = relaxPublicDomainCheckPath();
            try {
                if (!camille.exists(publicDomainPath) || StringUtils.isBlank(camille.get(publicDomainPath).getData())) {
                    camille.upsert(publicDomainPath, new Document("false"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
                }
                relaxPublicDomain = Boolean.valueOf(camille.get(publicDomainPath).getData());
            } catch (Exception e) {
                log.error("Failed to get RELAX_PUBLIC_DOMAIN_CHECK flag", e);
                relaxPublicDomain = false;
            }
        }
        return relaxPublicDomain;
    }

    private Path matchServicePath() {
        return PathBuilder.buildServicePath(podId, PROPDATA_SERVICE).append(MATCH_SERVICE);
    }

    private Path useRemoteDnBGlobalPath() {
        return matchServicePath().append(USE_REMOTE_DNB_GLOBAL);
    }

    private Path relaxPublicDomainCheckPath() {
        return PathBuilder.buildServicePath(podId, PROPDATA_SERVICE, leStack).append(RELAX_PUBLIC_DOMAIN_CHECK);
    }
}
