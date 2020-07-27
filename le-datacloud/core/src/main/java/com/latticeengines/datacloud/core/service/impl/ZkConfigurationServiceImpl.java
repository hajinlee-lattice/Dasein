package com.latticeengines.datacloud.core.service.impl;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

@Component("zkConfigurationService")
public class ZkConfigurationServiceImpl implements ZkConfigurationService {

    private static final Logger log = LoggerFactory.getLogger(ZkConfigurationServiceImpl.class);

    private Camille camille;
    private String podId;
    private static Boolean relaxPublicDomain;
    private static final String PROPDATA_SERVICE = "PropData";
    private static final String MATCH_SERVICE = "Match";
    private static final String USE_REMOTE_DNB_GLOBAL = "UseRemoteDnB";
    private static final String RELAX_PUBLIC_DOMAIN_CHECK = "RelaxPublicDomainCheck";

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${datacloud.dnb.use.remote.global}")
    private String useRemoteDnBGlobal;

    @Inject
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
        // Flag of UseRemoteDnBGlobal
        Path useRemoteDnBPath = useRemoteDnBGlobalPath();
        if (!camille.exists(useRemoteDnBPath) || StringUtils.isBlank(camille.get(useRemoteDnBPath).getData())) {
            camille.upsert(useRemoteDnBPath, new Document(useRemoteDnBGlobal), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }
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
