package com.latticeengines.upgrade.zk;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

@Component
public class ZooKeeperManager {

    private static final String DESCRIPTION = "A LPA tenant upgraded from 1.3.4 or 1.4.0";
    private static final String SPACE_CONFIG = "/SpaceConfiguration";
    @Autowired
    private BatonService batonService;

    private String podId;
    private Camille camille;

    @PostConstruct
    private void readCamilleEnvironment() {
        podId = CamilleEnvironment.getPodId();
        camille = CamilleEnvironment.getCamille();
    }

    public void registerTenant(String tenantId) {
        CustomerSpaceProperties properties = new CustomerSpaceProperties(tenantId, DESCRIPTION, null, null);
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, "");
        batonService.createTenant(tenantId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, spaceInfo);
    }

    public void uploadSpaceConfiguration(String tenantId, SpaceConfiguration spaceConfig) {
        Path spaceConfigPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                tenantId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID)
                .append(new Path(SPACE_CONFIG));
        batonService.loadDirectory(spaceConfig.toDocumentDirectory(), spaceConfigPath);
    }

}
