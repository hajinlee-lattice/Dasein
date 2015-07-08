package com.latticeengines.pls.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.TenantConfigService;

@Component("tenantConfigService")
public class TenantConfigServiceImpl implements TenantConfigService {

    private static final Log log = LogFactory.getLog(TenantConfigServiceImpl.class);
    private static final BatonService batonService = new BatonServiceImpl();
    private static final String SPACE_CONFIGURATION_ZNODE = "/SpaceConfiguration";
    private static final String TOPOLOGY_ZNODE = "/Topology";
    private static final String DL_ADDRESS_ZNODE = "/DL_Address";

    @Value("${pls.dataloader.rest.api}")
    private String defaultDataLoaderUrl;

    @Override
    public String getTopology(String tenantId) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                    customerSpace.getContractId(), customerSpace.getTenantId(),
                    customerSpace.getSpaceId()).append(new Path(SPACE_CONFIGURATION_ZNODE + TOPOLOGY_ZNODE));
            return camille.get(path).getData();
        } catch (Exception ex) {
            log.error("Can not get tenant's topology", ex);
            throw new LedpException(LedpCode.LEDP_18033, ex);
        }
    }

    @Override
    public String getDLRestServiceAddress(String tenantId) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                    customerSpace.getContractId(), customerSpace.getTenantId(),
                    customerSpace.getSpaceId()).append(new Path(SPACE_CONFIGURATION_ZNODE + DL_ADDRESS_ZNODE));
            return camille.get(path).getData();
        } catch (Exception ex) {
            log.error("Can not get tenant's data loader address from ZK", ex);
            return defaultDataLoaderUrl;
        }
    }

    @Override
    public TenantDocument getTenantDocument(String tenantId) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            return batonService.getTenant(customerSpace.getContractId(), customerSpace.getTenantId());
        } catch (Exception e) {
            log.error("Can not get tenant's topology", e);
            throw new LedpException(LedpCode.LEDP_18034, e);
        }
    }
}
