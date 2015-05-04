package com.latticeengines.pls.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.TenantConfigService;

@Component("tenantConfigService")
public class TenantConfigServiceImpl implements TenantConfigService {

    private static final Log log = LogFactory.getLog(TenantConfigServiceImpl.class);

    @Override
    public String getTopology(String tenantId) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            CustomerSpaceInfo spaceInfo = SpaceLifecycleManager.getInfo(customerSpace.getContractId(),
                    customerSpace.getTenantId(), customerSpace.getSpaceId());

            return spaceInfo.properties.topology;

        } catch (Exception ex) {
            log.error("Can not get tenant's topology", ex);
            throw new LedpException(LedpCode.LEDP_18033, ex);
        }
    }
}
