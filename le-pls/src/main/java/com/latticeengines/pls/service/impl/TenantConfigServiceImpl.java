package com.latticeengines.pls.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.TenantConfigService;

@Component("tenantConfigService")
public class TenantConfigServiceImpl implements TenantConfigService {

    private static final Log log = LogFactory.getLog(TenantConfigServiceImpl.class);
    private static final BatonService batonService = new BatonServiceImpl();

    @Override
    public String getTopology(String tenantId) {
        try {
            TenantDocument doc = getTenantDocument(tenantId);
            return doc.getSpaceConfig().getTopology().getName().toUpperCase();
        } catch (Exception ex) {
            log.error("Can not get tenant's topology", ex);
            throw new LedpException(LedpCode.LEDP_18033, ex);
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
