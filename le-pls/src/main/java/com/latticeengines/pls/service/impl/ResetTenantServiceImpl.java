package com.latticeengines.pls.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.pls.service.ResetTenantService;
import com.latticeengines.proxy.exposed.component.ComponentProxy;

@Component("resetTenantService")
public class ResetTenantServiceImpl implements ResetTenantService {

    @Inject
    private ComponentProxy componentProxy;

    @Override
    public void resetTenant(String tenantId) {
        componentProxy.reset(tenantId, ComponentConstants.CDL);
        componentProxy.reset(tenantId, ComponentConstants.LP);
        componentProxy.reset(tenantId, ComponentConstants.METADATA);
    }
}
