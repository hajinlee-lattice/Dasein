package com.latticeengines.pls.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.globalauth.authentication.GlobalTenantManagementService;
import com.latticeengines.pls.service.TenantService;

@Component("tenantService")
public class TenantServiceImpl implements TenantService {
    private static final Log log = LogFactory.getLog(TenantServiceImpl.class);

    @Autowired
    private GlobalTenantManagementService globalTenantManagementService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;


    @Override
    public void registerTenant(Tenant tenant) {
        try {
            globalTenantManagementService.registerTenant(tenant);
        } catch (Exception e) {
            log.warn("Error registering tenant with GA.", e);
        }
        tenantEntityMgr.create(tenant);
    }

    @Override
    public void discardTenant(Tenant tenant) {
        tenantEntityMgr.delete(tenant);
        try {
            globalTenantManagementService.discardTenant(tenant);
        } catch (Exception e) {
            log.warn("Error discarding tenant with GA.", e);
        }
    }


    @Override
    public List<Tenant> getAllTenants() {
        return tenantEntityMgr.findAll();
    }

}
