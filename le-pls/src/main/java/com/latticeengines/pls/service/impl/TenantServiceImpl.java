package com.latticeengines.pls.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.provisioning.PLSInstaller;
import com.latticeengines.pls.provisioning.PLSUpgrader;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;

@Component("tenantService")
@Lazy(value = true)
public class TenantServiceImpl implements TenantService {
    private static final Log log = LogFactory.getLog(TenantServiceImpl.class);

    @Autowired
    private GlobalTenantManagementService globalTenantManagementService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    public TenantServiceImpl() throws Exception {
        ServiceProperties serviceProps = new ServiceProperties();
        serviceProps.dataVersion = 1;
        serviceProps.versionString = "2.0";
        ServiceInfo serviceInfo = new ServiceInfo(serviceProps, //
                new PLSInstaller(), //
                new PLSUpgrader(), //
                null);
        ServiceWarden.registerService("PLS", serviceInfo);
    }

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
