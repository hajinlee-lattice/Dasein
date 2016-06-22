package com.latticeengines.security.exposed.globalauth.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;

@Component("globalTenantManagementService")
public class GlobalTenantManagementServiceImpl extends GlobalAuthenticationServiceBaseImpl
        implements GlobalTenantManagementService {

    private static final Log log = LogFactory.getLog(GlobalTenantManagementServiceImpl.class);

    @Autowired
    private GlobalAuthTenantEntityMgr gaTenantEntityMgr;

    @Override
    public synchronized Boolean registerTenant(Tenant tenant) {

        try {
            log.info(String.format("Registering tenant with id %s.", tenant.getId()));
            GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenant.getId());
            if (tenantData != null) {
                throw new Exception("The specified tenant already exists");
            }
            tenantData = new GlobalAuthTenant();
            tenantData.setId(tenant.getId());
            tenantData.setName(tenant.getName());
            gaTenantEntityMgr.create(tenantData);
            return true;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18012, new String[] { tenant.getId(),
                    tenant.getName() });
        }
    }

    @Override
    public synchronized Boolean discardTenant(Tenant tenant) {

        try {
            log.info(String.format("Discarding tenant with id %s.", tenant.getId()));
            GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenant.getId());
            if (tenantData == null) {
                return true;
            }
            gaTenantEntityMgr.delete(tenantData);
            return true;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18013, new String[] { tenant.getId() });
        }
    }

}
