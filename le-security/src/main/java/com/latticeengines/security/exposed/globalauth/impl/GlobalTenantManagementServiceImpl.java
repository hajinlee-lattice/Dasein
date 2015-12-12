package com.latticeengines.security.exposed.globalauth.impl;

import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.monitor.annotation.RestApiCall;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.globalauth.generated.tenantmgr.ITenantManagementService;
import com.latticeengines.security.globalauth.generated.tenantmgr.TenantManagementService;

@Component("globalTenantManagementService")
public class GlobalTenantManagementServiceImpl extends GlobalAuthenticationServiceBaseImpl implements GlobalTenantManagementService {

    private static final Log log = LogFactory.getLog(GlobalTenantManagementServiceImpl.class);

    @RestApiCall
    private ITenantManagementService getService() {
        TenantManagementService service;
        try {
            service = new TenantManagementService(new URL(globalAuthUrl + "/GlobalAuthTenantManager?wsdl"));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18000, e, new String[] { globalAuthUrl });
        }
        return service.getBasicHttpBindingITenantManagementService();
    }
    
    @Override
    @RestApiCall
    public synchronized Boolean registerTenant(Tenant tenant) {
        ITenantManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        
        try {
            log.info(String.format("Registering tenant with id %s.", tenant.getId()));
            return service.registerTenant(tenant.getId(), tenant.getName());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18012, new String[] { tenant.getId(), tenant.getName() });
        }
    }

    @Override
    @RestApiCall
    public synchronized Boolean discardTenant(Tenant tenant) {
        ITenantManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        
        try {
            log.info(String.format("Discarding tenant with id %s.", tenant.getId()));
            return service.discardTenant(tenant.getId());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18013, new String[] { tenant.getId() });
        }
    }

}
