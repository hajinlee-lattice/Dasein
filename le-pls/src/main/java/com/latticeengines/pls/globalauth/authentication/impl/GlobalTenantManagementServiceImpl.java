package com.latticeengines.pls.globalauth.authentication.impl;

import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.globalauth.authentication.GlobalTenantManagementService;
import com.latticeengines.pls.globalauth.generated.tenantmgr.ITenantManagementService;
import com.latticeengines.pls.globalauth.generated.tenantmgr.TenantManagementService;

@Component("globalTenantManagementService")
public class GlobalTenantManagementServiceImpl extends GlobalAuthenticationServiceBaseImpl implements GlobalTenantManagementService {

    private static final Log log = LogFactory.getLog(GlobalTenantManagementServiceImpl.class);

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
    public Boolean registerTenant(Tenant tenant) {
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
    public Boolean discardTenant(Tenant tenant) {
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
