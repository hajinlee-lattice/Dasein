package com.latticeengines.testframework.exposed.proxy.admin;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.proxy.exposed.ProtectedRestApiProxy;

@Component("adminTenantProxy")
public class AdminTenantProxy extends ProtectedRestApiProxy {

    @Inject
    private AdminLoginProxy loginProxy;

    public AdminTenantProxy() {
        super(PropertyUtils.getProperty("common.test.admin.url"), "admin/tenants");
    }

    @Override
    protected String loginInternal(String username, String password) {
        return loginProxy.login(username, password);
    }

    public TenantDocument getTenant(String tenantId) {
        tenantId = parseSingletonId(tenantId);
        String url = constructUrl(String.format("/%s?contractId=%s", tenantId, tenantId));
        return get("get tenant", url, TenantDocument.class);
    }

    public void createTenant(String tenantId, TenantRegistration registration) {
        tenantId = parseSingletonId(tenantId);
        String url = constructUrl(String.format("/%s?contractId=%s", tenantId, tenantId));
        Boolean success = post("create tenant", url, registration, Boolean.class);
        if (!Boolean.TRUE.equals(success)) {
            throw new IllegalStateException("Creating tenant via tenant console unsuccessful.");
        }
    }

    public void deleteTenant(String tenantId) {
        tenantId = parseSingletonId(tenantId);
        TenantDocument tenant = getTenant(tenantId);
        if (tenant != null) {
            String url = constructUrl(String.format("/%s?contractId=%s&deleteZookeeper=true", tenantId, tenantId));
            delete("delete tenant", url);
        }
    }

    private static String parseSingletonId(String tenantId) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        return customerSpace.getTenantId();
    }

}
