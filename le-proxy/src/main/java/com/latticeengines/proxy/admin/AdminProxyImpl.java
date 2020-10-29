package com.latticeengines.proxy.admin;

import org.springframework.stereotype.Component;

import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.admin.AdminProxy;

@Component("adminProxy")
public class AdminProxyImpl extends MicroserviceRestApiProxy implements AdminProxy {

    protected AdminProxyImpl() {
        super("admin");
    }

    @Override
    public void deleteTenant(String contractId, String tenantId) {
        String url = constructUrl(String.format("/internal/tenants/%s?contractId=%s&deleteZookeeper=true", tenantId,
                contractId));
        delete("delete tenant", url);
    }
}
