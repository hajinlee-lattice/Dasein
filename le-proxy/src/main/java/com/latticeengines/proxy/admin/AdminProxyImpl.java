package com.latticeengines.proxy.admin;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.admin.AdminProxy;

@Component("adminProxy")
public class AdminProxyImpl extends BaseRestApiProxy implements AdminProxy {

    protected AdminProxyImpl() {
        super(PropertyUtils.getProperty("common.admin.url"), "admin");
    }

    @Override
    public void deleteTenant(String contractId, String tenantId) {
        String url = constructUrl(String.format("/internal/tenants/%s?contractId=%s", tenantId,
                contractId));
        delete("delete tenant", url);
    }

    @Override
    public List<String> getAllTenantIds() {
        String url = constructUrl("/internal/tenants");
        return getList("get tenant ids", url, String.class);
    }
}
