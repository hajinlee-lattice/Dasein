package com.latticeengines.apps.lp.proxy;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component
public class PlaymakerTenantProxy extends MicroserviceRestApiProxy {

    public PlaymakerTenantProxy() {
        super("lp/playmaker/tenants");
    }

    public PlaymakerTenant createTenant(PlaymakerTenant tenant) {
        return post("create playmaker tenant", constructUrl("/"), tenant, PlaymakerTenant.class);
    }

    public void updateTenant(String tenantName, PlaymakerTenant tenant) {
        String url = constructUrl("/{tenantName}", tenantName);
        put("update playmaker tenant", url, tenant);
    }

    public PlaymakerTenant getTenant(String tenantName) {
        String url = constructUrl("/{tenantName}", tenantName);
        return get("get playmaker tenant", url, PlaymakerTenant.class);
    }

    public void deleteTenant(String tenantName) {
        String url = constructUrl("/{tenantName}", tenantName);
        delete("delete playmaker tenant", url);
    }

}
