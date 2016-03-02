package com.latticeengines.pls.util;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.pls.service.OauthService;
import com.latticeengines.security.exposed.util.BaseRestApiProxy;

@Component("oauthRestApiProxy")
public class OauthRestApiProxy extends BaseRestApiProxy implements OauthService {

    @Value("${proxy.oauth.rest.endpoint.hostport}")
    private String oauthHostPort;

    @Override
    public String getRestApiHostPort() {
        return oauthHostPort;
    }

    @Override
    public String generateAPIToken(String tenantId) {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setTenantName(tenantId);
        tenant.setJdbcDriver("dummy");
        tenant.setJdbcUrl("dummy");
        String url = constructUrl("tenants");
        tenant = restTemplate.postForObject(url, tenant, PlaymakerTenant.class);
        return tenant.getTenantPassword();
    }

}
