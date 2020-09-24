package com.latticeengines.proxy.exposed.cdl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("subscriptionProxy")
public class SubscriptionProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected SubscriptionProxy() {
        super("cdl");
    }

    public List<String> getEmailsByTenantId(String tenantId) {
        String url = constructUrl("/subscription/tenant/{tenantId}", tenantId);
        List<?> list = get("Get subscription emails by tenantId", url, List.class);
        return JsonUtils.convertList(list, String.class);
    }

    public List<String> saveByEmailsAndTenantId(Set<String> emails, String tenantId) {
        String url = constructUrl("/subscription/tenant/{tenantId}", tenantId);
        List<?> list = post("Create subscription by email list and tenantId", url, emails, List.class);
        return JsonUtils.convertList(list, String.class);
    }

    public void deleteByEmailAndTenantId(String email, String tenantId) {
        String url = constructUrl("/subscription/tenant/{tenantId}", tenantId);
        List<String> params = new ArrayList<>();
        params.add("email=" + email);
        url += "?" + StringUtils.join(params, "&");
        delete("Delete subscription by email and tenantId", url);
    }
}
