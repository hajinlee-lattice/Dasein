package com.latticeengines.proxy.exposed.cdl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    public Map<String, List<String>> getEmailsByTenantId(String tenantId) {
        String url = constructUrl("/subscription/tenant/{tenantId}", tenantId);
        Map<?, List<?>> map = get("Get subscription emails by tenantId", url, Map.class);
        return JsonUtils.convertMapWithListValue(map, String.class, String.class);
    }

    public Map<String, List<String>> saveByEmailsAndTenantId(Map<String, Set<String>> emails, String tenantId) {
        String url = constructUrl("/subscription/tenant/{tenantId}", tenantId);
        Map<?, List<?>> map = post("Create subscription by email list and tenantId", url, emails, Map.class);
        return JsonUtils.convertMapWithListValue(map, String.class, String.class);
    }

    public void deleteByEmailAndTenantId(String email, String tenantId) {
        String url = constructUrl("/subscription/tenant/{tenantId}", tenantId);
        List<String> params = new ArrayList<>();
        params.add("email=" + email);
        url += "?" + StringUtils.join(params, "&");
        delete("Delete subscription by email and tenantId", url);
    }
}
