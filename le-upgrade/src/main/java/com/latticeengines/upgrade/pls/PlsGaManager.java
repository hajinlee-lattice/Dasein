package com.latticeengines.upgrade.pls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.httpclient.util.URIUtil;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Tenant;

@Component
public class PlsGaManager {

    @Value("${upgrade.pls.url}")
    private String plsApiHost;

    @Value("${upgrade.pls.superadmin}")
    private String superAdmins;

    @Value("${upgrade.pls.latticeadmin}")
    private String latticeAdmin;

    private static ImmutableList<BasicNameValuePair> headers;
    private static ImmutableList<String> superAdminEmails;
    private static ImmutableList<String> latticeAdminEmails;

    static {
        List<BasicNameValuePair> list = new ArrayList<>();
        list.add(new BasicNameValuePair("MagicAuthentication", "Security through obscurity!"));
        list.add(new BasicNameValuePair("Content-Type", "application/json; charset=utf-8"));
        list.add(new BasicNameValuePair("Accept", "application/json"));
        headers = ImmutableList.copyOf(list);
    }

    @PostConstruct
    private void readEmails() {
        superAdminEmails = ImmutableList.copyOf(Arrays.asList(superAdmins.split(",")));
        latticeAdminEmails = ImmutableList.copyOf(Arrays.asList(latticeAdmin.split(",")));
    }

    public void registerTenant(String customer) {
        Tenant tenant = new Tenant();
        String tupleId = CustomerSpace.parse(customer).toString();
        String singularId = CustomerSpace.parse(customer).getTenantId();
        tenant.setId(tupleId);
        tenant.setName(singularId);

        String url = plsApiHost + "/pls/admin/tenants";
        try {
            HttpClientWithOptionalRetryUtils.sendPostRequest(url, false, headers, JsonUtils.serialize(tenant));
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_24003, "Error registering " + customer + " through PLS api.", e);
        }
    }

    public void setupAdminUsers(String customer) {
        String tenantId = CustomerSpace.parse(customer).toString();
        for (String email: latticeAdminEmails) {
            assignAccessLevel(tenantId, email, "INTERNAL_ADMIN");
        }
        for (String email: superAdminEmails) {
            assignAccessLevel(tenantId, email, "SUPER_ADMIN");
        }
    }


    private void assignAccessLevel(String tenantId, String email, String level) {
        try {
            String encodedEmail = URIUtil.encodeQuery(email, "UTF-8");
            String encodedTenant = URIUtil.encodeQuery(tenantId, "UTF-8");
            String url = plsApiHost + "/pls/admin/users?tenant=" + encodedTenant + "&username=" + encodedEmail;
            UserUpdateData data = new UserUpdateData();
            data.setAccessLevel(level);
            HttpClientWithOptionalRetryUtils.sendPutRequest(url, false, headers, JsonUtils.serialize(data));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24003,
                    String.format("Failed to assign %s as %s for customer %s", email, level, tenantId), e);
        }
    }
}
