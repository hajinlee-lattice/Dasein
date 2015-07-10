package com.latticeengines.upgrade.pls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.ModelActivationResult;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.ResponseDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;

@Component
public class PlsGaManager {

    private static final String PLS_USRNAME = "bnguyen@lattice-engines.com";
    private static final String PLS_PASSWORD = "tahoe";
    private static final ObjectMapper objectMapper = new ObjectMapper();

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

    public void setModelActivity(String modelId, boolean active) {
        String statusCode =
                active ? ModelSummaryStatus.ACTIVE.getStatusCode() : ModelSummaryStatus.INACTIVE.getStatusCode();
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", statusCode);

        try {
            String url = plsApiHost + "/pls/internal/modelsummaries/" + modelId;
            String response = HttpClientWithOptionalRetryUtils.sendPutRequest(url, false, headers,
                    JsonUtils.serialize(attrMap));
            ResponseDocument<ModelActivationResult> document =
                    ResponseDocument.generateFromJSON(response, ModelActivationResult.class);
            if (document == null) {
                throw new IOException("Unable to parse thre response: " + response);
            }
            if (!document.isSuccess()) {
                throw new IllegalStateException("Updating modelsummary did not return SUCCESS: " + response);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24003,
                    String.format("Failed to set model %s to ACTIVE", modelId), e);
        }
    }

    public void updateModelName(String modelId, String name) {
        try {
            String url = plsApiHost + "/pls/internal/modelsummaries/" + modelId;
            AttributeMap map = new AttributeMap();
            map.put("Name", name);
            HttpClientWithOptionalRetryUtils.sendPutRequest(url, false, headers, JsonUtils.serialize(map));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24003,
                    String.format("Failed to change name for model %s.", modelId), e);
        }
    }

    public List<String> getModelIds(String customer) {
        List<String> ids = new ArrayList<>();
        try {
            List<BasicNameValuePair> authHeaders = loginAndAttach(customer);
            String url = plsApiHost + "/pls/modelsummaries";
            String response = HttpClientWithOptionalRetryUtils.sendGetRequest(url, false, authHeaders);
            JsonNode models = objectMapper.readTree(response);
            for (JsonNode model: models) {
                ModelSummary summary = objectMapper.treeToValue(model, ModelSummary.class);
                ids.add(summary.getId());
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24003,
                    String.format("Failed to get all modelIDs for %s.", customer), e);
        }
        return ids;
    }

    public void deleteTenantWithSingularId(String customer) {
        String tenantId = CustomerSpace.parse(customer).getTenantId();
        try {
            String url = plsApiHost + "/pls/admin/tenants/" + tenantId;
            HttpClientWithOptionalRetryUtils.sendDeleteRequest(url, false, headers);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24003,
                    String.format("Failed to delete the old customer %s.", customer), e);
        }
    }

    private List<BasicNameValuePair> loginAndAttach(String customer) throws IOException {
        String url = plsApiHost + "/pls/login";
        Credentials credentials = new Credentials();
        credentials.setUsername(PLS_USRNAME);
        credentials.setPassword(DigestUtils.sha256Hex(PLS_PASSWORD));
        String payload = JsonUtils.serialize(credentials);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(url, false, headers, payload);
        LoginDocument loginDoc = objectMapper.readValue(response, LoginDocument.class);

        List<BasicNameValuePair> authHeaders = new ArrayList<>();
        authHeaders.add(new BasicNameValuePair("Content-Type", "application/json; charset=utf-8"));
        authHeaders.add(new BasicNameValuePair("Accept", "application/json; charset=utf-8"));
        authHeaders.add(new BasicNameValuePair("Authorization", loginDoc.getData()));

        url = plsApiHost + "/pls/attach";
        Tenant tenant = new Tenant();
        tenant.setId(CustomerSpace.parse(customer).toString());
        tenant.setName(customer);
        payload = objectMapper.writeValueAsString(tenant);
        HttpClientWithOptionalRetryUtils.sendPostRequest(url, false, authHeaders, payload);
        return authHeaders;
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
