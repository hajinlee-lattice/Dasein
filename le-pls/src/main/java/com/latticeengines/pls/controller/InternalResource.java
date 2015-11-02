package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.api.Status;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.ModelActivationResult;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl;
import com.latticeengines.pls.service.CrmCredentialService;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.exposed.service.UserService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "internal", description = "REST resource for internal operations")
@RestController
@RequestMapping(value = "/internal")
public class InternalResource extends InternalResourceBase {

    private static final Log LOGGER = LogFactory.getLog(InternalResource.class);
    private static final String passwordTester = "pls-password-tester@test.lattice-engines.ext";
    private static final String passwordTesterPwd = "Lattice123";
    private static final String adminTester = "pls-super-admin-tester@test.lattice-engines.com";
    private static final String adminTesterPwd = "admin";
    private static final String adUsername = "testuser1";
    private static final String adPassword = "Lattice1";
    protected static final String EXTERNAL_USER_USERNAME_1 = "pls-external-user-tester-1@test.lattice-engines.ext";

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private UserService userService;

    @Autowired
    private CrmCredentialService crmCredentialService;

    @Autowired
    private TenantConfigService tenantConfigService;

    @Autowired
    private InternalTestUserService internalTestUserService;

    @Value("${pls.test.contract}")
    protected String contractId;

    @Value("${pls.api.hostport}")
    private String hostPort;

    @Value("${pls.internal.admin.api}")
    private String adminApi;

    @RequestMapping(value = "/modelsummaries/{modelId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a model summary")
    public ResponseDocument<ModelActivationResult> update(@PathVariable String modelId,
            @RequestBody AttributeMap attrMap, HttpServletRequest request) {
        checkHeader(request);
        ModelSummary summary = modelSummaryEntityMgr.getByModelId(modelId);

        if (summary == null) {
            ModelActivationResult result = new ModelActivationResult();
            result.setExists(false);
            ResponseDocument<ModelActivationResult> response = new ResponseDocument<>();
            response.setSuccess(false);
            response.setResult(result);
            return response;
        }

        ((ModelSummaryEntityMgrImpl) modelSummaryEntityMgr).manufactureSecurityContextForInternalAccess(summary
                .getTenant());

        // Reuse the logic in the ModelSummaryResource to do the updates
        ModelSummaryResource msr = new ModelSummaryResource();
        msr.setModelSummaryEntityMgr(modelSummaryEntityMgr);
        ModelActivationResult result = new ModelActivationResult();
        result.setExists(true);
        ResponseDocument<ModelActivationResult> response = new ResponseDocument<>();
        response.setResult(result);
        if (msr.update(modelId, attrMap)) {
            response.setSuccess(true);
        } else {
            response.setSuccess(false);
        }
        return response;
    }

    @RequestMapping(value = "/testtenants", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Reset the testing environment for protractor tests.")
    public SimpleBooleanResponse createTestTenant(HttpServletRequest request) throws IOException {
        checkHeader(request);
        LOGGER.info("Cleaning up test tenants through internal API");

        List<String> Ids = getTestTenantIds();
        final String tenant1Id = Ids.get(0);
        final String tenant2Id = Ids.get(1);

        // ==================================================
        // Provision through tenant console if needed
        // ==================================================
        try {
            tenantConfigService.getTopology(tenant1Id);
        } catch (LedpException e) {
            try {
                provisionThroughTenantConsole(tenant1Id, "Marketo");
            } catch (Exception ex) {
                // do not interrupt, functional test could fail on this
                LOGGER.warn("Provision " + tenant1Id + " as a Marketo tenant failed: " + e.getMessage());
            }
        }

        try {
            tenantConfigService.getTopology(tenant2Id);
        } catch (LedpException e) {
            try {
                provisionThroughTenantConsole(tenant2Id, "Eloqua");
            } catch (Exception ex) {
                // do not interrupt, functional test could fail on this
                LOGGER.warn("Provision " + tenant1Id + " as a Marketo tenant failed: " + e.getMessage());
            }
        }

        // ==================================================
        // Upload modelsummary if necessary
        // ==================================================
        Credentials creds = new Credentials();
        creds.setUsername(adminTester);
        creds.setPassword(DigestUtils.sha256Hex(adminTesterPwd));

        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("Content-Type", "application/json"));
        headers.add(new BasicNameValuePair("Accept", "application/json"));

        String payload = JsonUtils.serialize(creds);
        String loginDocAsString = HttpClientWithOptionalRetryUtils.sendPostRequest(getHostPort() + "/pls/login", true,
                headers, payload);
        LoginDocument loginDoc = JsonUtils.deserialize(loginDocAsString, LoginDocument.class);

        headers.add(new BasicNameValuePair(Constants.AUTHORIZATION, loginDoc.getData()));

        for (Tenant tenant : loginDoc.getResult().getTenants()) {
            if (tenant.getId().equals(tenant2Id)) {
                payload = JsonUtils.serialize(tenant);
                HttpClientWithOptionalRetryUtils.sendPostRequest(getHostPort() + "/pls/attach", true, headers, payload);
                String response = HttpClientWithOptionalRetryUtils.sendGetRequest(getHostPort() + "/pls/modelsummaries",
                        true, headers);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jNode = mapper.readTree(response);
                while (jNode.size() < 2) {
                    InputStream ins = getClass().getClassLoader().getResourceAsStream(
                            "com/latticeengines/pls/controller/internal/modelsummary-eloqua.json");
                    ModelSummary data = new ModelSummary();
                    Tenant fakeTenant = new Tenant();
                    fakeTenant.setId("FAKE_TENANT");
                    fakeTenant.setName("Fake Tenant");
                    fakeTenant.setPid(-1L);
                    data.setTenant(fakeTenant);
                    data.setRawFile(new String(IOUtils.toByteArray(ins)));
                    HttpClientWithOptionalRetryUtils.sendPostRequest(getHostPort() + "/pls/modelsummaries?raw=true", true,
                            headers, JsonUtils.serialize(data));
                    response = HttpClientWithOptionalRetryUtils.sendGetRequest(getHostPort() + "/pls/modelsummaries", true,
                            headers);
                    jNode = mapper.readTree(response);
                }
            }
        }

        // ==================================================
        // Delete test users
        // ==================================================
        for (User user : userService.getUsers(tenant1Id)) {
            if (user.getUsername().indexOf("0tempuser") > 0) {
                userService.deleteUser(tenant1Id, user.getUsername());
            }
        }
        for (User user : userService.getUsers(tenant2Id)) {
            if (user.getUsername().indexOf("0tempuser") > 0) {
                userService.deleteUser(tenant2Id, user.getUsername());
            }
        }

        Map<AccessLevel, User> accessLevelToUsers = internalTestUserService
                .createAllTestUsersIfNecessaryAndReturnStandardTestersAtEachAccessLevel();

        // ==================================================
        // Reset password of password tester
        // ==================================================
        resetPasswordTester();

        // ==================================================
        // Cleanup stored credentials
        // ==================================================
        crmCredentialService.removeCredentials(CrmConstants.CRM_SFDC, tenant1Id, true);
        crmCredentialService.removeCredentials(CrmConstants.CRM_SFDC, tenant1Id, false);
        crmCredentialService.removeCredentials(CrmConstants.CRM_ELOQUA, tenant1Id, true);
        crmCredentialService.removeCredentials(CrmConstants.CRM_MARKETO, tenant1Id, true);
        crmCredentialService.removeCredentials(CrmConstants.CRM_SFDC, tenant2Id, true);
        crmCredentialService.removeCredentials(CrmConstants.CRM_SFDC, tenant2Id, false);
        crmCredentialService.removeCredentials(CrmConstants.CRM_ELOQUA, tenant2Id, true);
        crmCredentialService.removeCredentials(CrmConstants.CRM_MARKETO, tenant2Id, true);

        assignTestingUsersToTenants(accessLevelToUsers);

        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/{op}/{left}/{right}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Status check for this endpoint")
    public Status calculate(@PathVariable("op") String op, @PathVariable("left") int left,
            @PathVariable("right") int right) {
        Assert.notNull(op);
        Assert.notNull(left);
        Assert.notNull(right);
        Status result = new Status();
        result.setOperation(op);
        result.setLeft(left);
        result.setRight(right);
        return doCalc(result);
    }

    public List<String> getTestTenantIds() {
        String tenant1Id = contractId + "PLSTenant1." + contractId + "PLSTenant1.Production";
        String tenant2Id = contractId + "PLSTenant2." + contractId + "PLSTenant2.Production";
        return Arrays.asList(tenant1Id, tenant2Id);
    }

    private Status doCalc(Status c) {
        String op = c.getOperation();
        int left = c.getLeft();
        int right = c.getRight();
        if (op.equalsIgnoreCase("subtract")) {
            c.setResult(left - right);
        } else if (op.equalsIgnoreCase("multiply")) {
            c.setResult(left * right);
        } else if (op.equalsIgnoreCase("divide")) {
            c.setResult(left / right);
        } else {
            c.setResult(left + right);
        }
        return c;
    }

    private void provisionThroughTenantConsole(String tupleId, String topology) throws IOException {
        List<BasicNameValuePair> adHeaders = loginAd();

        String tenantToken = "${TENANT}";
        String topologyToken = "${TOPOLOGY}";
        String dlTenantName = CustomerSpace.parse(tupleId).getTenantId();
        InputStream ins = getClass().getClassLoader().getResourceAsStream(
                "com/latticeengines/pls/controller/internal/tenant-registration.json");
        String payload = IOUtils.toString(ins);
        payload = payload.replace(tenantToken, dlTenantName).replace(topologyToken, topology);
        HttpClientWithOptionalRetryUtils.sendPostRequest(adminApi + "/tenants/" + dlTenantName + "?contractId="
                + dlTenantName, false, adHeaders, payload);
    }

    private List<BasicNameValuePair> loginAd() throws IOException {
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("Content-Type", "application/json"));
        headers.add(new BasicNameValuePair("Accept", "application/json"));

        Credentials credentials = new Credentials();
        credentials.setUsername(adUsername);
        credentials.setPassword(adPassword);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(adminApi + "/adlogin", false, headers,
                JsonUtils.serialize(credentials));

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(response);
        String token = json.get("Token").asText();

        headers.add(new BasicNameValuePair("Authorization", token));
        return headers;
    }

    private void assignTestingUsersToTenants(Map<AccessLevel, User> accessLevelToUsers) {
        for (String testTenantId : getTestTenantIds()) {
            for (Map.Entry<AccessLevel, User> accessLevelToUser : accessLevelToUsers.entrySet()) {
                userService.assignAccessLevel(accessLevelToUser.getKey(), testTenantId, accessLevelToUser.getValue()
                        .getUsername());
            }
            userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testTenantId, passwordTester);
        }
        userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, getTestTenantIds().get(0), EXTERNAL_USER_USERNAME_1);
        userService.deleteUser(getTestTenantIds().get(1), EXTERNAL_USER_USERNAME_1);
    }

    private void resetPasswordTester() {
        String tempPwd = globalUserManagementService.resetLatticeCredentials(passwordTester);
        Ticket ticket = globalAuthenticationService.authenticateUser(passwordTester, DigestUtils.sha256Hex(tempPwd));

        Credentials oldCreds = new Credentials();
        oldCreds.setUsername(passwordTester);
        oldCreds.setPassword(DigestUtils.sha256Hex(tempPwd));
        Credentials newCreds = new Credentials();
        newCreds.setUsername(passwordTester);
        newCreds.setPassword(DigestUtils.sha256Hex(passwordTesterPwd));
        globalUserManagementService.modifyLatticeCredentials(ticket, oldCreds, newCreds);
    }

    private String getHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }
}
