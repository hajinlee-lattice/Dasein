package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.api.Status;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.ModelActivationResult;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ResponseDocument;
import com.latticeengines.domain.exposed.pls.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl;
import com.latticeengines.pls.service.CrmConstants;
import com.latticeengines.pls.service.CrmCredentialService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.UserService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "internal", description = "REST resource for internal operations")
@RestController
@RequestMapping(value = "/internal")
public class InternalResource extends InternalResourceBase {

    private static final Log LOGGER = LogFactory.getLog(InternalResource.class);

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

    @Value("${pls.test.contract}")
    protected String contractId;

    @Value("${pls.api.hostport}")
    private String hostPort;

    @RequestMapping(value = "/modelsummaries/{modelId}",
        method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a model summary")
    public ResponseDocument<ModelActivationResult> update(
        @PathVariable String modelId,
        @RequestBody AttributeMap attrMap, HttpServletRequest request
    ) {
        checkHeader(request);
        ModelSummary summary = modelSummaryEntityMgr.getByModelId(modelId);

        if (summary == null) {
            ModelActivationResult result = new ModelActivationResult();
            result.setExists(false);
            ResponseDocument<ModelActivationResult> response
                = new ResponseDocument<>();
            response.setSuccess(false);
            response.setResult(result);
            return response;
        }

        ((ModelSummaryEntityMgrImpl) modelSummaryEntityMgr)
            .manufactureSecurityContextForInternalAccess(summary.getTenant());

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

    @RequestMapping(value = "/users",
        method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete users. Mainly for cleaning up testing users.")
    public SimpleBooleanResponse deleteUsers(
        @RequestParam(value = "namepattern") String namePattern,
        @RequestParam(value = "tenants") String tenantIds,
        HttpServletRequest request
    ) throws URIException {
        checkHeader(request);
        String decodedNamePattern = URIUtil.decode(namePattern);

        List<String> tenants = new ArrayList<>();

        JsonNode tenantNodes;
        ObjectMapper mapper = new ObjectMapper();
        try {
            tenantNodes = mapper.readTree(tenantIds);
        } catch (IOException e) {
            return SimpleBooleanResponse.getFailResponse(
                    Collections.singletonList("Could not parse the tenant id array.")
            );
        }
        for (JsonNode node : tenantNodes) {
            tenants.add(node.asText());
        }

        for (String tid: tenants) {
            LOGGER.info(String.format(
                    "Deleting users matching %s from the tenant %s using the internal API",
                    decodedNamePattern, tid
            ));
            for (User user : userService.getUsers(tid)) {
                if (user.getUsername().matches(decodedNamePattern)) {
                    userService.deleteUser(tid, user.getUsername());
                    LOGGER.info(String.format(
                            "User %s has been deleted from the tenant %s through the internal API",
                            user.getUsername(), tid
                    ));
                }
            }
        }

        return SimpleBooleanResponse.getSuccessResponse();
    }


    @RequestMapping(value = "/users",
            method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update users. Mainly for upgrade from old GrantedRights to new AccessLevel.")
    public SimpleBooleanResponse updateUsers(
            @RequestParam(value = "namepattern") String namePattern,
            @RequestParam(value = "tenants") String tenantIds,
            @RequestBody UserUpdateData userUpdateData,
            HttpServletRequest request
    ) throws URIException {
        checkHeader(request);
        String decodedNamePattern = URIUtil.decode(namePattern);
        AccessLevel accessLevel = null;
        if (userUpdateData.getAccessLevel() != null) {
            accessLevel = AccessLevel.valueOf(userUpdateData.getAccessLevel());
        }
        String oldPassword = null;
        if (userUpdateData.getOldPassword() != null) {
            oldPassword = DigestUtils.sha256Hex(userUpdateData.getOldPassword());
        }
        String newPassword = null;
        if (userUpdateData.getNewPassword() != null) {
            newPassword = DigestUtils.sha256Hex(userUpdateData.getNewPassword());
        }

        List<String> tenants = new ArrayList<>();

        JsonNode tenantNodes;
        ObjectMapper mapper = new ObjectMapper();
        try {
            tenantNodes = mapper.readTree(tenantIds);
        } catch (IOException e) {
            return SimpleBooleanResponse.getFailResponse(
                    Collections.singletonList("Could not parse the tenant id array.")
            );
        }
        for (JsonNode node : tenantNodes) {
            tenants.add(node.asText());
        }

        for (String tid: tenants) {
            LOGGER.info(String.format(
                    "Updating users matching %s in the tenant %s using the internal API",
                    decodedNamePattern, tid
            ));

            for (User user : userService.getUsers(tid)) {
                if (user.getUsername().matches(decodedNamePattern)) {
                    if (accessLevel != null) {
                        userService.assignAccessLevel(accessLevel, tid, user.getUsername());
                        LOGGER.info(String.format(
                                "User %s has been updated to %s for the tenant %s through the internal API",
                                user.getUsername(), accessLevel.name(), tid
                        ));
                    }
                    if (oldPassword != null && newPassword != null) {
                        Ticket ticket = globalAuthenticationService.authenticateUser(user.getUsername(), oldPassword);

                        Credentials oldCreds = new Credentials();
                        oldCreds.setUsername(user.getUsername());
                        oldCreds.setPassword(oldPassword);

                        Credentials newCreds = new Credentials();
                        newCreds.setUsername(user.getUsername());
                        newCreds.setPassword(newPassword);
                        globalUserManagementService.modifyLatticeCredentials(ticket, oldCreds, newCreds);

                        LOGGER.info(String.format(
                                "The password of user %s has been updated through the internal API", user.getUsername()
                        ));

                        globalAuthenticationService.discard(ticket);
                    }
                }
            }
        }

        return SimpleBooleanResponse.getSuccessResponse();
    }


    @RequestMapping(value = "/testtenants", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Reset the testing environment for protracter tests.")
    public SimpleBooleanResponse createTestTenant(HttpServletRequest request) throws IOException {
        checkHeader(request);
        LOGGER.info("Cleaning up test tenants through internal API");

        final String tenant1Id = contractId + "PLSContract.Tenant1.Production";
        final String tenant2Id = contractId + "PLSContract.Tenant2.Production";

        //==================================================
        // Upload modelsummary if necessary
        //==================================================
        RestTemplate restTemplate = new RestTemplate();
        Credentials creds = new Credentials();
        creds.setUsername("pls-super-admin-tester@test.lattice-engines.com");
        creds.setPassword(DigestUtils.sha256Hex("admin"));
        final LoginDocument loginDoc = restTemplate.postForObject(hostPort + "/pls/login", creds, LoginDocument.class);
        ClientHttpRequestInterceptor interceptor = new ClientHttpRequestInterceptor() {
            @Override
            public ClientHttpResponse intercept(HttpRequest httpRequest, byte[] bytes, ClientHttpRequestExecution execution)
                    throws IOException {
                HttpRequestWrapper requestWrapper = new HttpRequestWrapper(httpRequest);
                requestWrapper.getHeaders().add(Constants.AUTHORIZATION, loginDoc.getData());

                return execution.execute(requestWrapper, bytes);
            }
        };
        restTemplate.setInterceptors(Collections.singletonList(interceptor));
        for (Tenant tenant: loginDoc.getResult().getTenants()) {
            if (tenant.getId().equals(tenant1Id) || tenant.getId().equals(tenant2Id)) {
                restTemplate.postForObject(hostPort + "/pls/attach", tenant, UserDocument.class);
                List response = restTemplate.getForObject(hostPort + "/pls/modelsummaries/", List.class);
                while (response.size() < 2) {
                    InputStream ins = getClass().getClassLoader()
                            .getResourceAsStream("com/latticeengines/pls/controller/internal/modelsummary-eloqua.json");
                    ModelSummary data = new ModelSummary();
                    Tenant fakeTenant = new Tenant();
                    fakeTenant.setId("FAKE_TENANT");
                    fakeTenant.setName("Fake Tenant");
                    fakeTenant.setPid(-1L);
                    data.setTenant(fakeTenant);
                    data.setRawFile(new String(IOUtils.toByteArray(ins)));
                    restTemplate.postForObject(hostPort + "/pls/modelsummaries?raw=true", data, ModelSummary.class);
                    response = restTemplate.getForObject(hostPort + "/pls/modelsummaries/", List.class);
                }
            }
        }

        //==================================================
        // Cleanup stored credentials
        //==================================================
        crmCredentialService.removeCredentials(CrmConstants.CRM_SFDC, tenant1Id, true);
        crmCredentialService.removeCredentials(CrmConstants.CRM_SFDC, tenant1Id, false);
        crmCredentialService.removeCredentials(CrmConstants.CRM_ELOQUA, tenant1Id, true);
        crmCredentialService.removeCredentials(CrmConstants.CRM_MARKETO, tenant1Id, true);
        crmCredentialService.removeCredentials(CrmConstants.CRM_SFDC, tenant2Id, true);
        crmCredentialService.removeCredentials(CrmConstants.CRM_SFDC, tenant2Id, false);
        crmCredentialService.removeCredentials(CrmConstants.CRM_ELOQUA, tenant2Id, true);
        crmCredentialService.removeCredentials(CrmConstants.CRM_MARKETO, tenant2Id, true);

        return SimpleBooleanResponse.getSuccessResponse();
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
}
