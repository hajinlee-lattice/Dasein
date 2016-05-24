package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.ModelActivationResult;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.CrmCredentialService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.service.TargetMarketService;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.EmailService;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.workflow.exposed.service.ReportService;
import com.wordnik.swagger.annotations.ApiParam;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "internal", description = "REST resource for internal operations")
@RestController
@RequestMapping(value = "/internal")
public class InternalResource extends InternalResourceBase {

    protected static final String EXTERNAL_USER_USERNAME_1 = "pls-external-user-tester-1@test.lattice-engines.ext";
    private static final Log log = LogFactory.getLog(InternalResource.class);
    private static final String passwordTester = "pls-password-tester@test.lattice-engines.ext";
    private static final String passwordTesterPwd = "Lattice123";
    private static final String adminTester = "pls-super-admin-tester@test.lattice-engines.com";
    private static final String adminTesterPwd = "admin";
    private static final String adUsername = "testuser1";
    private static final String adPassword = "Lattice1";
    private static final String TENANT_ID_PATH = "{tenantId:\\w+\\.\\w+\\.\\w+}";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final String UTC = "UTC";

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private UserService userService;

    @Autowired
    private CrmCredentialService crmCredentialService;

    @Autowired
    private TenantConfigService tenantConfigService;

    @Autowired
    private InternalTestUserService internalTestUserService;

    @Autowired
    private TargetMarketService targetMarketService;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private ReportService reportService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private EmailService emailService;

    @Autowired
    private VersionManager versionManager;

    @Value("${pls.test.contract}")
    protected String contractId;

    @Value("${pls.api.hostport}")
    private String hostPort;

    @Value("${pls.internal.admin.api}")
    private String adminApi;

    @Value("${pls.test.tenant.reg.json}")
    private String testTenantRegJson;

    @Value("${pls.test.deployment.reset.by.admin:true}")
    private boolean resetByAdminApi;

    @Value("${security.app.public.url:http://localhost:8081}")
    private String appPublicUrl;

    @Value("${pls.current.stack:}")
    private String currentStack;

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    @RequestMapping(value = "/targetmarkets/default/"
            + TENANT_ID_PATH, method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create default target market")
    public void createDefaultTargetMarket(@PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        targetMarketService.createDefaultTargetMarket();
    }

    @RequestMapping(value = "/targetmarkets/{targetMarketName}/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Find target market by name")
    public TargetMarket findTargetMarketByName(@PathVariable("targetMarketName") String targetMarketName,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        return targetMarketService.findTargetMarketByName(targetMarketName);
    }

    @RequestMapping(value = "/targetmarkets/{targetMarketName}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update target market")
    public void updateTargetMarket(@PathVariable("targetMarketName") String targetMarketName,
            @PathVariable("tenantId") String tenantId, @RequestBody TargetMarket targetMarket,
            HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        targetMarketService.updateTargetMarketByName(targetMarket, targetMarketName);
    }

    @RequestMapping(value = "/targetmarkets/{targetMarketName}/"
            + TENANT_ID_PATH, method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a target market")
    public void deleteTargetMarketByName(@PathVariable("targetMarketName") String targetMarketName,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        targetMarketService.deleteTargetMarketByName(targetMarketName);
    }

    @RequestMapping(value = "/targetmarkets/"
            + TENANT_ID_PATH, method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a target market")
    public void deleteAllTargetMarkets(@PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        targetMarketService.deleteAll();
    }

    @RequestMapping(value = "/targetmarkets/{targetMarketName}/reports/"
            + TENANT_ID_PATH, method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a target market report")
    public void registerReport(@PathVariable("targetMarketName") String targetMarketName,
            @PathVariable("tenantId") String tenantId, @RequestBody Report report, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        targetMarketService.registerReport(targetMarketName, report);
    }

    @RequestMapping(value = "/reports/"
            + TENANT_ID_PATH, method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a report")
    public void registerReport(@PathVariable("tenantId") String tenantId, @RequestBody Report report,
            HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        reportService.createOrUpdateReport(report);
    }

    @RequestMapping(value = "/reports/{reportName}/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve a Report")
    public Report findReportByName(@PathVariable("reportName") String reportName,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        return reportService.getReportByName(reportName);
    }

    @RequestMapping(value = "/sourcefiles/{sourceFileName}/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve a SourceFile")
    public SourceFile findSourceFileByName(@PathVariable("sourceFileName") String sourceFileName,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        return sourceFileService.findByName(sourceFileName);
    }

    @RequestMapping(value = "/sourcefiles/{sourceFileName}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a SourceFile")
    public void updateSourceFile(@PathVariable("sourceFileName") String sourceFileName,
            @PathVariable("tenantId") String tenantId, @RequestBody SourceFile sourceFile, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        sourceFileService.update(sourceFile);
    }

    @RequestMapping(value = "/sourcefiles/{sourceFileName}/"
            + TENANT_ID_PATH, method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a SourceFile")
    public void createSourceFile(@PathVariable("sourceFileName") String sourceFileName,
            @PathVariable("tenantId") String tenantId, @RequestBody SourceFile sourceFile, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        sourceFileService.create(sourceFile);
    }

    @RequestMapping(value = "/modelsummaries/"
            + TENANT_ID_PATH, method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a ModelSummary")
    public void createModelSummary(@PathVariable("tenantId") String tenantId, @RequestBody ModelSummary modelSummary,
            HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        modelSummaryService.createModelSummary(modelSummary, tenantId);
    }

    @RequestMapping(value = "/modelsummaries/{modelId}/"
            + TENANT_ID_PATH, method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a model summary")
    public Boolean deleteModelSummary(@PathVariable String modelId, @PathVariable("tenantId") String tenantId,
            HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        modelSummaryEntityMgr.deleteByModelId(modelId);
        return true;
    }

    @RequestMapping(value = "/modelsummaries/active/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all active model summaries")
    public List<ModelSummary> getActiveModelSummaries(@PathVariable("tenantId") String tenantId,
            HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAllActive();

        for (ModelSummary summary : summaries) {
            summary.setPredictors(new ArrayList<Predictor>());
            summary.setDetails(null);
        }

        return summaries;
    }

    @RequestMapping(value = "/modelsummarydetails/count/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all model summaries count")
    public int getModelSummariesCount(
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = false) @RequestParam(value = "start", required = false) String start,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) throws ParseException {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        long lastUpdateTime = 0;
        if (!StringUtils.isEmpty(start)) {
            lastUpdateTime = dateFormat.parse(start).getTime();
        }
        return modelSummaryEntityMgr.getTotalCount(lastUpdateTime, considerAllStatus);

    }

    @RequestMapping(value = "/modelsummaries/{applicationId}/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a model summary by applicationId")
    public ModelSummary findModelSummaryByAppId(@PathVariable("applicationId") String applicationId,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        return modelSummaryEntityMgr.findByApplicationId(applicationId);
    }

    @RequestMapping(value = "/modelsummaries/modelid/{modelId}/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a model summary by modelId")
    public ModelSummary findModelSummaryByModelId(@PathVariable("modelId") String modelId,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        return modelSummaryService.getModelSummaryEnrichedByDetails(modelId);
    }

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

        manufactureSecurityContextForInternalAccess(summary.getTenant());

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

    @RequestMapping(value = "/emails/createmodel/result/{result}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after model creation")
    public void sendPlsCreateModelEmail(@PathVariable("result") String result,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        for (User user : users) {
            if (user.getAccessLevel().equals(AccessLevel.EXTERNAL_ADMIN.name())) {
                if (result.equals("COMPLETED")) {
                    emailService.sendPlsCreateModelCompletionEmail(user, appPublicUrl);
                } else {
                    emailService.sendPlsCreateModelErrorEmail(user, appPublicUrl);
                }
            }
        }
    }

    @RequestMapping(value = "/emails/score/result/{result}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after scoring")
    public void sendPlsScoreEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
            HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        for (User user : users) {
            if (user.getAccessLevel().equals(AccessLevel.EXTERNAL_ADMIN.name())) {
                if (result.equals("COMPLETED")) {
                    emailService.sendPlsScoreCompletionEmail(user, appPublicUrl);
                } else {
                    emailService.sendPlsScoreErrorEmail(user, appPublicUrl);
                }
            }
        }
    }

    @RequestMapping(value = "/currentstack", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get current active stack")
    public Map<String, String> getActiveStack() {
        Map<String, String> response = new HashMap<>();
        response.put("CurrentStack", currentStack);
        response.put("ArtifactVersion", versionManager.getCurrentVersion());
        return response;
    }

    @RequestMapping(value = "/testtenants", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Reset the testing environment for protractor tests.")
    public SimpleBooleanResponse createTestTenant(
            @RequestParam(value = "forceinstall", required = false, defaultValue = "false") Boolean forceInstallation,
            HttpServletRequest request) throws IOException {
        checkHeader(request);
        String productPrefix = request.getParameter("product");

        String jsonFileName = testTenantRegJson;
        if (productPrefix != null && !jsonFileName.startsWith(productPrefix)) {
            jsonFileName = productPrefix + "-" + jsonFileName;
        }
        log.info("Cleaning up test tenants through internal API");

        List<String> testTenantIds = getTestTenantIds();
        final String tenant1Id = testTenantIds.get(0);
        final String tenant2Id = testTenantIds.get(1);

        // ==================================================
        // Provision through tenant console if needed
        // ==================================================
        if (forceInstallation) {

            provisionThroughTenantConsole(tenant1Id, "Marketo", jsonFileName);
            provisionThroughTenantConsole(tenant2Id, "Eloqua", jsonFileName);

            waitForTenantConsoleInstallation(CustomerSpace.parse(tenant1Id));
            waitForTenantConsoleInstallation(CustomerSpace.parse(tenant2Id));

        } else {
            if (StringUtils.isEmpty(productPrefix)) {
                Camille camille = CamilleEnvironment.getCamille();
                Path productsPath = PathBuilder
                        .buildCustomerSpacePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant1Id))
                        .append("SpaceConfiguration").append("Products");
                try {
                    camille.upsert(productsPath,
                            new Document(JsonUtils.serialize(Collections.singleton(LatticeProduct.LPA.getName()))),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                productsPath = PathBuilder
                        .buildCustomerSpacePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant2Id))
                        .append("SpaceConfiguration").append("Products");
                try {
                    camille.upsert(productsPath,
                            new Document(JsonUtils.serialize(Collections.singleton(LatticeProduct.LPA.getName()))),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                tenantConfigService.getTopology(tenant1Id);
            } catch (LedpException e) {
                try {
                    provisionThroughTenantConsole(tenant1Id, "Marketo", jsonFileName);
                } catch (Exception ex) {
                    // do not interrupt, functional test could fail on this
                    log.warn("Provision " + tenant1Id + " as a Marketo tenant failed: " + e.getMessage());
                }
            }

            try {
                tenantConfigService.getTopology(tenant2Id);
            } catch (LedpException e) {
                try {
                    provisionThroughTenantConsole(tenant2Id, "Eloqua", jsonFileName);
                } catch (Exception ex) {
                    // do not interrupt, functional test could fail on this
                    log.warn("Provision " + tenant1Id + " as a Marketo tenant failed: " + e.getMessage());
                }
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
        if (!forceInstallation) {
            for (Tenant tenant : loginDoc.getResult().getTenants()) {
                if (tenant.getId().equals(tenant2Id)) {
                    log.info("Checking models for tenant " + tenant.getId());
                    payload = JsonUtils.serialize(tenant);
                    HttpClientWithOptionalRetryUtils.sendPostRequest(getHostPort() + "/pls/attach", true, headers,
                            payload);
                    String response = HttpClientWithOptionalRetryUtils
                            .sendGetRequest(getHostPort() + "/pls/modelsummaries?selection=all", true, headers);
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jNode = mapper.readTree(response);
                    log.info("Found " + jNode.size() + " models for " + tenant.getId());
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
                        HttpClientWithOptionalRetryUtils.sendPostRequest(getHostPort() + "/pls/modelsummaries?raw=true",
                                true, headers, JsonUtils.serialize(data));
                        response = HttpClientWithOptionalRetryUtils
                                .sendGetRequest(getHostPort() + "/pls/modelsummaries", true, headers);
                        jNode = mapper.readTree(response);
                        log.info("Uploaded a model to " + tenant.getId() + ". Now there are " + jNode.size()
                                + " models");
                    }
                    for (JsonNode modelNode : jNode) {
                        ModelSummary data = mapper.treeToValue(modelNode, ModelSummary.class);
                        ModelSummaryStatus status = data.getStatus();
                        if (ModelSummaryStatus.DELETED.equals(status)) {
                            log.info("Found a deleted model " + data.getId());
                            String modelApi = getHostPort() + "/pls/modelsummaries/" + data.getId();
                            payload = "{ \"Status\": \"UpdateAsInactive\" }";
                            HttpClientWithOptionalRetryUtils.sendPutRequest(modelApi, false, headers, payload);
                            log.info("Update model " + data.getId() + " to inactive.");
                        }
                    }
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

        List<Tenant> testTenants = new ArrayList<>();
        for (String tenantId : testTenantIds) {
            Tenant tenant = new Tenant();
            tenant.setId(tenantId);
            tenant.setName(tenant1Id);
            testTenants.add(tenant);
        }

        Map<AccessLevel, User> accessLevelToUsers = internalTestUserService
                .createAllTestUsersIfNecessaryAndReturnStandardTestersAtEachAccessLevel(testTenants);

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

    public List<String> getTestTenantIds() {
        String tenant1Id = contractId + "PLSTenant1." + contractId + "PLSTenant1.Production";
        String tenant2Id = contractId + "PLSTenant2." + contractId + "PLSTenant2.Production";
        return Arrays.asList(tenant1Id, tenant2Id);
    }

    private void provisionThroughTenantConsole(String tupleId, String topology, String tenantRegJson)
            throws IOException {
        if (resetByAdminApi) {
            List<BasicNameValuePair> adHeaders = loginAd();

            String tenantToken = "${TENANT}";
            String topologyToken = "${TOPOLOGY}";
            String dlTenantName = CustomerSpace.parse(tupleId).getTenantId();
            InputStream ins = getClass().getClassLoader()
                    .getResourceAsStream("com/latticeengines/pls/controller/internal/" + tenantRegJson);
            String payload = IOUtils.toString(ins);
            payload = payload.replace(tenantToken, dlTenantName).replace(topologyToken, topology);
            HttpClientWithOptionalRetryUtils.sendPostRequest(
                    adminApi + "/tenants/" + dlTenantName + "?contractId=" + dlTenantName, false, adHeaders, payload);
        } else {
            throw new RuntimeException(
                    "We need to add the request tenant into ZK, but we do not have AD credentials in the environment. "
                            + tupleId);
        }
    }

    private void waitForTenantConsoleInstallation(CustomerSpace customerSpace) {
        long timeout = 1800000L; // bardjams has a long long timeout
        long totTime = 0L;
        String url = adminApi + "/tenants/" + customerSpace.getTenantId() + "?contractId="
                + customerSpace.getContractId();
        BootstrapState state = BootstrapState.createInitialState();
        while (!BootstrapState.State.OK.equals(state.state) && !BootstrapState.State.ERROR.equals(state.state)
                && totTime <= timeout) {
            try {
                List<BasicNameValuePair> adHeaders = loginAd();
                String jsonResponse = HttpClientWithOptionalRetryUtils.sendGetRequest(url, false, adHeaders);
                log.info("JSON response from tenant console: " + jsonResponse);
                TenantDocument tenantDocument = JsonUtils.deserialize(jsonResponse, TenantDocument.class);
                BootstrapState newState = tenantDocument.getBootstrapState();
                state = newState == null ? state : newState;
            } catch (IOException e) {
                throw new RuntimeException("Failed to query tenant installation state", e);
            } finally {
                try {
                    Thread.sleep(5000L);
                    totTime += 5000L;
                } catch (InterruptedException e) {
                    log.error(e);
                }
            }
        }

        if (!BootstrapState.State.OK.equals(state.state)) {
            throw new IllegalArgumentException("The tenant state is not OK after " + timeout + " msec.");
        }
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
                userService.assignAccessLevel(accessLevelToUser.getKey(), testTenantId,
                        accessLevelToUser.getValue().getUsername());
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

    private void manufactureSecurityContextForInternalAccess(String tenantId) {
        log.info("Manufacturing security context for " + tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_18074, new String[] { tenantId });
        }
        manufactureSecurityContextForInternalAccess(tenant);
    }

    private void manufactureSecurityContextForInternalAccess(Tenant tenant) {
        TicketAuthenticationToken auth = new TicketAuthenticationToken(null, "x.y");
        Session session = new Session();
        session.setTenant(tenant);
        auth.setSession(session);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }
}
