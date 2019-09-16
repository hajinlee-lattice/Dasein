package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.app.exposed.controller.LatticeInsightsResource;
import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationLevel;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationType;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.pls.service.CrmCredentialService;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.pls.service.ModelNoteService;
import com.latticeengines.pls.service.ScoringRequestConfigService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "internal", description = "REST resource for internal operations")
@RestController
@RequestMapping(value = "/internal")
public class InternalResource extends InternalResourceBase {

    private static final String EXTERNAL_USER_USERNAME_1 = "pls-external-user-tester-1@test.lattice-engines.ext";
    private static final Logger log = LoggerFactory.getLogger(InternalResource.class);
    private static final String passwordTester = "pls-password-tester@test.lattice-engines.ext";
    private static final String passwordTesterPwd = "Lattice123";
    private static final String adminTester = "pls-super-admin-tester@test.lattice-engines.com";
    private static final String adminTesterPwd = "admin";
    private static final String adUsername = "testuser1";
    private static final String adPassword = "Lattice1";
    public static final String TENANT_ID_PATH = "{tenantId:\\w+\\.\\w+\\.\\w+}";

    @Inject
    private GlobalAuthenticationService globalAuthenticationService;

    @Inject
    private GlobalUserManagementService globalUserManagementService;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private AttributeService attributeService;

    @Inject
    private UserService userService;

    @Inject
    private CrmCredentialService crmCredentialService;

    @Inject
    private TenantConfigService tenantConfigService;

    @Inject
    private InternalTestUserService internalTestUserService;

    @Inject
    private TenantService tenantService;

    @Inject
    private ReportService reportService;

    @Inject
    private SourceFileService sourceFileService;

    @Inject
    private EmailService emailService;

    @Inject
    private ModelNoteService modelNoteService;

    @Inject
    private MetadataSegmentService metadataSegmentService;

    @Inject
    private MetadataSegmentExportService metadataSegmentExportService;

    @Inject
    private WorkflowJobService workflowJobService;

    @Inject
    private ScoringRequestConfigService scoringRequestConfigService;

    @Value("${pls.test.contract}")
    protected String contractId;

    @Value("${common.pls.url}")
    private String hostPort;

    @Value("${common.admin.url}")
    private String adminApi;

    @Value("${pls.test.tenant.reg.json}")
    private String testTenantRegJson;

    @Value("${pls.test.deployment.reset.by.admin:true}")
    private boolean resetByAdminApi;

    @Value("${security.app.public.url:http://localhost:8081}")
    private String appPublicUrl;

    @RequestMapping(value = "/reports/"
            + TENANT_ID_PATH, method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a report")
    public void registerReport(@PathVariable("tenantId") String tenantId, @RequestBody Report report,
            HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        reportService.createOrUpdateReport(report);
        log.info(String.format(
                "Registered a report for tenant=%s, name=%s, purpose=%s, payload=%s", report.getTenant().getId(),
                report.getName(), report.getPurpose().name(),
                report.getJson().getPayload()));
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

    @RequestMapping(value = "/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/categories" + "/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of categories")
    public List<String> getLeadEnrichmentCategories(HttpServletRequest request, //
            @PathVariable("tenantId") String tenantId) {
        List<LeadEnrichmentAttribute> allAttributes = getLeadEnrichmentAttributes(request, tenantId, null, null, null,
                false, null, null, Boolean.FALSE);

        List<String> categoryStrList = new ArrayList<>();
        for (Category category : Category.values()) {
            if (containsAtleastOneAttributeForCategory(allAttributes, category)) {
                categoryStrList.add(category.toString());
            }
        }
        return categoryStrList;
    }

    @RequestMapping(value = "/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/subcategories" + "/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of subcategories for a given category")
    public List<String> getLeadEnrichmentSubcategories(HttpServletRequest request, //
            @PathVariable("tenantId") String tenantId, //
            @ApiParam(value = "category", required = true) //
            @RequestParam String category) {
        Set<String> subcategories = new HashSet<>();
        List<LeadEnrichmentAttribute> allAttributes = getLeadEnrichmentAttributes(request, tenantId, null, category,
                null, false, null, null, Boolean.FALSE);

        for (LeadEnrichmentAttribute attr : allAttributes) {
            subcategories.add(attr.getSubcategory());
        }
        return new ArrayList<>(subcategories);
    }

    @RequestMapping(value = "/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get lead enrichment")
    public List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(HttpServletRequest request, //
            @PathVariable("tenantId") String tenantId, //
            @ApiParam(value = "Get attributes with display name containing specified " //
                    + "text (case insensitive) for attributeDisplayNameFilter", required = false) //
            @RequestParam(value = "attributeDisplayNameFilter", required = false) //
            String attributeDisplayNameFilter, //
            @ApiParam(value = "Get attributes " //
                    + "with specified category", required = false) //
            @RequestParam(value = "category", required = false) //
            String category, //
            @ApiParam(value = "Get attributes " //
                    + "with specified subcategory", required = false) //
            @RequestParam(value = "subcategory", required = false) //
            String subcategory, //
            @ApiParam(value = "Should get only selected attribute", //
                    required = false) //
            @RequestParam(value = "onlySelectedAttributes", required = false) //
            Boolean onlySelectedAttributes, //
            @ApiParam(value = "Offset for pagination of matching attributes", required = false) //
            @RequestParam(value = "offset", required = false) //
            Integer offset, //
            @ApiParam(value = "Maximum number of matching attributes in page", required = false) //
            @RequestParam(value = "max", required = false) //
            Integer max, //
            @ApiParam(value = "Consider internal attributes", required = false) //
            @RequestParam(value = "considerInternalAttributes", required = false, defaultValue = "false") //
            Boolean considerInternalAttributes) {
        checkHeader(request);
        Tenant tenant = manufactureSecurityContextForInternalAccess(tenantId);
        Category categoryEnum = (StringStandardizationUtils.objectIsNullOrEmptyString(category) ? null
                : Category.fromName(category));
        return attributeService.getAttributes(tenant, attributeDisplayNameFilter, categoryEnum, subcategory,
                onlySelectedAttributes, offset, max, considerInternalAttributes);
    }

    @RequestMapping(value = "/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/" + "count" + "/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get lead enrichment")
    public int getLeadEnrichmentAttributesCount(HttpServletRequest request, //
            @PathVariable("tenantId") String tenantId, //
            @ApiParam(value = "Get attributes with display name containing specified " //
                    + "text (case insensitive) for attributeDisplayNameFilter", required = false) //
            @RequestParam(value = "attributeDisplayNameFilter", required = false) //
            String attributeDisplayNameFilter, //
            @ApiParam(value = "Get attributes " //
                    + "with specified category", required = false) //
            @RequestParam(value = "category", required = false) //
            String category, //
            @ApiParam(value = "Get attributes " //
                    + "with specified subcategory", required = false) //
            @RequestParam(value = "subcategory", required = false) //
            String subcategory, //
            @ApiParam(value = "Should get only selected attribute", //
                    required = false) //
            @RequestParam(value = "onlySelectedAttributes", required = false) //
            Boolean onlySelectedAttributes, //
            @ApiParam(value = "Consider internal attributes", required = false) //
            @RequestParam(value = "considerInternalAttributes", required = false, defaultValue = "false") //
            Boolean considerInternalAttributes //
    ) {
        checkHeader(request);
        Tenant tenant = manufactureSecurityContextForInternalAccess(tenantId);
        Category categoryEnum = (StringStandardizationUtils.objectIsNullOrEmptyString(category) ? null
                : Category.fromName(category));
        return attributeService.getAttributesCount(tenant, attributeDisplayNameFilter, categoryEnum, subcategory,
                onlySelectedAttributes, considerInternalAttributes);
    }

    @RequestMapping(value = "/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save lead enrichment selection")
    public void saveLeadEnrichmentAttributes(HttpServletRequest request, //
            @PathVariable("tenantId") String tenantId, //
            @ApiParam(value = "Update lead enrichment selection", required = true) //
            @RequestBody LeadEnrichmentAttributesOperationMap attributes) {
        checkHeader(request);
        Tenant tenant = manufactureSecurityContextForInternalAccess(tenantId);
        Map<String, Integer> limitationMap = attributeService.getPremiumAttributesLimitation(tenant);
        attributeService.save(attributes, tenant, limitationMap, Boolean.FALSE);
    }

    @RequestMapping(value = "/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/premiumattributeslimitation" + "/"
            + TENANT_ID_PATH, //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Map<String, Integer> getLeadEnrichmentPremiumAttributesLimitation(HttpServletRequest request, //
            @PathVariable("tenantId") String tenantId) {
        checkHeader(request);
        Tenant tenant = manufactureSecurityContextForInternalAccess(tenantId);
        return attributeService.getPremiumAttributesLimitation(tenant);
    }

    @RequestMapping(value = "/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/selectedattributes/count" + "/"
            + TENANT_ID_PATH, //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected attributes count")
    public Integer getLeadEnrichmentSelectedAttributeCount(HttpServletRequest request, //
            @PathVariable("tenantId") String tenantId, //
            @ApiParam(value = "Consider internal attributes", required = false) //
            @RequestParam(value = "considerInternalAttributes", required = false, defaultValue = "false") //
            Boolean considerInternalAttributes) {
        checkHeader(request);
        Tenant tenant = manufactureSecurityContextForInternalAccess(tenantId);
        return attributeService.getSelectedAttributeCount(tenant, considerInternalAttributes);
    }

    @RequestMapping(value = "/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/selectedpremiumattributes/count"
            + "/" + TENANT_ID_PATH, //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected premium attributes count")
    public Integer getLeadEnrichmentSelectedAttributePremiumCount(HttpServletRequest request, //
            @PathVariable("tenantId") String tenantId, //
            @ApiParam(value = "Consider internal attributes", required = false) //
            @RequestParam(value = "considerInternalAttributes", required = false, defaultValue = "false") //
            Boolean considerInternalAttributes) {
        checkHeader(request);
        Tenant tenant = manufactureSecurityContextForInternalAccess(tenantId);
        return attributeService.getSelectedAttributePremiumCount(tenant, considerInternalAttributes);
    }

    @RequestMapping(value = "/enrichment/all"
            + LatticeInsightsResource.INSIGHTS_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all lead enrichment")
    public List<LeadEnrichmentAttribute> getAllLeadEnrichmentAttributes(HttpServletRequest request) {
        return attributeService.getAllAttributes();
    }

    @RequestMapping(value = "/emails/createmodel/result/{result}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after model creation")
    public void sendPlsCreateModelEmail(@PathVariable("result") String result,
            @PathVariable("tenantId") String tenantId, @RequestBody AdditionalEmailInfo emailInfo,
            HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        String modelName = emailInfo.getModelId();
        if (modelName != null && !modelName.isEmpty()) {
            for (User user : users) {
                if (user.getEmail().equals(emailInfo.getUserId())) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    switch (result.toUpperCase()) {
                    case "COMPLETED":
                        if (user.getAccessLevel().equals(AccessLevel.INTERNAL_ADMIN.name())
                                || user.getAccessLevel().equals(AccessLevel.INTERNAL_USER.name())) {
                            emailService.sendPlsCreateModelCompletionEmail(user, appPublicUrl, tenantName, modelName,
                                    true);
                        } else {
                            emailService.sendPlsCreateModelCompletionEmail(user, appPublicUrl, tenantName, modelName,
                                    false);
                        }
                        break;
                    case "FAILED":
                        if (user.getAccessLevel().equals(AccessLevel.INTERNAL_ADMIN.name())
                                || user.getAccessLevel().equals(AccessLevel.INTERNAL_USER.name())) {
                            emailService.sendPlsCreateModelErrorEmail(user, appPublicUrl, tenantName, modelName, true);
                        } else {
                            emailService.sendPlsCreateModelErrorEmail(user, appPublicUrl, tenantName, modelName, false);
                        }
                        break;
                    default:
                        log.warn(String.format(
                                "Non-completed nor failed model created. Model status: %s, "
                                        + "Tenant ID: %s, Details: %s",
                                result, tenantId, JsonUtils.serialize(emailInfo)));
                        break;
                    }
                }
            }
        }
    }

    @RequestMapping(value = "/emails/score/result/{result}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after scoring")
    public void sendPlsScoreEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
            @RequestBody AdditionalEmailInfo emailInfo, HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        String modelId = emailInfo.getModelId();
        if (modelId != null && !modelId.isEmpty()) {
            for (User user : users) {
                if (user.getEmail().equals(emailInfo.getUserId())) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    ModelSummary modelSummary = modelSummaryProxy.getByModelId(modelId);
                    if (modelSummary != null) {
                        String modelName = modelSummary.getDisplayName();
                        if (result.equals("COMPLETED")) {
                            if (user.getAccessLevel().equals(AccessLevel.INTERNAL_ADMIN.name())
                                    || user.getAccessLevel().equals(AccessLevel.INTERNAL_USER.name())) {
                                emailService.sendPlsScoreCompletionEmail(user, appPublicUrl, tenantName, modelName,
                                        true);
                            } else {
                                emailService.sendPlsScoreCompletionEmail(user, appPublicUrl, tenantName, modelName,
                                        false);
                            }
                        } else if (result.equals("FAILED")) {
                            if (user.getAccessLevel().equals(AccessLevel.INTERNAL_ADMIN.name())
                                    || user.getAccessLevel().equals(AccessLevel.INTERNAL_USER.name())) {
                                emailService.sendPlsScoreErrorEmail(user, appPublicUrl, tenantName, modelName, true);
                            } else {
                                emailService.sendPlsScoreErrorEmail(user, appPublicUrl, tenantName, modelName, false);
                            }
                        }
                    }
                }
            }
        }
    }

    @RequestMapping(value = "/emails/enrichment/internal/result/{result}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after enrichment of internal attributes")
    public void sendPlsInternalEnrichmentEmail(@PathVariable("result") String result,
            @PathVariable("tenantId") String tenantId, @RequestBody AdditionalEmailInfo emailInfo,
            HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        String modelId = emailInfo.getModelId();
        if (modelId != null && !modelId.isEmpty()) {
            for (User user : users) {
                if (user.getEmail().equals(emailInfo.getUserId())) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    ModelSummary modelSummary = modelSummaryProxy.getByModelId(modelId);
                    if (modelSummary != null) {
                        String modelName = modelSummary.getDisplayName();
                        if (result.equals("COMPLETED")) {
                            emailService.sendPlsEnrichInternalAttributeCompletionEmail(user, appPublicUrl, tenantName,
                                    modelName, true, emailInfo.getExtraInfoList());
                        } else if (result.equals("FAILED")) {
                            emailService.sendPlsEnrichInternalAttributeErrorEmail(user, appPublicUrl, tenantName,
                                    modelName, false, emailInfo.getExtraInfoList());
                        }
                    }
                }
            }
        }
    }

    @RequestMapping(value = "/emails/processanalyze/result/{result}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after processanalyze")
    public void sendCDLProcessAnalyzeEmail(@PathVariable("result") String result,
            @PathVariable("tenantId") String tenantId, @RequestBody AdditionalEmailInfo emailInfo) {
        List<User> users = userService.getUsers(tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        boolean onlyCurrentUser = tenant.getNotificationType().equals(TenantEmailNotificationType.SINGLE_USER);
        for (User user : users) {
            if (result.equals("COMPLETED") && tenant.getNotificationLevel().compareTo(TenantEmailNotificationLevel.INFO) >= 0) {
                if ((!onlyCurrentUser && AccessLevel.EXTERNAL_ADMIN.name().equals(user.getAccessLevel())) || user.getEmail().equals(emailInfo.getUserId())) {
                    emailService.sendCDLProcessAnalyzeCompletionEmail(user, tenant, appPublicUrl);
                }
            } else if (result.equals("FAILED") && tenant.getNotificationLevel().compareTo(TenantEmailNotificationLevel.ERROR) >= 0) {
                if ((!onlyCurrentUser && AccessLevel.EXTERNAL_ADMIN.name().equals(user.getAccessLevel())) || user.getEmail().equals(emailInfo.getUserId())) {
                    emailService.sendCDLProcessAnalyzeErrorEmail(user, tenant, appPublicUrl);
                }
            }
        }
    }

    @PutMapping(value = "/emails/orphanexport/result/{result}/" + TENANT_ID_PATH, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after orphan records export")
    public void sendOrphanRecordsExportEmail(@PathVariable String result, @PathVariable String tenantId,
            @RequestBody OrphanRecordsExportRequest exportRequest, HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        String exportID = exportRequest.getExportId();
        String exportType = exportRequest.getOrphanRecordsType().getDisplayName();

        if (StringUtils.isNotBlank(exportID)) {
            for (User user : users) {
                if (user.getEmail().equals(exportRequest.getCreatedBy())) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    String url = String.format("%s/atlas/tenant/%s/orphanexport/%s", appPublicUrl, tenantName,
                            exportID);
                    log.info(String.format("URL=%s, result=%s", url, result));
                    switch (result) {
                    case "READY":
                        emailService.sendPlsExportOrphanRecordsSuccessEmail(user, tenantName, appPublicUrl, url,
                                exportID, exportType);
                        break;
                    case "GENERATING":
                        emailService.sendPlsExportOrphanRecordsRunningEmail(user, exportID, exportType);
                        break;
                    }
                }
            }
        }
    }

    @RequestMapping(value = "/emails/segmentexport/result/{result}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after segment export")
    public void sendSegmentExportEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
                                       @RequestBody MetadataSegmentExport export) {
        List<User> users = userService.getUsers(tenantId);
        String exportID = export.getExportId();
        AtlasExportType exportType = export.getType();
        sendEmail(exportType, exportID, users, result, tenantId, export.getCreatedBy());
    }

    private void sendEmail(AtlasExportType exportType, String exportID, List<User> users, String result,
                           String tenantId, String createBy) {
        String exportTypeStr = exportType.getDisplayName();
        if (exportID != null && !exportID.isEmpty()) {
            for (User user : users) {
                if (user.getEmail().equals(createBy)) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    String url = appPublicUrl + "/atlas/tenant/" + tenantName + "/export/" + exportID;
                    switch (result) {
                        case "COMPLETED":
                            emailService.sendPlsExportSegmentSuccessEmail(user, url, exportID, exportTypeStr);
                            break;
                        case "FAILED":
                            emailService.sendPlsExportSegmentErrorEmail(user, exportID, exportTypeStr);
                            break;
                        case "STARTED":
                            emailService.sendPlsExportSegmentRunningEmail(user, exportID);
                            break;
                    }
                }
            }
        }
    }

    @RequestMapping(value = "/emails/atlasexport/result/{result}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after segment export")
    public void sendAtlasExportEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
                                     @RequestBody AtlasExport export) {
        List<User> users = userService.getUsers(tenantId);
        String exportID = export.getUuid();
        AtlasExportType exportType = export.getExportType();
        sendEmail(exportType, exportID, users, result, tenantId, export.getCreatedBy());
    }

    @RequestMapping(value = "/emails/s3import/result/{result}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after s3 import")
    public void sendS3ImportEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
            @RequestBody S3ImportEmailInfo emailInfo, HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        log.info("tenant {} notification_level is: {}, notification_type is: {}.", tenant.getId(),
                tenant.getNotificationLevel().name(), tenant.getNotificationType());
        boolean onlyCurrentUser = tenant.getNotificationType().equals(TenantEmailNotificationType.SINGLE_USER);
        for (User user : users) {
            if ((onlyCurrentUser && user.getEmail().equalsIgnoreCase(emailInfo.getUser()))
                    || (!onlyCurrentUser && user.getAccessLevel().equals(AccessLevel.EXTERNAL_ADMIN.name()))) {
                switch (result.toUpperCase()) {
                    case "FAILED":
                        if (tenant.getNotificationLevel().compareTo(TenantEmailNotificationLevel.ERROR) >= 0) {
                            emailService.sendIngestionStatusEmail(user, tenant, appPublicUrl, result, emailInfo);
                        }
                        break;
                    case "SUCCESS":
                        if (tenant.getNotificationLevel().compareTo(TenantEmailNotificationLevel.INFO) >= 0) {
                            emailService.sendIngestionStatusEmail(user, tenant, appPublicUrl, result, emailInfo);
                        }
                        break;
                    case "IN_PROGRESS":
                        if ((tenant.getNotificationLevel().compareTo(TenantEmailNotificationLevel.INFO) >= 0 && StringUtils.isEmpty(emailInfo.getErrorMsg())) ||
                                (tenant.getNotificationLevel().compareTo(TenantEmailNotificationLevel.WARNING) >= 0 && StringUtils.isNotEmpty(emailInfo.getErrorMsg()))) {
                            emailService.sendIngestionStatusEmail(user, tenant, appPublicUrl, result, emailInfo);
                        }
                        break;
                    default: break;
                }
            }
        }
    }

    @RequestMapping(value = "/emails/s3template/create/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after s3 template created")
    public void sendS3TemplateCreateEmail(@PathVariable("tenantId") String tenantId,
            @RequestBody S3ImportEmailInfo emailInfo, HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        if (tenant.getNotificationLevel().compareTo(TenantEmailNotificationLevel.INFO) >= 0) {
            boolean onlyCurrentUser = tenant.getNotificationType().equals(TenantEmailNotificationType.SINGLE_USER);
            for (User user : users) {
                if ((onlyCurrentUser && user.getEmail().equalsIgnoreCase(emailInfo.getUser()))
                        || (!onlyCurrentUser && user.getAccessLevel().equals(AccessLevel.EXTERNAL_ADMIN.name()))) {
                    emailService.sendS3TemplateCreateEmail(user, tenant, appPublicUrl, emailInfo);
                }
            }
        }
    }

    @RequestMapping(value = "/emails/s3template/update/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after s3 template update")
    public void sendS3TemplateUpdateEmail(@PathVariable("tenantId") String tenantId,
            @RequestBody S3ImportEmailInfo emailInfo, HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);

        if (tenant.getNotificationLevel().compareTo(TenantEmailNotificationLevel.INFO) >= 0) {
            boolean onlyCurrentUser = tenant.getNotificationType().equals(TenantEmailNotificationType.SINGLE_USER);
            for (User user : users) {
                if ((onlyCurrentUser && user.getEmail().equalsIgnoreCase(emailInfo.getUser())) ||
                        (!onlyCurrentUser && user.getAccessLevel().equals(AccessLevel.EXTERNAL_ADMIN.name()))) {
                    emailService.sendS3TemplateUpdateEmail(user, tenant, appPublicUrl, emailInfo);
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
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
            if (StringStandardizationUtils.objectIsNullOrEmptyString(productPrefix)) {
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
                            payload = String.format("{ \"Status\": \"%s\" }",
                                    ModelSummaryStatus.INACTIVE.getStatusCode());
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

    @RequestMapping(value = "/modelnotes/{modelSummaryId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one note for certain model summary.")
    public boolean createNote(@PathVariable String modelSummaryId, @RequestBody NoteParams noteParams,
            HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("ModelSummary %s's ModelNote created by %s", modelSummaryId, noteParams.getUserName()));
        modelNoteService.create(modelSummaryId, noteParams);
        return true;
    }

    @RequestMapping(value = "/modelnotes/{fromModelSummaryId}/{toModelSummaryId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one note for certain model summary.")
    public boolean copyNotes(@PathVariable String fromModelSummaryId, @PathVariable String toModelSummaryId,
            HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Copy notes from ModelSummary %s to ModelSummary %s ModelNote", fromModelSummaryId,
                toModelSummaryId));
        modelNoteService.copyNotes(fromModelSummaryId, toModelSummaryId);
        return true;
    }

    @RequestMapping(value = "/segment/{segmentName}/restriction/" + TENANT_ID_PATH, method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get segment restriction.")
    public Restriction getSegmentRestriction(@PathVariable("tenantId") String tenantId, //
            @PathVariable("segmentName") String segmentName, HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Getting restriction from %s segment", segmentName));
        manufactureSecurityContextForInternalAccess(tenantId);
        MetadataSegment segment = metadataSegmentService.getSegmentByName(segmentName, false);
        if (segment != null) {
            return segment.getAccountRestriction();
        } else {
            return null;
        }
    }

    @RequestMapping(value = "/segment/export/{exportId}/" + TENANT_ID_PATH, method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Segment export job info.")
    public MetadataSegmentExport getMetadataSegmentExport(@PathVariable("tenantId") String tenantId, //
            @PathVariable("exportId") String exportId, HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Getting MetadataSegmentExport from %s exportId", exportId));
        manufactureSecurityContextForInternalAccess(tenantId);
        return metadataSegmentExportService.getSegmentExportByExportId(exportId);
    }

    @RequestMapping(value = "/segment/export/{exportId}/" + TENANT_ID_PATH, method = RequestMethod.PUT)
    @ResponseBody
    @ApiOperation(value = "Update segment export job info.")
    public MetadataSegmentExport updateMetadataSegmentExport(@PathVariable("tenantId") String tenantId, //
            @PathVariable("exportId") String exportId, //
            @RequestParam("state") Status state, HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Updating MetadataSegmentExport from %s exportId", exportId));
        manufactureSecurityContextForInternalAccess(tenantId);
        MetadataSegmentExport metadataSegmentExport = metadataSegmentExportService.getSegmentExportByExportId(exportId);
        metadataSegmentExport.setStatus(state);
        return metadataSegmentExportService.updateSegmentExportJob(metadataSegmentExport);
    }

    @PostMapping(value = "/segment/orphan/customerspace/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "create orphan record through MetadataSegmentExportEntityMgr")
    public MetadataSegmentExport createOrphanRecordThruMgr(@PathVariable("tenantId") String tenantId,
            HttpServletRequest request, @RequestBody MetadataSegmentExport metadataSegmentExport) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        return metadataSegmentExportService.createOrphanRecordThruMgr(metadataSegmentExport);
    }

    public List<String> getTestTenantIds() {
        String tenant1Id = contractId + "PLSTenant1." + contractId + "PLSTenant1.Production";
        String tenant2Id = contractId + "PLSTenant2." + contractId + "PLSTenant2.Production";
        return Arrays.asList(tenant1Id, tenant2Id);
    }

    @SuppressWarnings("deprecation")
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

    @SuppressWarnings("deprecation")
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
                    log.error(e.getMessage(), e);
                }
            }
        }

        if (!BootstrapState.State.OK.equals(state.state)) {
            throw new IllegalArgumentException("The tenant state is not OK after " + timeout + " msec.");
        }
    }

    @SuppressWarnings("deprecation")
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

    private Tenant manufactureSecurityContextForInternalAccess(String tenantId) {
        log.info("Manufacturing security context for " + tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_18074, new String[] { tenantId });
        }
        manufactureSecurityContextForInternalAccess(tenant);
        return tenant;
    }

    private void manufactureSecurityContextForInternalAccess(Tenant tenant) {
        TicketAuthenticationToken auth = new TicketAuthenticationToken(null, "x.y");
        Session session = new Session();
        session.setTenant(tenant);
        auth.setSession(session);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    private boolean containsAtleastOneAttributeForCategory(List<LeadEnrichmentAttribute> allAttributes,
            Category category) {
        if (!CollectionUtils.isEmpty(allAttributes)) {
            for (LeadEnrichmentAttribute attr : allAttributes) {
                if (category.toString().equals(attr.getCategory())) {
                    return true;
                }
            }
        }
        return false;
    }

    @RequestMapping(value = "/jobs/all/" + TENANT_ID_PATH, method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get actions for a tenant")
    public List<Job> getJobsBasedOnActionIdsAndType( //
            @PathVariable("tenantId") String customerSpace, //
            @RequestParam(value = "pid") List<Long> pids, //
            @RequestParam(value = "type") ActionType actionType, //
            HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Retrieve Jobs for tenant: %s based on type %s. Pid list = %s", //
                customerSpace, actionType, //
                (pids == null ? "{}" : JsonUtils.serialize(pids))));
        manufactureSecurityContextForInternalAccess(CustomerSpace.parse(customerSpace).toString());
        return workflowJobService.findJobsBasedOnActionIdsAndType(pids, actionType);
    }

    @RequestMapping(value = "/external-scoring-config-context/{configUuid}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get attributes within a predefined group for a tenant")
    public ScoringRequestConfigContext getScoringRequestConfigContext(HttpServletRequest request,
            @PathVariable(name = "configUuid") String configUuid) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Retrieve ScoringRequestConfiguration metadata for ConfigId: %s", configUuid));
        }
        ScoringRequestConfigContext srcContext = scoringRequestConfigService
                .retrieveScoringRequestConfigContext(configUuid);
        if (srcContext == null) {
            throw new LedpException(LedpCode.LEDP_18194, new String[] { configUuid });
        }
        return srcContext;
    }
}
