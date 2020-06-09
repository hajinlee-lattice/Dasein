package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.controller.LatticeInsightsResource;
import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationLevel;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationType;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.pls.service.ScoringRequestConfigService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.pls.service.dcp.UploadService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "internal", description = "REST resource for internal operations")
@RestController
@RequestMapping("/internal")
public class InternalResource extends InternalResourceBase {

    private static final Logger log = LoggerFactory.getLogger(InternalResource.class);

    public static final String TENANT_ID_PATH = "{tenantId:\\w+\\.\\w+\\.\\w+}";

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private AttributeService attributeService;

    @Inject
    private UserService userService;

    @Inject
    private TenantService tenantService;

    @Inject
    private ReportService reportService;

    @Inject
    private EmailService emailService;

    @Inject
    private MetadataSegmentService metadataSegmentService;

    @Inject
    private MetadataSegmentExportService metadataSegmentExportService;

    @Inject
    private WorkflowJobService workflowJobService;

    @Inject
    private ScoringRequestConfigService scoringRequestConfigService;

    @Inject
    private UploadService uploadService;

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

    @PostMapping("/reports/" + TENANT_ID_PATH)
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

    @GetMapping("/reports/{reportName}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Retrieve a Report")
    public Report findReportByName(@PathVariable("reportName") String reportName,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);

        return reportService.getReportByName(reportName);
    }

    @GetMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/categories" + "/"  + TENANT_ID_PATH)
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

    @GetMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/subcategories" + "/" + TENANT_ID_PATH)
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

    @GetMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/" + TENANT_ID_PATH)
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

    @GetMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/" + "count" + "/" + TENANT_ID_PATH)
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

    @PutMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/" + TENANT_ID_PATH)
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

    @GetMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/premiumattributeslimitation/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Map<String, Integer> getLeadEnrichmentPremiumAttributesLimitation(HttpServletRequest request, //
            @PathVariable("tenantId") String tenantId) {
        checkHeader(request);
        Tenant tenant = manufactureSecurityContextForInternalAccess(tenantId);
        return attributeService.getPremiumAttributesLimitation(tenant);
    }

    @GetMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/selectedattributes/count/" + TENANT_ID_PATH)
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

    @GetMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/selectedpremiumattributes/count/" + TENANT_ID_PATH)
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

    @GetMapping("/enrichment/all" + LatticeInsightsResource.INSIGHTS_PATH)
    @ResponseBody
    @ApiOperation(value = "Get all lead enrichment")
    public List<LeadEnrichmentAttribute> getAllLeadEnrichmentAttributes(HttpServletRequest request) {
        return attributeService.getAllAttributes();
    }

    @PutMapping("/emails/createmodel/result/{result}/" + TENANT_ID_PATH)
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

    @PutMapping("/emails/score/result/{result}/" + TENANT_ID_PATH)
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
                        if ("COMPLETED".equals(result)) {
                            if (user.getAccessLevel().equals(AccessLevel.INTERNAL_ADMIN.name())
                                    || user.getAccessLevel().equals(AccessLevel.INTERNAL_USER.name())) {
                                emailService.sendPlsScoreCompletionEmail(user, appPublicUrl, tenantName, modelName,
                                        true);
                            } else {
                                emailService.sendPlsScoreCompletionEmail(user, appPublicUrl, tenantName, modelName,
                                        false);
                            }
                        } else if ("FAILED".equals(result)) {
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

    @PutMapping("/emails/enrichment/internal/result/{result}/" + TENANT_ID_PATH)
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
                        if ("COMPLETED".equals(result)) {
                            emailService.sendPlsEnrichInternalAttributeCompletionEmail(user, appPublicUrl, tenantName,
                                    modelName, emailInfo.getExtraInfoList());
                        } else if ("FAILED".equals(result)) {
                            emailService.sendPlsEnrichInternalAttributeErrorEmail(user, appPublicUrl, tenantName,
                                    modelName, emailInfo.getExtraInfoList());
                        }
                    }
                }
            }
        }
    }

    @PutMapping("/emails/processanalyze/result/{result}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after processanalyze")
    public void sendCDLProcessAnalyzeEmail(@PathVariable("result") String result,
            @PathVariable("tenantId") String tenantId, @RequestBody AdditionalEmailInfo emailInfo) {
        List<User> users = userService.getUsers(tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        boolean onlyCurrentUser = tenant.getNotificationType().equals(TenantEmailNotificationType.SINGLE_USER);
        for (User user : users) {
            if ("COMPLETED".equals(result) && tenant.getNotificationLevel().compareTo(TenantEmailNotificationLevel.INFO) >= 0) {
                if ((!onlyCurrentUser && AccessLevel.EXTERNAL_ADMIN.name().equals(user.getAccessLevel())) || user.getEmail().equals(emailInfo.getUserId())) {
                    emailService.sendCDLProcessAnalyzeCompletionEmail(user, tenant, appPublicUrl);
                }
            } else if ("FAILED".equals(result) && tenant.getNotificationLevel().compareTo(TenantEmailNotificationLevel.ERROR) >= 0) {
                if ((!onlyCurrentUser && AccessLevel.EXTERNAL_ADMIN.name().equals(user.getAccessLevel())) || user.getEmail().equals(emailInfo.getUserId())) {
                    emailService.sendCDLProcessAnalyzeErrorEmail(user, tenant, appPublicUrl);
                }
            }
        }
    }

    @PutMapping("/emails/orphanexport/result/{result}/" + TENANT_ID_PATH)
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
                        default:
                            log.warn("Unknown result {}", result);
                    }
                }
            }
        }
    }

    @PutMapping("/emails/segmentexport/result/{result}/" + TENANT_ID_PATH)
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
                            emailService.sendPlsExportSegmentSuccessEmail(user, url, exportID, exportTypeStr, tenantName);
                            break;
                        case "FAILED":
                            emailService.sendPlsExportSegmentErrorEmail(user, exportID, exportTypeStr);
                            break;
                        case "STARTED":
                            emailService.sendPlsExportSegmentRunningEmail(user, exportID);
                            break;
                        default:
                            log.warn("Unknown result {}", result);
                    }
                }
            }
        }
    }

    @PutMapping("/emails/atlasexport/result/{result}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after segment export")
    public void sendAtlasExportEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
                                     @RequestBody AtlasExport export) {
        List<User> users = userService.getUsers(tenantId);
        String exportID = export.getUuid();
        AtlasExportType exportType = export.getExportType();
        sendEmail(exportType, exportID, users, result, tenantId, export.getCreatedBy());
    }

    @PutMapping("/emails/s3import/result/{result}/" + TENANT_ID_PATH)
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

    @PutMapping("/emails/s3template/create/" + TENANT_ID_PATH)
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

    @PutMapping("/emails/s3template/update/" + TENANT_ID_PATH)
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

    @GetMapping("/segment/{segmentName}/restriction/" + TENANT_ID_PATH)
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

    @GetMapping("/segment/export/{exportId}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Get Segment export job info.")
    public MetadataSegmentExport getMetadataSegmentExport(@PathVariable("tenantId") String tenantId, //
            @PathVariable("exportId") String exportId, HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Getting MetadataSegmentExport from %s exportId", exportId));
        manufactureSecurityContextForInternalAccess(tenantId);
        return metadataSegmentExportService.getSegmentExportByExportId(exportId);
    }

    @PutMapping("/segment/export/{exportId}/" + TENANT_ID_PATH)
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

    @PostMapping("/segment/orphan/customerspace/" + TENANT_ID_PATH)
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

    @GetMapping("/jobs/all/" + TENANT_ID_PATH)
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

    @GetMapping("/external-scoring-config-context/{configUuid}")
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

    @PutMapping("/emails/upload")
    @ResponseBody
    @ApiOperation(value = "Send out email after upload state change")
    public void sendUploadEmail(@RequestBody UploadEmailInfo uploadEmailInfo) {
        uploadService.sendUploadEmail(uploadEmailInfo);
    }
}
