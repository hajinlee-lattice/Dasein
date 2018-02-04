package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
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
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
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
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.BucketedScore;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.pls.ModelActivationResult;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.CrmCredentialService;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ModelNoteService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.service.TargetMarketService;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.pls.service.impl.TenantConfigServiceImpl;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.db.exposed.service.ReportService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "internal", description = "REST resource for internal operations")
@RestController
@RequestMapping(value = "/internal")
public class InternalResource extends InternalResourceBase {

    protected static final String EXTERNAL_USER_USERNAME_1 = "pls-external-user-tester-1@test.lattice-engines.ext";
    private static final Logger log = LoggerFactory.getLogger(InternalResource.class);
    private static final String passwordTester = "pls-password-tester@test.lattice-engines.ext";
    private static final String passwordTesterPwd = "Lattice123";
    private static final String adminTester = "pls-super-admin-tester@test.lattice-engines.com";
    private static final String adminTesterPwd = "admin";
    private static final String adUsername = "testuser1";
    private static final String adPassword = "Lattice1";
    public static final String TENANT_ID_PATH = "{tenantId:\\w+\\.\\w+\\.\\w+}";
    private static final String NULL_STRING = "null";
    private static final Integer BUCKET_0 = 99;
    private static final Integer BUCKET_1 = 95;
    private static final Integer BUCKET_2 = 85;
    private static final Integer BUCKET_3 = 50;
    private static final Integer BUCKET_4 = 5;

    @Inject
    private GlobalAuthenticationService globalAuthenticationService;

    @Inject
    private GlobalUserManagementService globalUserManagementService;

    @Inject
    private ModelMetadataService modelMetadataService;

    @Inject
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Inject
    private ModelSummaryService modelSummaryService;

    @Inject
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

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
    private TargetMarketService targetMarketService;

    @Inject
    private TenantService tenantService;

    @Inject
    private ReportService reportService;

    @Inject
    private SourceFileService sourceFileService;

    @Inject
    private EmailService emailService;

    @Inject
    private BucketedScoreService bucketedScoreService;

    @Inject
    private ModelNoteService modelNoteService;

    @Inject
    private MetadataSegmentService metadataSegmentService;

    @Inject
    private MetadataSegmentExportService metadataSegmentExportService;

    @Inject
    private DataLakeService dataLakeService;

    @Inject
    private ActionService actionService;

    @Inject
    private WorkflowJobService workflowJobService;

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

    @RequestMapping(value = "/modelsummaries/updated/{timeframe}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a list of model summary updated within the timeframe as specified")
    public List<ModelSummary> getModelSummariesUpdatedWithinTimeFrame(@PathVariable long timeframe,
            HttpServletRequest request) {
        checkHeader(request);
        return modelSummaryService.getModelSummariesModifiedWithinTimeFrame(timeframe);
    }

    @RequestMapping(value = "/modelsummarydownloadflag/"
            + TENANT_ID_PATH, method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of model summary need for refresh in Scoringapi cachce")
    public void setModelSummaryDownloadFlag(@PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        log.info(String.format("Set model summary download flag for tenant %s", tenantId));
        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(tenantId);
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
        return postProcessModelSummaryList(modelSummaryEntityMgr.findAllActive());
    }

    private List<ModelSummary> postProcessModelSummaryList(List<ModelSummary> summaries) {
        return postProcessModelSummaryList(summaries, true);
    }

    private List<ModelSummary> postProcessModelSummaryList(List<ModelSummary> summaries, boolean dropPredictors) {
        for (ModelSummary summary : summaries) {
            if (dropPredictors) {
                summary.setPredictors(new ArrayList<>());
            }
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
        if (StringUtils.isNotEmpty(start)) {
            log.info("getModelSummariesCount - start = " + start + ", tenant = " + tenantId);
            lastUpdateTime = DateTimeUtils.convertToLongUTCISO8601(start);
        }
        return modelSummaryEntityMgr.findTotalCount(lastUpdateTime, considerAllStatus);

    }

    @RequestMapping(value = "/modelsummarydetails/paginate/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get paginated model summaries")
    public List<ModelSummary> getPaginatedModelSummaries(
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = false) @RequestParam(value = "start", required = false) String start,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus,
            @ApiParam(value = "Offset", required = false) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum entries in page", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) throws ParseException {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        long lastUpdateTime = 0;
        if (StringUtils.isNotEmpty(start)) {
            log.info("getPaginatedModelSummaries - start = " + start + ", tenant = " + tenantId);
            lastUpdateTime = DateTimeUtils.convertToLongUTCISO8601(start);
        }
        return postProcessModelSummaryList(
                modelSummaryEntityMgr.findPaginatedModels(lastUpdateTime, considerAllStatus, offset, maximum), false);

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

    @RequestMapping(value = "/modelsummaries/crosstenant/{applicationId}/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a model summary by applicationId")
    public List<ModelSummary> getModelSummariesCrossTenantByAppId(@PathVariable("applicationId") String applicationId,
            @PathVariable("tenantId") String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        return modelSummaryEntityMgr.getModelSummariesByApplicationId(applicationId);
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

    @RequestMapping(value = "/metadata/required/modelId/{modelId}/"
            + TENANT_ID_PATH, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get required column names for the event table used for the specified model")
    public List<Attribute> getRequiredColumns(@PathVariable String modelId, @PathVariable("tenantId") String tenantId,
            HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        return modelMetadataService.getRequiredColumns(modelId);
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

        ModelActivationResult result = new ModelActivationResult();
        result.setExists(true);
        ResponseDocument<ModelActivationResult> response = new ResponseDocument<>();
        response.setResult(result);
        if (!NameValidationUtils.validateModelName(modelId)) {
            log.error(String.format("Not qualified modelId %s contains unsupported characters.", modelId));
            response.setSuccess(false);
        } else {
            modelSummaryService.updateModelSummary(modelId, attrMap);
            response.setSuccess(true);
        }
        return response;
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

    @RequestMapping(value = "/bucketmetadata/{modelId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "create default abcd scored buckets")
    public List<BucketMetadata> createDefaultABCDBuckets(@PathVariable String modelId, @RequestBody String userId,
            HttpServletRequest request) throws Exception {
        BucketMetadata bucketMetadata1 = new BucketMetadata();
        BucketMetadata bucketMetadata2 = new BucketMetadata();
        BucketMetadata bucketMetadata3 = new BucketMetadata();
        BucketMetadata bucketMetadata4 = new BucketMetadata();

        checkHeader(request);
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        manufactureSecurityContextForInternalAccess(modelSummary.getTenant());
        BucketedScoreSummary bucketedScoreSummary = bucketedScoreService.getBucketedScoreSummaryForModelId(modelId);
        log.info(String.format("Creating abcd buckets for model: %s bucketed score summary: %s", modelId,
                JsonUtils.serialize(bucketedScoreSummary)));
        BucketedScore[] bucketedScores = bucketedScoreSummary.getBucketedScores();
        Double overallLift = bucketedScoreSummary.getOverallLift();

        bucketMetadata1.setBucket(BucketName.A);
        bucketMetadata1.setLeftBoundScore(BUCKET_0);
        bucketMetadata1.setRightBoundScore(BUCKET_1);
        bucketMetadata1.setLastModifiedByUser(userId);
        bucketMetadata1.setNumLeads(
                bucketedScores[BUCKET_1 - 1].getLeftNumLeads() - bucketedScores[BUCKET_0].getLeftNumLeads());
        if (bucketMetadata1.getNumLeads() == 0 || overallLift == 0) {
            bucketMetadata1.setLift(0);
        } else {
            bucketMetadata1.setLift(((bucketedScores[BUCKET_1 - 1].getLeftNumConverted()
                    - bucketedScores[BUCKET_0].getLeftNumConverted()) / (double) bucketMetadata1.getNumLeads())
                    / overallLift);
        }

        bucketMetadata2.setBucket(BucketName.B);
        bucketMetadata2.setLeftBoundScore(BUCKET_1 - 1);
        bucketMetadata2.setRightBoundScore(BUCKET_2);
        bucketMetadata2.setLastModifiedByUser(userId);
        bucketMetadata2.setNumLeads(
                bucketedScores[BUCKET_2 - 1].getLeftNumLeads() - bucketedScores[BUCKET_1 - 1].getLeftNumLeads());
        if (bucketMetadata2.getNumLeads() == 0 || overallLift == 0) {
            bucketMetadata2.setLift(0);
        } else {
            bucketMetadata2.setLift(((bucketedScores[BUCKET_2 - 1].getLeftNumConverted()
                    - bucketedScores[BUCKET_1 - 1].getLeftNumConverted()) / (double) bucketMetadata2.getNumLeads())
                    / overallLift);
        }

        bucketMetadata3.setBucket(BucketName.C);
        bucketMetadata3.setLeftBoundScore(BUCKET_2 - 1);
        bucketMetadata3.setRightBoundScore(BUCKET_3);
        bucketMetadata3.setLastModifiedByUser(userId);
        bucketMetadata3.setNumLeads(
                bucketedScores[BUCKET_3 - 1].getLeftNumLeads() - bucketedScores[BUCKET_2 - 1].getLeftNumLeads());
        if (bucketMetadata3.getNumLeads() == 0 || overallLift == 0) {
            bucketMetadata3.setLift(0);
        } else {
            bucketMetadata3.setLift(((bucketedScores[BUCKET_3 - 1].getLeftNumConverted()
                    - bucketedScores[BUCKET_2 - 1].getLeftNumConverted()) / (double) bucketMetadata3.getNumLeads())
                    / overallLift);
        }

        bucketMetadata4.setBucket(BucketName.D);
        bucketMetadata4.setLeftBoundScore(BUCKET_3 - 1);
        bucketMetadata4.setRightBoundScore(BUCKET_4);
        bucketMetadata4.setLastModifiedByUser(userId);
        bucketMetadata4.setNumLeads(
                bucketedScores[BUCKET_4 - 1].getLeftNumLeads() - bucketedScores[BUCKET_3 - 1].getLeftNumLeads());
        if (bucketMetadata4.getNumLeads() == 0 || overallLift == 0) {
            bucketMetadata4.setLift(0);
        } else {
            bucketMetadata4.setLift(((bucketedScores[BUCKET_4 - 1].getLeftNumConverted()
                    - bucketedScores[BUCKET_3 - 1].getLeftNumConverted()) / (double) bucketMetadata4.getNumLeads())
                    / overallLift);
        }

        bucketedScoreService.createBucketMetadatas(modelId,
                Arrays.asList(bucketMetadata1, bucketMetadata2, bucketMetadata3, bucketMetadata4));
        return Arrays.asList(bucketMetadata1, bucketMetadata2, bucketMetadata3, bucketMetadata4);
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
                    ModelSummary modelSummary = modelSummaryEntityMgr.getByModelId(modelId);
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
                    ModelSummary modelSummary = modelSummaryEntityMgr.getByModelId(modelId);
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

    @RequestMapping(value = "/emails/segmentexport/result/{result}/"
            + TENANT_ID_PATH, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Send out email after segment export")
    public void sendSegmentExportEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
            @RequestBody MetadataSegmentExport export, HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        String exportID = export.getExportId();
        String exportType = export.getType().toString();
        switch (exportType) {
        case "ACCOUNT":
            exportType = "Account";
            break;
        case "CONTACT":
            exportType = "Contact";
            break;
        case "ACCOUNT_AND_CONTACT":
            exportType = "Account and Contact";
            break;
        }
        if (exportID != null && !exportID.isEmpty()) {
            for (User user : users) {
                if (user.getEmail().equals(export.getCreatedBy())) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    if (export != null) {
                        String url = appPublicUrl + "/lp/tenant/" + tenantName + "/export/" + exportID;
                        if (result.equals("COMPLETED")) {
                            emailService.sendPlsExportSegmentSuccessEmail(user, url, exportID, exportType,
                                    export.getCleanupBy());
                        } else if (result.equals("FAILED")) {
                            emailService.sendPlsExportSegmentErrorEmail(user, url, exportID, exportType);
                        }
                    }
                }
            }
        }
    }

    @RequestMapping(value = "/abcdbuckets/uptodate/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get up-to-date ABCD Buckets info for the model")
    public List<BucketMetadata> getUpToDateABCDBuckets(@PathVariable String modelId,
            @RequestParam(value = "tenantId", required = false) String tenantId, HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        return bucketedScoreService.getUpToDateModelBucketMetadata(modelId);
    }

    @RequestMapping(value = "/abcdbuckets/{modelId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Create a group of ABCD buckets")
    public void createABCDBuckets(@PathVariable String modelId,
            @RequestParam(value = "tenantId", required = false) String tenantId, HttpServletRequest request,
            @RequestBody List<BucketMetadata> bucketMetadatas) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        bucketedScoreService.createBucketMetadatas(modelId, bucketMetadatas);
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

    @RequestMapping(value = "/datacollection/attributes/{predefined}/" + TENANT_ID_PATH, method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get attributes within a predefined group for a tenant")
    public List<ColumnMetadata> getAttributesInPredefinedGroup(
            @PathVariable("predefined") ColumnSelection.Predefined predefined,
            @PathVariable("tenantId") String customerSpace) {
        log.debug(String.format("Retrieve Account attributes for attribute group: %s", predefined.getName()));
        manufactureSecurityContextForInternalAccess(CustomerSpace.parse(customerSpace).toString());
        return dataLakeService.getAttributesInPredefinedGroup(predefined);
    }

    @RequestMapping(value = "/actions/all/" + TENANT_ID_PATH, method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get actions for a tenant")
    public List<Action> getAllActions( //
            @PathVariable("tenantId") String customerSpace, //
            @RequestParam(value = "pid", required = false) List<Long> pids, //
            HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Retrieve Actions for tenant: %s", customerSpace));
        manufactureSecurityContextForInternalAccess(CustomerSpace.parse(customerSpace).toString());
        if (pids == null) {
            return actionService.findAll();
        } else {
            return actionService.findByPidIn(pids);
        }
    }

    @RequestMapping(value = "/actions/ownerid/{ownerId}/" + TENANT_ID_PATH, method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get actions for a tenant given tenant Id")
    public List<Action> getActionsByOwnerId( //
            @PathVariable("tenantId") String customerSpace, //
            @PathVariable("ownerId") String ownerId, //
            HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Retrieve Actions for tenant: %s with ownerId: %s", customerSpace, ownerId));
        Long ownerIdLong = null;
        if (!ownerId.equals(NULL_STRING)) {
            ownerIdLong = Long.parseLong(ownerId);
        }
        manufactureSecurityContextForInternalAccess(CustomerSpace.parse(customerSpace).toString());
        return actionService.findByOwnerId(ownerIdLong, null);
    }

    @RequestMapping(value = "/actions/" + TENANT_ID_PATH, method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "create an action")
    public Action createAction( //
            @PathVariable("tenantId") String customerSpace, //
            @RequestBody Action action, //
            HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Create action for tenant: %s", customerSpace));
        Tenant tenant = manufactureSecurityContextForInternalAccess(CustomerSpace.parse(customerSpace).toString());
        action.setTenant(tenant);
        return actionService.create(action);
    }

    @RequestMapping(value = "/actions/" + TENANT_ID_PATH, method = RequestMethod.PUT)
    @ResponseBody
    @ApiOperation(value = "Update an action")
    public Action updateAction( //
            @PathVariable("tenantId") String customerSpace, //
            @RequestBody Action action, //
            HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Update action for tenant: %s", customerSpace));
        Tenant tenant = manufactureSecurityContextForInternalAccess(CustomerSpace.parse(customerSpace).toString());
        action.setTenant(tenant);
        return actionService.update(action);
    }

    @RequestMapping(value = "/actions/" + TENANT_ID_PATH, method = RequestMethod.PATCH)
    @ResponseBody
    @ApiOperation(value = "Update an action")
    public void patchOwnerIdOfActions( //
            @PathVariable("tenantId") String customerSpace, //
            @RequestParam(value = "pid", required = true) List<Long> pids, //
            @RequestParam(value = "ownerId", required = true) Long ownerId, //
            HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Patch action for tenant: %s", customerSpace));
        Tenant tenant = manufactureSecurityContextForInternalAccess(CustomerSpace.parse(customerSpace).toString());
        actionService.updateOwnerIdIn(ownerId, pids);
    }

    @RequestMapping(value = "/actions/{pid}/" + TENANT_ID_PATH, method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete an action")
    public void deleteAction( //
            @PathVariable("pid") Long pid, //
            @PathVariable("tenantId") String customerSpace, //
            HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Delete action for tenant: %s, with pid=%d", customerSpace, pid));
        manufactureSecurityContextForInternalAccess(CustomerSpace.parse(customerSpace).toString());
        Action action = actionService.findByPid(pid);
        if (action == null) {
            throw new NullPointerException(String.format("Action with Pid = %d cannot be found", pid));
        }
        actionService.delete(action);
    }

    @RequestMapping(value = "/jobs/all/" + TENANT_ID_PATH, method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get actions for a tenant")
    public List<Job> getJobsBasedOnActionIdsAndType( //
            @PathVariable("tenantId") String customerSpace, //
            @RequestParam(value = "pid", required = true) List<Long> pids, //
            @RequestParam(value = "type", required = true) ActionType actionType, //
            HttpServletRequest request) {
        checkHeader(request);
        log.debug(String.format("Retrieve Jobs for tenant: %s based on type %s", customerSpace, actionType));
        manufactureSecurityContextForInternalAccess(CustomerSpace.parse(customerSpace).toString());
        return workflowJobService.findJobsBasedOnActionIdsAndType(pids, actionType);
    }

    @RequestMapping(value = "/plscomponent/invoketime/" + TENANT_ID_PATH, method = RequestMethod.GET,
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get actions for a tenant")
    public int getInvokeTime(@PathVariable("tenantId") String tenantId,  HttpServletRequest request) {
        checkHeader(request);
        manufactureSecurityContextForInternalAccess(tenantId);
        log.debug(String.format("Retrieve pls component invoke time for tenant: %s", tenantId));
        CustomerSpace customer_space = CustomerSpace.parse(tenantId);

        Camille camille = CamilleEnvironment.getCamille();
        Path contractPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), customer_space.getContractId(),
                customer_space.getTenantId(), customer_space.getSpaceId()).append(
                new Path(TenantConfigServiceImpl.SERVICES_ZNODE + TenantConfigServiceImpl.PLS_ZNODE
                        + TenantConfigServiceImpl.INVOKE_TIME));
        try {
            return Integer.parseInt(camille.get(contractPath).getData());
        } catch (Exception e) {
            log.error("Get PLS component invoke time failed. ", e);
            return 0;
        }
    }

}
