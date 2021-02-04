package com.latticeengines.pls.controller;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.app.exposed.download.TemplateFileHttpDownloader;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.DeleteRequest;
import com.latticeengines.domain.exposed.cdl.ImportFileInfo;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.Status;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.domain.exposed.exception.UIActionCode;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.exception.UIActionUtils;
import com.latticeengines.domain.exposed.exception.UIMessage;
import com.latticeengines.domain.exposed.exception.View;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.FieldCategory;
import com.latticeengines.domain.exposed.pls.frontend.LatticeFieldCategory;
import com.latticeengines.domain.exposed.pls.frontend.TemplateFieldPreview;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.SystemStatusService;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;
import com.latticeengines.proxy.exposed.cdl.CDLJobProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdl consolidate and profile")
@RestController
@RequestMapping("/cdl")
@PreAuthorize("hasRole('Edit_PLS_CDL_Data')")
public class CDLResource {

    private static final Logger log = LoggerFactory.getLogger(CDLResource.class);

    private static final String createS3TemplateMsg = "<p>%s template has been created.</p>";
    private static final String createS3TemplateAndImportMsg = "<p>%s template has been created.  Your data import is being validated and queued. Visit <a ui-sref='home.jobs.data'>Data P&A</a> to track the process.</p>";
    private static final String editS3TemplateMsg = "<p>%s template has been edited.</p>";
    private static final String editS3TemplateAndImportMsg = "<p>%s template has been edited.  Your data import is being validated and queued. Visit <a ui-sref='home.jobs.data'>Data P&A</a> to track the process.</p>";
    private static final String importUsingTemplateMsg = "<p>Your data import is being validated and queued. Visit <a ui-sref='home.jobs.data'>Data P&A</a> to track the process.</p>";
    private static final String resetTemplateMsg = "<p>Your import template has been reset.</p>";
    private static final String validateAndUpdatePriorityMsg = "<p>System priorities has been updated.</p>";
    private static final String createS3ImportSystemMsg = "<p>%s system has been created.</p>";
    private static final String updateS3ImportSystemPriorityMsg = "System priority has been updated.</p>";

    @Inject
    private CDLJobProxy cdlJobProxy;

    @Inject
    private CDLService cdlService;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @Inject
    private SystemStatusService systemStatusService;

    @PostMapping("/consolidateAndProfile")
    @ApiOperation(value = "Start Consolidate And Profile job")
    public ResponseDocument<String> startConsolidateAndProfileJob() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        ApplicationId result = cdlJobProxy.createConsolidateJob(customerSpace.toString());
        return ResponseDocument.successResponse(result.toString());
    }

    @PostMapping("/processanalyze")
    @ApiOperation(value = "Start Process And Analyze job")
    public ResponseDocument<String> processAnalyze(@RequestBody(required = false) ProcessAnalyzeRequest request) {
        StatusDocument statusDocument = systemStatusService.getSystemStatus();
        if (StatusDocument.UNDER_MAINTENANCE.equals(statusDocument.getStatus())) {
            return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18182));
        }
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        if (request == null) {
            request = new ProcessAnalyzeRequest();
        }
        request.setUserId(MultiTenantContext.getEmailAddress());
        try {
            ApplicationId result = cdlService.processAnalyze(customerSpace.toString(), request);
            if (result == null) {
                return ResponseDocument.successResponse(null);
            } else {
                return ResponseDocument.successResponse(result.toString());
            }
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit processAnalyze job: %s", e.getMessage()));
            if (e instanceof UIActionException) {
                // rethrow UI action exception to show error message
                throw e;
            }
            return ResponseDocument.failedResponse(
                    new LedpException(LedpCode.LEDP_18182, new String[]{"ProcessAnalyze", e.getMessage()}));
        }
    }

    @PostMapping("/import/csv")
    @ApiOperation(value = "Start import job")
    public ResponseDocument<String> startImportCSV(@RequestParam(value = "templateFileName") String templateFileName,
                                                   @RequestParam(value = "dataFileName") String dataFileName, @RequestParam(value = "source") String source, //
                                                   @RequestParam(value = "entity") String entity, //
                                                   @RequestParam(value = "feedType") String feedType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        try {
            ApplicationId applicationId = cdlService.submitCSVImport(customerSpace.toString(), templateFileName,
                    dataFileName, source, entity, feedType);
            if (applicationId != null) {
                return ResponseDocument.successResponse(applicationId.toString());
            } else {
                return ResponseDocument.successResponse("");
            }
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit import job: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"ImportFile", e.getMessage()});
        }
    }

    @ResponseBody
    @PostMapping("/s3/template")
    @ApiOperation(value = "Create s3 import template")
    public Map<String, UIAction> createS3Template(@RequestParam(value = "templateFileName") String templateFileName,
                                                  @RequestParam(value = "source", required = false, defaultValue = "File") String source, //
                                                  @RequestParam(value = "importData", required = false, defaultValue = "false") boolean importData,
                                                  @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        try {
            EntityType entityType = EntityType.fromDisplayNameToEntityType(templateDisplay.getObject());
            String entity = entityType.getEntity().name();
            String subType = entityType.getSubType() != null ? entityType.getSubType().name() : null;
            String feedType = StringUtils.isBlank(templateDisplay.getFeedType()) ? entityType.getDefaultFeedTypeName()
                    : templateDisplay.getFeedType();
            String taskId = cdlService.createS3Template(customerSpace.toString(), templateFileName, source, entity,
                    feedType, subType, templateDisplay.getTemplateName());

            UIAction uiAction;
            if (importData) {
                cdlService.submitS3ImportWithTemplateData(customerSpace.toString(), taskId, templateFileName);
                if (Boolean.TRUE.equals(templateDisplay.getExist())) {
                    uiAction = UIActionUtils.generateUIAction("", View.Banner, Status.Success,
                            String.format(editS3TemplateAndImportMsg, entity));
                } else {
                    uiAction = UIActionUtils.generateUIAction("", View.Banner, Status.Success,
                            String.format(createS3TemplateAndImportMsg, entity));
                }
            } else {
                if (Boolean.TRUE.equals(templateDisplay.getExist())) {
                    uiAction = UIActionUtils.generateUIAction("", View.Banner, Status.Success,
                            String.format(editS3TemplateMsg, entity));
                } else {
                    uiAction = UIActionUtils.generateUIAction("", View.Banner, Status.Success,
                            String.format(createS3TemplateMsg, entity));
                }
            }
            return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
        } catch (RuntimeException e) {
            log.error(String.format("Failed to create template for S3 import: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"S3CreateTemplateAndImport", e.getMessage()});
        }
    }

    @PostMapping("/s3/template/import")
    @ApiOperation(value = "Start s3 import job")
    public Map<String, UIAction> importS3Template(@RequestParam(value = "templateFileName") String templateFileName,
                                                  @RequestParam(value = "source", required = false, defaultValue = "File") String source, //
                                                  @RequestParam(value = "subType", required = false) String subType,
                                                  @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        try {
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source,
                    templateDisplay.getFeedType());
            if (dataFeedTask == null) {
                throw new RuntimeException("Cannot find template for S3 import!");
            }
            cdlService.submitS3ImportOnlyData(customerSpace.toString(), dataFeedTask.getUniqueId(), templateFileName);
            UIAction uiAction = UIActionUtils.generateUIAction("", View.Banner, Status.Success,
                    importUsingTemplateMsg);
            return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit S3 import: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"S3ImportFile", e.getMessage()});
        }
    }

    @PutMapping("/s3/template/displayname")
    @ApiOperation(value = "Update template display name")
    public ResponseDocument<String> updateTemplateName(
            @RequestParam(value = "source", required = false, defaultValue = "File") String source, //
            @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        try {
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source,
                    templateDisplay.getFeedType());
            if (dataFeedTask == null) {
                throw new RuntimeException("Cannot find template for S3 import!");
            }
            dataFeedTask.setTemplateDisplayName(templateDisplay.getTemplateName());
            dataFeedProxy.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
            return ResponseDocument.successResponse(dataFeedTask.getUniqueId());
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit S3 import: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"UpdateTemplateName", e.getMessage()});
        }
    }

    @PutMapping("/s3/template/status")
    @ApiOperation(value = "Update template import status")
    public ResponseDocument<String> updateS3TemplateStatus(
            @RequestParam(value = "source", required = false, defaultValue = "File") String source, //
            @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        try {
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source,
                    templateDisplay.getFeedType());
            if (dataFeedTask == null) {
                throw new RuntimeException("Cannot find template for S3 import!");
            }
            if (templateDisplay.getImportStatus() != null &&
                    !templateDisplay.getImportStatus().equals(dataFeedTask.getS3ImportStatus())) {
                dataFeedTask.setS3ImportStatus(templateDisplay.getImportStatus());
                dataFeedProxy.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
            }
            return ResponseDocument.successResponse(dataFeedTask.getUniqueId());
        } catch (RuntimeException e) {
            log.error(String.format("Failed to update S3 import status: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"UpdateS3ImportStatus", e.getMessage()});
        }
    }

    @PostMapping("/s3/template/reset")
    @ApiOperation(value = "Reset template")
    @ResponseBody
    public Map<String, UIMessage> resetTemplate(@RequestParam(value = "forceReset", required = false,
            defaultValue = "false") Boolean forceReset,
                                               @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        try {
            cdlService.resetTemplate(customerSpace.toString(), templateDisplay.getFeedType(), forceReset);
            UIAction uiAction = UIActionUtils.generateUIAction("", View.Banner, Status.Success,
                    resetTemplateMsg);
            return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
        } catch (LedpException e) {
            log.error(String.format("Failed to reset import template: %s", e.getMessage()));
            if (LedpCode.LEDP_40093.equals(e.getCode()) || LedpCode.LEDP_40089.equals(e.getCode())) {
                String errorMsg = LedpCode.LEDP_40093.equals(e.getCode()) ? removeExceptionCode(LedpCode.LEDP_40093,
                        e.getMessage()) : removeExceptionCode(LedpCode.LEDP_40089, e.getMessage());
                errorMsg = LedpException.buildMessage(LedpCode.LEDP_18244, new String[]{errorMsg});
                return ImmutableMap.of(UIMessage.class.getSimpleName(), generateUIMessage(Status.Error, errorMsg));
            } else if (LedpCode.LEDP_40090.equals(e.getCode()) || LedpCode.LEDP_40092.equals(e.getCode())) {
                String warningMsg = LedpCode.LEDP_40090.equals(e.getCode()) ? removeExceptionCode(LedpCode.LEDP_40090,
                        e.getMessage()) : removeExceptionCode(LedpCode.LEDP_40092, e.getMessage());
                warningMsg = LedpException.buildMessage(LedpCode.LEDP_18247, new String[]{warningMsg});
                return ImmutableMap.of(UIMessage.class.getSimpleName(), generateUIMessage(Status.Warning, warningMsg));
            } else {
                log.error("Unknown error code: " + e.getCode());
                throw e;
            }
        } catch (RuntimeException e) {
            log.error(String.format("Failed to reset import template: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18245, new String[]{e.getMessage()});
        }
    }

    @PostMapping("/soft-delete")
    @ApiOperation(value = "Start cleanup job")
    public Map<String, UIAction> softDelete(@RequestBody DeleteRequest deleteRequest) {
        // FIXME this API is now used for both soft/hard delete, coordinate with UI for
        // the endpoint url change
        UIAction uiAction = cdlService.delete(deleteRequest);
        return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
    }


    @PostMapping("/cleanupbyupload")
    @ApiOperation(value = "Start cleanup job")
    public Map<String, UIAction> cleanup(@RequestParam(value = "fileName") String fileName,
                                         @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation,
                                         @RequestParam(value = "cleanupOperationType") CleanupOperationType type) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        UIAction uiAction = cdlService.cleanup(customerSpace.toString(), fileName, schemaInterpretation, type);
        return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
    }

    @PostMapping("/cleanupbyrange")
    @ApiOperation(value = "Start cleanup job")
    public ResponseDocument<String> cleanupByRange(@RequestParam(value = "startTime") String startTime,
                                                   @RequestParam(value = "endTime") String endTime,
                                                   @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        try {
            ApplicationId applicationId = cdlService.cleanupByTimeRange(customerSpace.toString(), startTime, endTime,
                    schemaInterpretation);
            return ResponseDocument.successResponse(applicationId.toString());
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit cleanup by range job: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"Cleanup", e.getMessage()});
        }
    }

    @PostMapping("/cleanupall")
    @ApiOperation(value = "Start cleanup job")
    public ResponseDocument<String> cleanupAll(
            @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        try {
            cdlService.replaceData(customerSpace.toString(), schemaInterpretation);
            return ResponseDocument.successResponse("");
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit cleanup all job: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"Cleanup", e.getMessage()});
        }
    }

    @PostMapping("/replaceData")
    @ApiOperation(value = "create Replace Action to replace data")
    public ResponseDocument<String> replaceData(@RequestParam(value = "schema") SchemaInterpretation schemaInterpretation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        try {
            cdlService.replaceData(customerSpace.toString(), schemaInterpretation);
            return ResponseDocument.successResponse("");
        } catch (RuntimeException e) {
            log.error(String.format("Failed to create replace action: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"Replace", e.getMessage()});
        }
    }

    @GetMapping("/s3import/template")
    @ResponseBody
    @ApiOperation("get template table fields")
    public List<S3ImportTemplateDisplay> getS3ImportTemplateEntries(
            @RequestParam(required = false, defaultValue = "SystemDisplay") String sortBy) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return cdlService.getS3ImportTemplate(customerSpace.toString(), sortBy,
                ImmutableSet.of(EntityType.CustomIntent));
    }

    @GetMapping("/s3import/fileList")
    @ResponseBody
    @ApiOperation("get file list under s3Path")
    public List<FileProperty> getFileList(@RequestParam String s3Path) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        return cdlService.getFileListForS3Path(customerSpace.toString(), s3Path, "csv");
    }

    @PostMapping("/s3import/system")
    @ResponseBody
    @ApiOperation("create new S3 Import system")
    public Map<String, UIAction> createS3ImportSystem(@RequestParam String systemDisplayName,
                                                      @RequestParam S3ImportSystem.SystemType systemType,
                                                      @RequestParam(value = "primary", required = false,
                                                              defaultValue = "false") Boolean primary) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        try {
            String systemName = cdlService.createS3ImportSystem(customerSpace.toString(), systemDisplayName, systemType,
                    primary);
            switch (systemType) {
                case Salesforce:
                    cdlService.createDefaultOpportunityTemplate(customerSpace.toString(), systemName);
                    break;
                case Marketo:
                case Eloqua:
                case Pardot:
                    cdlService.createDefaultMarketingTemplate(customerSpace.toString(), systemName, systemType.toString());
                    break;
                default:
                    break;
            }
            UIAction uiAction = UIActionUtils.generateUIAction("", View.Banner, Status.Success,
                    String.format(createS3ImportSystemMsg, systemDisplayName));
            return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
        } catch (LedpException e) {
            log.error("Failed to create S3ImportSystem: " + e.getMessage());
            UIAction action = UIActionUtils.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, e.getCode());
        }
    }

    @Deprecated
    @GetMapping("/s3import/system/list")
    @ResponseBody
    @ApiOperation("create new S3 Import system")
    public List<S3ImportSystem> getS3ImportSystemList(
            @RequestParam(value = "Account", required = false, defaultValue = "false") Boolean filterByAccountSystemId,
            @RequestParam(value = "Contact", required = false, defaultValue = "false") Boolean filterByContactSystemId,
            @RequestBody(required = false) S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return cdlService.getS3ImportSystemWithFilter(customerSpace.toString(),
                Boolean.TRUE.equals(filterByAccountSystemId),
                Boolean.TRUE.equals(filterByContactSystemId),
                templateDisplay);
    }

    @PostMapping("/s3import/system/mappinglist")
    @ResponseBody
    @ApiOperation("Get S3ImportSystem list for Id mapping")
    public List<S3ImportSystem> getS3ImportSystemListForMapping(
            @RequestParam(value = "Account", required = false, defaultValue = "false") Boolean filterByAccountSystemId,
            @RequestParam(value = "Contact", required = false, defaultValue = "false") Boolean filterByContactSystemId,
            @RequestBody(required = false) S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return cdlService.getS3ImportSystemWithFilter(customerSpace.toString(),
                Boolean.TRUE.equals(filterByAccountSystemId),
                Boolean.TRUE.equals(filterByContactSystemId),
                templateDisplay);
    }

    @PostMapping("/s3import/system/list")
    @ResponseBody
    @ApiOperation("update import system priority based on sequence")
    public Map<String, UIAction> updateSystemPriorityBasedOnSequence(@RequestBody List<S3ImportSystem> systemList) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        try {
            cdlService.updateS3ImportSystemPriorityBasedOnSequence(customerSpace.toString(), systemList);
            UIAction uiAction = UIActionUtils.generateUIAction("", View.Banner, Status.Success,
                    updateS3ImportSystemPriorityMsg);
            return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
        } catch (RuntimeException e) {
            log.error("Failed to Update system priority: " + e.getMessage());
            throw new LedpException(LedpCode.LEDP_18223, new String[] {e.getMessage()});
        }
    }

    @PostMapping("/s3import/system/list/validateandupdate")
    @ResponseBody
    @ApiOperation("Try update import system priority based on sequence with validation")
    public Map<String, UIMessage> validateAndUpdateSystemPriority(@RequestBody List<S3ImportSystem> systemList) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        try {
            if (cdlService.validateAndUpdateS3ImportSystemPriority(customerSpace.toString(), systemList)) {
                UIAction uiAction = UIActionUtils.generateUIAction("", View.Banner, Status.Success,
                        validateAndUpdatePriorityMsg);
                return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
            } else {
                throw new LedpException(LedpCode.LEDP_18248);
            }
        } catch (LedpException e) {
            if (LedpCode.LEDP_40091.equals(e.getCode())) {
                return ImmutableMap.of(UIMessage.class.getSimpleName(), generateUIMessage(Status.Warning,
                        LedpCode.LEDP_18246.getMessage()));
            } else {
                if (!LedpCode.LEDP_18248.equals(e.getCode())) {
                    log.error("Unknown exception code: " + e.getCode());
                }
                throw e;
            }
        }
    }

    @GetMapping("/s3import/system")
    @ResponseBody
    @ApiOperation("Get S3 Import system")
    public ResponseDocument<S3ImportSystem> getS3ImportSystem(@RequestParam String systemName) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return ResponseDocument.successResponse(cdlService.getS3ImportSystem(customerSpace.toString(), systemName));
    }

    @PostMapping("s3import/template/preview")
    @ResponseBody
    @ApiOperation("Get template preview")
    public List<TemplateFieldPreview> getTemplatePreview(
            @RequestParam(value = "source", required = false, defaultValue = "File") String source, //
            @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        try {
            DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, source, templateDisplay);
            boolean enableEntityMatch = batonService.isEntityMatchEnabled(customerSpace);
            EntityType entityType = EntityTypeUtils.matchFeedType(templateDisplay.getFeedType());
            log.info("1. Get standard template");
            Table standardTable;
            BusinessEntity entity = BusinessEntity.getByName(dataFeedTask.getEntity());
            if (entityType != null && templateDisplay.getS3ImportSystem() != null) {
                standardTable = SchemaRepository.instance().getSchema(templateDisplay.getS3ImportSystem().getSystemType(),
                        entityType, enableEntityMatch, batonService.onlyEntityMatchGAEnabled(customerSpace));
            } else {
                standardTable = SchemaRepository.instance().getSchema(
                        entity, true, false, enableEntityMatch,
                        batonService.onlyEntityMatchGAEnabled(customerSpace));
            }
            Set<String> matchingFields =
                    SchemaRepository.instance().matchingAttributes(entity, enableEntityMatch)
                            .stream()
                            .map(Attribute::getName)
                            .collect(Collectors.toSet());
            List<TemplateFieldPreview> fieldPreviews = cdlService.getTemplatePreview(customerSpace.toString(),
                    dataFeedTask.getImportTemplate(), standardTable, matchingFields);
            updateProductUniqueId(entity, fieldPreviews);
            if (CollectionUtils.isEmpty(fieldPreviews)) {
                return fieldPreviews;
            }
            log.info("2. Get System list");
            List<S3ImportSystem> systemList = cdlService.getAllS3ImportSystem(customerSpace.toString());
            log.info("3. Update Match Id field");
            Set<String> systemIds = updateUniqueAndMatchIdField(fieldPreviews, systemList, entityType,
                    templateDisplay.getS3ImportSystem());
            Map<String, String> standardNameMapping =
                    standardTable.getAttributes()
                            .stream()
                            .collect(Collectors.toMap(Attribute::getName, Attribute::getDisplayName));
            log.info("4. Get decorated display name mapping");
            try (PerformanceTimer ignored =
                         new PerformanceTimer("GetDecoratedDisplayNameMapping: Tenant=" + customerSpace.getTenantId() + " entity=" + entityType)) {
                Map<String, String> nameMapping = cdlService.getDecoratedDisplayNameMapping(customerSpace.toString(), entityType);

                log.info(String.format("Total %d decorated display name pair.", nameMapping.size()));

                log.info("5. Update display name with decorated value.");
                fieldPreviews.forEach(preview -> {
                    if (nameMapping.containsKey(preview.getNameInTemplate())) {
                        preview.setDisplayName(nameMapping.get(preview.getNameInTemplate()));
                    } else if (standardNameMapping.containsKey(preview.getNameInTemplate())) {
                        preview.setDisplayName(standardNameMapping.get(preview.getNameInTemplate()));
                    } else if (!systemIds.contains(preview.getNameInTemplate())) {
                        preview.setDisplayName(preview.getNameFromFile());
                    }
                });
            }
            return fieldPreviews;
        } catch (RuntimeException e) {
            log.error("Get template preview Failed: " + e.toString());
            throw new LedpException(LedpCode.LEDP_18218, new String[]{e.getMessage()});
        }
    }

    /**
     * hardcode the id logic the product previews
     * @param entity
     * @param previews
     */
    private void updateProductUniqueId(BusinessEntity entity, List<TemplateFieldPreview> previews) {

        if (BusinessEntity.Product == entity && CollectionUtils.isNotEmpty(previews)) {
            previews.stream()
                    .filter(e -> InterfaceName.ProductId.name().equals(e.getNameInTemplate()))
                    .findFirst().ifPresent(preview -> preview.setLatticeFieldCategory(LatticeFieldCategory.UniqueId));
        }
    }

    private Set<String> updateUniqueAndMatchIdField(List<TemplateFieldPreview> fieldPreviews,
                                                    List<S3ImportSystem> s3ImportSystems,
                                                    EntityType entityType,
                                                    S3ImportSystem currentSystem) {
        if (CollectionUtils.isEmpty(s3ImportSystems) || entityType == null) {
            return Collections.emptySet();
        }
        log.info("Update UniqueId Preview.");
        String currentSystemName = null;
        if (currentSystem != null) {
            currentSystemName = currentSystem.getName();
        }
        Map<String, Pair<S3ImportSystem, EntityType>> accountSystemIdMap = new HashMap<>();
        Map<String, Pair<S3ImportSystem, EntityType>> contactSystemIdMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(s3ImportSystems)) {
            s3ImportSystems.forEach(s3ImportSystem -> {
                accountSystemIdMap.put(s3ImportSystem.getAccountSystemId(), Pair.of(s3ImportSystem,
                        s3ImportSystem.getSystemType().getPrimaryAccount()));
                if (s3ImportSystem.getSecondaryAccountIds() != null) {
                    for (Map.Entry<String, EntityType> entry :
                            s3ImportSystem.getSecondaryAccountIds().getSecondaryIdToEntityTypeMap().entrySet()) {
                        accountSystemIdMap.put(entry.getKey(), Pair.of(s3ImportSystem, entry.getValue()));
                    }
                }
                contactSystemIdMap.put(s3ImportSystem.getContactSystemId(), Pair.of(s3ImportSystem,
                        s3ImportSystem.getSystemType().getPrimaryContact()));
                if (s3ImportSystem.getSecondaryContactIds() != null) {
                    for (Map.Entry<String, EntityType> entry :
                            s3ImportSystem.getSecondaryContactIds().getSecondaryIdToEntityTypeMap().entrySet()) {
                        contactSystemIdMap.put(entry.getKey(), Pair.of(s3ImportSystem, entry.getValue()));
                    }
                }
            });
        }
        if (!(MapUtils.isEmpty(accountSystemIdMap) && MapUtils.isEmpty(contactSystemIdMap))) {
            for (TemplateFieldPreview fieldPreview : fieldPreviews) {
                switch (entityType) {
                    case Accounts:
                        if (accountSystemIdMap.containsKey(fieldPreview.getNameInTemplate())) {
                            fieldPreview.setFieldCategory(FieldCategory.LatticeField);
                            Pair<S3ImportSystem, EntityType> attrInfoPair = accountSystemIdMap.get(fieldPreview.getNameInTemplate());
                            S3ImportSystem matchedSystem = attrInfoPair.getLeft();
                            EntityType matchedEntityType = attrInfoPair.getRight();
                            if (entityType == matchedEntityType && matchedSystem.getName().equals(currentSystemName)) {
                                fieldPreview.setLatticeFieldCategory(LatticeFieldCategory.UniqueId);
                            } else {
                                fieldPreview.setLatticeFieldCategory(LatticeFieldCategory.MatchId);
                            }
                            fieldPreview.setDisplayName(String.format("%s %s ID", attrInfoPair.getLeft().getName(),
                                    convertPluralToSingular(attrInfoPair.getRight().getDisplayName())));
                        }
                        break;
                    case Contacts:
                    case Leads:
                    case ProductPurchases:
                        if (contactSystemIdMap.containsKey(fieldPreview.getNameInTemplate())) {
                            fieldPreview.setFieldCategory(FieldCategory.LatticeField);
                            Pair<S3ImportSystem, EntityType> attrInfoPair = contactSystemIdMap.get(fieldPreview.getNameInTemplate());
                            S3ImportSystem matchedSystem = attrInfoPair.getLeft();
                            EntityType matchedEntityType = attrInfoPair.getRight();
                            if (entityType == matchedEntityType && matchedSystem.getName().equals(currentSystemName)) {
                                fieldPreview.setLatticeFieldCategory(LatticeFieldCategory.UniqueId);
                            } else {
                                fieldPreview.setLatticeFieldCategory(LatticeFieldCategory.MatchId);
                            }
                            fieldPreview.setDisplayName(String.format("%s %s ID", attrInfoPair.getLeft().getName(),
                                    convertPluralToSingular(attrInfoPair.getRight().getDisplayName())));
                        }
                        if (accountSystemIdMap.containsKey(fieldPreview.getNameInTemplate())) {
                            fieldPreview.setFieldCategory(FieldCategory.LatticeField);
                            fieldPreview.setLatticeFieldCategory(LatticeFieldCategory.MatchId);
                            Pair<S3ImportSystem, EntityType> attrInfoPair = accountSystemIdMap.get(fieldPreview.getNameInTemplate());
                            S3ImportSystem matchedSystem = attrInfoPair.getLeft();
                            EntityType matchedEntityType = attrInfoPair.getRight();
                            if (entityType == matchedEntityType && matchedSystem.getName().equals(currentSystemName)) {
                                fieldPreview.setLatticeFieldCategory(LatticeFieldCategory.UniqueId);
                            } else {
                                fieldPreview.setLatticeFieldCategory(LatticeFieldCategory.MatchId);
                            }
                            fieldPreview.setDisplayName(String.format("%s %s ID", attrInfoPair.getLeft().getName(),
                                    convertPluralToSingular(attrInfoPair.getRight().getDisplayName())));
                        }
                        break;
                    default:
                }
            }
        }
        Set<String> systemIds = new HashSet<>();
        if (MapUtils.isNotEmpty(accountSystemIdMap)) {
            systemIds.addAll(accountSystemIdMap.keySet());
        }
        if (MapUtils.isNotEmpty(contactSystemIdMap)) {
            systemIds.addAll(contactSystemIdMap.keySet());
        }
        return systemIds;
    }

    private String convertPluralToSingular(String displayName) {
        if (StringUtils.isNotEmpty(displayName) && displayName.endsWith("s")) {
            return displayName.substring(0, displayName.length() - 1);
        }
        return displayName;
    }

    private DataFeedTask getDataFeedTask(CustomerSpace customerSpace, String source, S3ImportTemplateDisplay templateDisplay) {
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source,
                templateDisplay.getFeedType());
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find template for S3 import!");
        }
        return dataFeedTask;
    }

    @PostMapping("s3import/template/downloadcsv")
    @ResponseBody
    @ApiOperation("Download template csv file")
    public void downloadTemplateCSV(HttpServletRequest request, HttpServletResponse response,
                                    @RequestParam(value = "source", required = false, defaultValue = "File") String source,
                                    @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        try {
            // get template preview, then render the csv
            List<TemplateFieldPreview> previews = getTemplatePreview(source, templateDisplay);
            String fileContent = cdlService.getTemplateMappingContent(previews);
            DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
            String dateString = dateFormat.format(new Date());
            // generate file name with feed type and date
            String fileName = String.format("template_%s_%s.csv", templateDisplay.getFeedType(), dateString);
            TemplateFileHttpDownloader.TemplateFileHttpDownloaderBuilder builder = new TemplateFileHttpDownloader.TemplateFileHttpDownloaderBuilder();
            builder.setMimeType("application/csv").setFileName(fileName).setFileContent(fileContent).setBatonService(batonService);
            TemplateFileHttpDownloader downloader = new TemplateFileHttpDownloader(builder);
            downloader.downloadFile(request, response);
        } catch (RuntimeException e) {
            log.error("Download template csv Failed: " + e.getMessage());
            throw new LedpException(LedpCode.LEDP_18218, new String[]{e.getMessage()});
        }
    }

    @PostMapping("/s3import/template/create/webvisit")
    @ResponseBody
    @ApiOperation("Create WebVist template")
    public boolean createWebVisitTemplate(@RequestParam(value = "entityType") EntityType entityType,
                                          @RequestParam("file") MultipartFile file) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        try {
            return cdlService.createWebVisitProfile(customerSpace.toString(), entityType, file.getInputStream());
        } catch (IOException e) {
            log.error("Cannot open csv file as stream! {}", e.getMessage());
            return false;
        }
    }

    @PostMapping("/s3import/template/create/opportunity")
    @ResponseBody
    @ApiOperation("Create Opportunity template")
    public boolean createDefaultOpportunity(@RequestParam("systemName") String systemName) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        return cdlService.createDefaultOpportunityTemplate(customerSpace.toString(), systemName);
    }

    @PostMapping("/s3import/template/create/marketing")
    @ResponseBody
    @ApiOperation("Create Marketing template")
    public boolean createDefaultMarketing(@RequestParam("systemName") String systemName,
                                          @RequestParam("systemType") String systemType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }

        return cdlService.createDefaultMarketingTemplate(customerSpace.toString(), systemName, systemType);
    }

    @PostMapping("/s3import/template/create/dnbIntentData")
    @ResponseBody
    @ApiOperation("Create DnbIntentData template")
    public boolean createDefaultDnbIntentData() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return cdlService.createDefaultDnbIntentDataTemplate(customerSpace.toString());
    }

    @GetMapping("/bundle/upload")
    @ResponseBody
    @ApiOperation("")
    public boolean checkUploadBundleFile() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        try {
            return cdlService.checkBundleUpload(customerSpace.toString());
        } catch(Exception e) {
            log.error("Can't upload bundle file due to {}", e.getMessage());
        }
        return false;
    }

    @GetMapping("/s3import/template/getDimensionMetadataInStream")
    @ResponseBody
    @ApiOperation("get dimension metadata using streamName")
    public Map<String, List<Map<String, Object>>> getDimensionMetadataInStream(@RequestParam("systemName") String systemName,
                                                                               @RequestParam("entityType") EntityType entityType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return cdlService.getDimensionMetadataInStream(customerSpace.toString(), systemName, entityType);
    }

    @PostMapping("s3import/template/downloadDimensionMetadataInStream")
    @ResponseBody
    @ApiOperation("Download DimensionMetadata csv file using streamName and dimensionName")
    public void downloadDimensionMetadataInStream(HttpServletRequest request, HttpServletResponse response,
                                                  @RequestParam("systemName") String systemName,
                                                  @RequestParam("entityType") EntityType entityType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
        String dateString = dateFormat.format(new Date());
        String fileName = String.format("%s_%s_%s.csv", systemName, entityType, dateString);
        try {
            cdlService.downloadDimensionMetadataInStream(request, response, "application/csv", fileName,
                    customerSpace.toString(), systemName, entityType);
        } catch (RuntimeException e) {
            log.error("Download DimensionMetadata csv Failed: " + e.getMessage());
            String title = "500 Internal Server Error Occurred.";
            UIActionCode code = UIActionCode.fromLedpCode(LedpCode.LEDP_40076);
            UIAction action = UIActionUtils.generateUIError(title, View.Banner, code);
            throw UIActionException.fromAction(action);
        }
    }

    @GetMapping("/import/csv")
    @ResponseBody
    @ApiOperation(value = "Get all import files")
    public List<ImportFileInfo> getAllImportFiles() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        return cdlService.getAllImportFiles(customerSpace.toString());
    }

    @PostMapping("/generateintentalert")
    @ApiOperation(value = "Generate Intent email alert")
    public ResponseDocument<String> generateIntentAlert() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);

        try {
            ApplicationId result = cdlService.generateIntentAlert(customerSpace.toString());
            if (result == null) {
                return ResponseDocument.successResponse(null);
            } else {
                return ResponseDocument.successResponse(result.toString());
            }
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit generateIntentAlert job: %s", e.getMessage()));
            return ResponseDocument.failedResponse(e);
        }
    }

    private String removeExceptionCode(LedpCode code, String errorMessage) {
        if (StringUtils.isEmpty(errorMessage)) {
            return errorMessage;
        }
        return errorMessage.replace(code.name() + ": ", "");
    }

    private UIMessage generateUIMessage(Status status, String msg) {
        UIMessage uiMessage = new UIMessage();
        uiMessage.setStatus(status);
        uiMessage.setMessage(msg);
        return uiMessage;
    }
}
