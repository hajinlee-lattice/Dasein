package com.latticeengines.pls.controller;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.TemplateFieldPreview;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.download.TemplateFileHttpDownloader;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.SystemStatusService;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;
import com.latticeengines.proxy.exposed.cdl.CDLJobProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdl consolidate and profile", description = "REST resource for cdl")
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

    @RequestMapping(value = "/consolidateAndProfile", method = RequestMethod.POST)
    @ApiOperation(value = "Start Consolidate And Profile job")
    public ResponseDocument<String> startConsolidateAndProfileJob() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        ApplicationId result = cdlJobProxy.createConsolidateJob(customerSpace.toString());
        return ResponseDocument.successResponse(result.toString());
    }

    @RequestMapping(value = "/processanalyze", method = RequestMethod.POST)
    @ApiOperation(value = "Start Process And Analyze job")
    public ResponseDocument<String> processAnalyze(@RequestBody(required = false) ProcessAnalyzeRequest request) {
        StatusDocument statusDocument = systemStatusService.getSystemStatus();
        if (StatusDocument.UNDER_MAINTENANCE.equals(statusDocument.getStatus())) {
            return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18182));
        }
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
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

    @RequestMapping(value = "/import/csv", method = RequestMethod.POST)
    @ApiOperation(value = "Start import job")
    public ResponseDocument<String> startImportCSV(@RequestParam(value = "templateFileName") String templateFileName,
                                                   @RequestParam(value = "dataFileName") String dataFileName, @RequestParam(value = "source") String source, //
                                                   @RequestParam(value = "entity") String entity, //
                                                   @RequestParam(value = "feedType") String feedType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        try {
            ApplicationId applicationId = cdlService.submitCSVImport(customerSpace.toString(), templateFileName,
                    dataFileName, source, entity, feedType);
            return ResponseDocument.successResponse(applicationId.toString());
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit import job: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"ImportFile", e.getMessage()});
        }
    }

    @ResponseBody
    @RequestMapping(value = "/s3/template", method = RequestMethod.POST)
    @ApiOperation(value = "Create s3 import template")
    public Map<String, UIAction> createS3Template(@RequestParam(value = "templateFileName") String templateFileName,
                                                  @RequestParam(value = "source", required = false, defaultValue = "File") String source, //
                                                  @RequestParam(value = "importData", required = false, defaultValue = "false") boolean importData,
                                                  @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        try {
            EntityType entityType = EntityType.fromDisplayNameToEntityType(templateDisplay.getObject());
            String entity = entityType.getEntity().name();
            String subType = entityType.getSubType() != null ? entityType.getSubType().name() : null;
            String feedType = StringUtils.isBlank(templateDisplay.getFeedType()) ? entityType.getDefaultFeedTypeName()
                    : templateDisplay.getFeedType();
            String taskId = cdlService.createS3Template(customerSpace.toString(), templateFileName, source, entity,
                    feedType, subType, templateDisplay.getTemplateName());

            UIAction uiAction = null;
            if (importData) {
                cdlService.submitS3ImportWithTemplateData(customerSpace.toString(), taskId, templateFileName);
                if (Boolean.TRUE.equals(templateDisplay.getExist())) {
                    uiAction = graphDependencyToUIActionUtil.generateUIAction("", View.Banner, Status.Success,
                            String.format(editS3TemplateAndImportMsg, entity));
                } else {
                    uiAction = graphDependencyToUIActionUtil.generateUIAction("", View.Banner, Status.Success,
                            String.format(createS3TemplateAndImportMsg, entity));
                }
                return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
            } else {
                if (Boolean.TRUE.equals(templateDisplay.getExist())) {
                    uiAction = graphDependencyToUIActionUtil.generateUIAction("", View.Banner, Status.Success,
                            String.format(editS3TemplateMsg, entity));
                } else {
                    uiAction = graphDependencyToUIActionUtil.generateUIAction("", View.Banner, Status.Success,
                            String.format(createS3TemplateMsg, entity));
                }
                return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
            }
        } catch (RuntimeException e) {
            log.error(String.format("Failed to create template for S3 import: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"S3CreateTemplateAndImport", e.getMessage()});
        }
    }

    @RequestMapping(value = "/s3/template/import", method = RequestMethod.POST)
    @ApiOperation(value = "Start s3 import job")
    public Map<String, UIAction> importS3Template(@RequestParam(value = "templateFileName") String templateFileName,
                                                  @RequestParam(value = "source", required = false, defaultValue = "File") String source, //
                                                  @RequestParam(value = "subType", required = false) String subType,
                                                  @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        try {
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source,
                    templateDisplay.getFeedType());
            if (dataFeedTask == null) {
                throw new RuntimeException("Cannot find template for S3 import!");
            }
            cdlService.submitS3ImportOnlyData(customerSpace.toString(), dataFeedTask.getUniqueId(), templateFileName);
            UIAction uiAction = graphDependencyToUIActionUtil.generateUIAction("", View.Banner, Status.Success,
                    importUsingTemplateMsg);
            return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit S3 import: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"S3ImportFile", e.getMessage()});
        }
    }

    @RequestMapping(value = "/s3/template/displayname", method = RequestMethod.PUT)
    @ApiOperation(value = "Update template display name")
    public ResponseDocument<String> updateTemplateName(
            @RequestParam(value = "source", required = false, defaultValue = "File") String source, //
            @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
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

    @RequestMapping(value = "/s3/template/status", method = RequestMethod.PUT)
    @ApiOperation(value = "Update template import status")
    public ResponseDocument<String> updateS3TemplateStatus(
            @RequestParam(value = "source", required = false, defaultValue = "File") String source, //
            @RequestBody S3ImportTemplateDisplay templateDisplay) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
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

    @RequestMapping(value = "/cleanupbyupload", method = RequestMethod.POST)
    @ApiOperation(value = "Start cleanup job")
    public Map<String, UIAction> cleanup(@RequestParam(value = "fileName") String fileName,
                                         @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation,
                                         @RequestParam(value = "cleanupOperationType") CleanupOperationType type) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        UIAction uiAction = cdlService.cleanup(customerSpace.toString(), fileName, schemaInterpretation, type);
        return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
    }

    @RequestMapping(value = "/cleanupbyrange", method = RequestMethod.POST)
    @ApiOperation(value = "Start cleanup job")
    public ResponseDocument<String> cleanupByRange(@RequestParam(value = "startTime") String startTime,
                                                   @RequestParam(value = "endTime") String endTime,
                                                   @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        try {
            ApplicationId applicationId = cdlService.cleanupByTimeRange(customerSpace.toString(), startTime, endTime,
                    schemaInterpretation);
            return ResponseDocument.successResponse(applicationId.toString());
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit cleanup by range job: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"Cleanup", e.getMessage()});
        }
    }

    @RequestMapping(value = "/cleanupall", method = RequestMethod.POST)
    @ApiOperation(value = "Start cleanup job")
    public ResponseDocument<String> cleanupAll(
            @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        try {
            ApplicationId applicationId = cdlService.cleanupAllData(customerSpace.toString(), schemaInterpretation);
            return ResponseDocument.successResponse(applicationId.toString());
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit cleanup all job: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[]{"Cleanup", e.getMessage()});
        }
    }

    @GetMapping(value = "/s3import/template")
    @ResponseBody
    @ApiOperation("get template table fields")
    public List<S3ImportTemplateDisplay> getS3ImportTemplateEntries() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return cdlService.getS3ImportTemplate(customerSpace.toString());
    }

    @GetMapping(value = "/s3import/fileList")
    @ResponseBody
    @ApiOperation("get file list under s3Path")
    public List<FileProperty> getFileList(@RequestParam String s3Path) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return cdlService.getFileListForS3Path(customerSpace.toString(), s3Path, "csv");
    }

    @PostMapping(value = "/s3import/system")
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
            cdlService.createS3ImportSystem(customerSpace.toString(), systemDisplayName, systemType, primary);
            UIAction uiAction = graphDependencyToUIActionUtil.generateUIAction("", View.Banner, Status.Success,
                    String.format(createS3ImportSystemMsg, systemDisplayName));
            return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
        } catch (RuntimeException e) {
            log.error("Failed to create S3ImportSystem: " + e.getMessage());
            throw new LedpException(LedpCode.LEDP_18216, new String[]{systemDisplayName});
        }
    }

    @GetMapping(value = "/s3import/system/list")
    @ResponseBody
    @ApiOperation("create new S3 Import system")
    public List<S3ImportSystem> getS3ImportSystemList(
            @RequestParam(value = "Account", required = false, defaultValue = "false") Boolean filterByAccountSystemId,
            @RequestParam(value = "Contact", required = false, defaultValue = "false") Boolean filterByContactSystemId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return cdlService.getAllS3ImportSystem(customerSpace.toString());
    }

    @PostMapping(value = "/s3import/system/list")
    @ResponseBody
    @ApiOperation("update import system priority based on sequence")
    public Map<String, UIAction> updateSystemPriorityBasedOnSequence(@RequestBody List<S3ImportSystem> systemList) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        try {
            cdlService.updateS3ImportSystemPriorityBasedOnSequence(customerSpace.toString(), systemList);
            UIAction uiAction = graphDependencyToUIActionUtil.generateUIAction("", View.Banner, Status.Success,
                    updateS3ImportSystemPriorityMsg);
            return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
        } catch (RuntimeException e) {
            log.error("Failed to Update system priority: " + e.getMessage());
            throw new LedpException(LedpCode.LEDP_18222, new String[] {e.getMessage()});
        }
    }

    @GetMapping(value = "/s3import/system")
    @ResponseBody
    @ApiOperation("Get S3 Import system")
    public ResponseDocument<S3ImportSystem> getS3ImportSystem(@RequestParam String systemName) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return ResponseDocument.successResponse(cdlService.getS3ImportSystem(customerSpace.toString(), systemName));
    }

    @PostMapping(value = "s3import/template/preview")
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
            boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
            Table standardTable = SchemaRepository.instance().getSchema(
                    BusinessEntity.getByName(dataFeedTask.getEntity()), true, false, enableEntityMatch);
            return cdlService.getTemplatePreview(customerSpace.toString(),
                    dataFeedTask.getImportTemplate(), standardTable);
        } catch (RuntimeException e) {
            log.error("Get template preview Failed: " + e.getMessage());
            throw new LedpException(LedpCode.LEDP_18218, new String[]{e.getMessage()});
        }
    }

    private DataFeedTask getDataFeedTask(CustomerSpace customerSpace, String source, S3ImportTemplateDisplay templateDisplay) {
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source,
                templateDisplay.getFeedType());
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find template for S3 import!");
        }
        return dataFeedTask;
    }

    @RequestMapping(value = "s3import/template/downloadcsv", headers = "Accept=application/json", method =
            RequestMethod.POST)
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
            DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, source, templateDisplay);
            boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
            Table standardTable = SchemaRepository.instance().getSchema(
                    BusinessEntity.getByName(dataFeedTask.getEntity()), true, false, enableEntityMatch);
            String fileContent = cdlService.getTemplateMappingContent(dataFeedTask.getImportTemplate(), standardTable);
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
}
