package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.service.DataFeedTaskTemplateService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.ImportTemplateDiagnostic;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.cdl.VdbImportConfig;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafile", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/datacollection/datafeed/tasks")
public class DataFeedTaskController {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskController.class);

    private final DataFeedTaskManagerService dataFeedTaskManagerService;

    @Inject
    private DataFeedTaskTemplateService dataFeedTaskTemplateService;

    @Inject
    public DataFeedTaskController(DataFeedTaskManagerService dataFeedTaskManagerService) {
        this.dataFeedTaskManagerService = dataFeedTaskManagerService;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> createDataFeedTaskForVdb(@PathVariable String customerSpace,
            @RequestParam(value = "source") String source, @RequestParam(value = "feedtype") String feedtype,
            @RequestParam(value = "entity") String entity, @RequestBody VdbLoadTableConfig vdbLoadTableConfig) {
        if (vdbLoadTableConfig == null) {
            return ResponseDocument.failedResponse(new RuntimeException("Vdb load table config can't be null!"));
        } else {
            VdbImportConfig vdbImportConfig = new VdbImportConfig();
            vdbImportConfig.setVdbLoadTableConfig(vdbLoadTableConfig);
            return createDataFeedTask(customerSpace, source, feedtype, entity, "", "", false, "", vdbImportConfig);
        }
    }

    @RequestMapping(value = "/create", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> createDataFeedTask(@PathVariable String customerSpace,
            @RequestParam(value = "source") String source, @RequestParam(value = "feedtype") String feedtype,
            @RequestParam(value = "entity") String entity,
            @RequestParam(value = "subType", required = false) String subType,
            @RequestParam(value = "displayName", required = false) String displayName,
            @RequestParam(value = "sendEmail", required = false, defaultValue = "false") boolean sendEmail,
            @RequestParam(value = "user", required = false) String user, @RequestBody CDLImportConfig importConfig) {
        try {
            customerSpace = CustomerSpace.parse(customerSpace).toString();
            entity = BusinessEntity.getByName(entity).name();
            String taskId = dataFeedTaskManagerService.createDataFeedTask(customerSpace, feedtype, entity, source,
                    subType, displayName != null ? java.net.URLDecoder.decode(displayName, "UTF-8") : null, sendEmail,
                    user, importConfig);
            return ResponseDocument.successResponse(taskId);
        } catch (Exception e) {
            log.error(String.format("Failed to create data feed task, exception: %s", e.toString()), e);
            return ResponseDocument.failedResponse(e);
        }

    }

    @RequestMapping(value = "/import/{taskIdentifier}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> startImportJobForVdb(@PathVariable String customerSpace,
            @PathVariable String taskIdentifier, @RequestBody VdbLoadTableConfig vdbLoadTableConfig) {
        VdbImportConfig vdbImportConfig = new VdbImportConfig();
        vdbImportConfig.setVdbLoadTableConfig(vdbLoadTableConfig);
        return startImportJob(customerSpace, taskIdentifier, false, vdbImportConfig);
    }

    @RequestMapping(value = "/import/internal/{taskIdentifier}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> startImportJob(@PathVariable String customerSpace,
            @PathVariable String taskIdentifier,
            @RequestParam(value = "onlyData", required = false, defaultValue = "false") boolean onlyData,
            @RequestBody CDLImportConfig importConfig) {
        try {
            if (onlyData) {
                String applicationId = dataFeedTaskManagerService.submitDataOnlyImportJob(customerSpace, taskIdentifier,
                        (CSVImportConfig) importConfig);
                return ResponseDocument.successResponse(applicationId);
            } else {
                String applicationId = dataFeedTaskManagerService.submitImportJob(customerSpace, taskIdentifier,
                        importConfig);
                return ResponseDocument.successResponse(applicationId);
            }
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/s3import", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> startImportJobForS3(@PathVariable String customerSpace,
            @RequestBody S3FileToHdfsConfiguration s3FileToHdfsConfiguration) {
        try {
            String applicationId = dataFeedTaskManagerService.submitS3ImportJob(customerSpace,
                    s3FileToHdfsConfiguration);
            return ResponseDocument.successResponse(applicationId);
        } catch (Exception e) {
            log.info("Failed to start s3 import", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/reset", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    public ResponseDocument<Boolean> resetImport(@PathVariable String customerSpace,
            @RequestParam(value = "entity", required = false) BusinessEntity entity) {
        if (dataFeedTaskManagerService.resetImport(customerSpace, entity)) {
            return ResponseDocument.successResponse(true);
        } else {
            return ResponseDocument.failedResponse(new RuntimeException("Cannot reset datafeed task."));
        }

    }

    @PostMapping(value = "/setup/webvisit")
    @ResponseBody
    @ApiOperation(value = "Create a WebVisit template")
    public ResponseDocument<Boolean> createWebVisitTemplate(
            @PathVariable String customerSpace,
            @RequestBody List<SimpleTemplateMetadata> simpleTemplateMetadataList,
            @RequestParam(value = "enableGA", required = false, defaultValue = "false") boolean enableGA) {
        if (!dataFeedTaskTemplateService.validateGAEnabled(customerSpace, enableGA)) {
            return ResponseDocument.failedResponse(new IllegalStateException("EntityMatchGATenant doesn't support to " +
                    "create WebVisit template."));
        }
        try {
            if (CollectionUtils.isNotEmpty(simpleTemplateMetadataList)) {
                Boolean result = Boolean.TRUE;
                for (SimpleTemplateMetadata simpleTemplateMetadata : simpleTemplateMetadataList) {
                    result = result && dataFeedTaskTemplateService.setupWebVisitProfile(customerSpace,
                            simpleTemplateMetadata);
                }
                return ResponseDocument.successResponse(result);
            }
            return ResponseDocument.successResponse(Boolean.FALSE);
        } catch (RuntimeException e) {
            log.error("Create WebVisit template failed with error: {}", e.toString());
            String stacktrace = ExceptionUtils.getStackTrace(e);
            log.error("Stack trace is: {}", stacktrace);
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/setup/webvisit2")
    @ResponseBody
    @ApiOperation(value = "Create a WebVisit template with IW 2.0")
    public ResponseDocument<Boolean> createWebVisitTemplate2(
            @PathVariable String customerSpace,
            @RequestBody List<SimpleTemplateMetadata> simpleTemplateMetadataList,
            @RequestParam(value = "enableGA", required = false, defaultValue = "false") boolean enableGA) {
        if (!dataFeedTaskTemplateService.validateGAEnabled(customerSpace, enableGA)) {
            return ResponseDocument.failedResponse(new IllegalStateException("EntityMatchGATenant doesn't support to " +
                    "create WebVisit template."));
        }
        try {
            if (CollectionUtils.isNotEmpty(simpleTemplateMetadataList)) {
                Boolean result = Boolean.TRUE;
                for (SimpleTemplateMetadata simpleTemplateMetadata : simpleTemplateMetadataList) {
                    result = result && dataFeedTaskTemplateService.setupWebVisitProfile2(customerSpace,
                            simpleTemplateMetadata);
                }
                return ResponseDocument.successResponse(result);
            }
            return ResponseDocument.successResponse(Boolean.FALSE);
        } catch (RuntimeException e) {
            log.error("Create WebVisit 2.0 template failed with error: {}", e.toString());
            String stacktrace = ExceptionUtils.getStackTrace(e);
            log.error("Stack trace is: {}", stacktrace);
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/backup/{uniqueTaskId}")
    @ResponseBody
    @ApiOperation(value = "Back up template to S3")
    public ResponseDocument<String> backupTemplate(@PathVariable String customerSpace,
                                                   @PathVariable String uniqueTaskId) {
        try {
            return ResponseDocument.successResponse(
                    dataFeedTaskTemplateService.backupTemplate(customerSpace, uniqueTaskId));
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/restore/{uniqueTaskId}")
    @ResponseBody
    @ApiOperation(value = "Read table from backup file")
    public ResponseDocument<Table> restoreTemplate(@PathVariable String customerSpace,
                                  @PathVariable String uniqueTaskId,
                                  @RequestParam(value = "onlyGetTable", required = false, defaultValue = "true") boolean onlyGetTable,
                                  @RequestBody String backupName) {
        try {
            return ResponseDocument.successResponse(
                    dataFeedTaskTemplateService.restoreTemplate(customerSpace, uniqueTaskId, backupName, onlyGetTable));
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/diagnostic/{taskIdentifier}")
    @ResponseBody
    @ApiOperation(value = "Template diagnostic")
    public ResponseDocument<ImportTemplateDiagnostic> templateDiagnostic(@PathVariable String customerSpace,
                                                                         @PathVariable String taskIdentifier) {
        return ResponseDocument.successResponse(dataFeedTaskManagerService.diagnostic(customerSpace, taskIdentifier));
    }

    @PostMapping(value = "/setup/defaultOpportunity")
    @ResponseBody
    @ApiOperation(value = "Create a default opportunity template")
    public ResponseDocument<Boolean> createDefaultOpportunityTemplate(@PathVariable String customerSpace,
                                                                      @RequestParam(value = "systemName") String systemName,
                                                                      @RequestParam(value = "enableGA", required = false, defaultValue = "false") boolean enableGA) {
        if (!dataFeedTaskTemplateService.validateGAEnabled(customerSpace, enableGA)) {
            return ResponseDocument.failedResponse(new IllegalStateException("EntityMatchGATenant doesn't support to " +
                    "create Opportunity template."));
        }
        log.info("systemName = {}.", systemName);
        if (StringUtils.isEmpty(systemName)) {
            return ResponseDocument.failedResponse(new IllegalArgumentException("systemName cannot be null."));
        }
        if (!dataFeedTaskTemplateService.validationOpportunity(customerSpace, systemName, EntityType.Opportunity)) {
            return ResponseDocument.failedResponse(new IllegalStateException("Opportunities by stage cannot be " +
                    "created as the corresponding system Account object does not have a Unique ID"));
        }
        try {
            Boolean result = dataFeedTaskTemplateService.createDefaultOpportunityTemplate(customerSpace, systemName);
            return ResponseDocument.successResponse(result);
        } catch (Exception e) {
            log.error("Create Default Opportunity template failed with error: {}", e.toString());
            String stacktrace = ExceptionUtils.getStackTrace(e);
            log.error("Stack trace is: {}", stacktrace);
            return ResponseDocument.failedResponse(e);
        }
    }


    @PostMapping(value = "/setup/opportunity")
    @ResponseBody
    @ApiOperation(value = "Create a opportunity template")
    public ResponseDocument<Boolean> createOpportunityTemplate(@PathVariable String customerSpace,
                                                               @RequestParam(value = "systemName") String systemName,
                                                               @RequestBody(required = false) SimpleTemplateMetadata simpleTemplateMetadata,
                                                               @RequestParam(value = "enableGA", required =
                                                                       false, defaultValue = "false") boolean enableGA) {
        if (!dataFeedTaskTemplateService.validateGAEnabled(customerSpace, enableGA)) {
            return ResponseDocument.failedResponse(new IllegalStateException("EntityMatchGATenant doesn't support to " +
                    "create Opportunity template."));
        }
        log.info("systemName = {}.", systemName);
        if (StringUtils.isEmpty(systemName)) {
            return ResponseDocument.failedResponse(new IllegalArgumentException("systemName cannot be null."));
        }
        Preconditions.checkNotNull(simpleTemplateMetadata);
        if (!dataFeedTaskTemplateService.validationOpportunity(customerSpace, systemName, simpleTemplateMetadata.getEntityType())) {
            return ResponseDocument.failedResponse(new IllegalStateException("Opportunities by stage cannot be " +
                    "created as the corresponding Account object does not have a Unique ID"));
        }
        try {
            return ResponseDocument.successResponse(dataFeedTaskTemplateService.createOpportunityTemplate(customerSpace, systemName,
                    simpleTemplateMetadata.getEntityType(), simpleTemplateMetadata));
        } catch (Exception e) {
            log.error("Create Opportunity template failed with error: {}", e.toString());
            String stacktrace = ExceptionUtils.getStackTrace(e);
            log.error("Stack trace is: {}", stacktrace);
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/setup/defaultMarketing")
    @ResponseBody
    @ApiOperation(value = "Create a default marketing template")
    public ResponseDocument<Boolean> createDefaultMarketingTemplate(@PathVariable String customerSpace,
                                                                    @RequestParam(value = "systemName") String systemName,
                                                                    @RequestParam(value = "systemType") String systemType,
                                                                    @RequestParam(value = "enableGA", required = false, defaultValue = "false") boolean enableGA) {
        if (!dataFeedTaskTemplateService.validateGAEnabled(customerSpace, enableGA)) {
            return ResponseDocument.failedResponse(new IllegalStateException("EntityMatchGATenant doesn't support to " +
                    "create Marketing template."));
        }
        log.info("systemName = {}.", systemName);
        if (StringUtils.isEmpty(systemName)) {
            return ResponseDocument.failedResponse(new IllegalArgumentException("systemName cannot be null."));
        }
        if (!dataFeedTaskTemplateService.validationMarketing(customerSpace, systemName, systemType,
                EntityType.MarketingActivity)) {
            return ResponseDocument.failedResponse(new IllegalStateException("Marketing by ActivityType cannot be " +
                    "created as the corresponding system Contact object does not have a Unique ID"));
        }
        try {
            Boolean result = dataFeedTaskTemplateService.createDefaultMarketingTemplate(customerSpace, systemName, systemType);
            return ResponseDocument.successResponse(result);
        } catch (Exception e) {
            log.error("Create Default Marketing template failed with error: {}", e.toString());
            String stacktrace = ExceptionUtils.getStackTrace(e);
            log.error("Stack trace is: {}", stacktrace);
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/setup/marketing")
    @ResponseBody
    @ApiOperation(value = "Create a marketing template")
    public ResponseDocument<Boolean> createMarketingTemplate(@PathVariable String customerSpace,
                                                             @RequestParam(value = "systemName") String systemName,
                                                             @RequestParam(value = "systemType") String systemType,
                                                             @RequestBody(required = false) SimpleTemplateMetadata simpleTemplateMetadata,
                                                             @RequestParam(value = "enableGA", required = false, defaultValue = "false") boolean enableGA) {
        if (!dataFeedTaskTemplateService.validateGAEnabled(customerSpace, enableGA)) {
            return ResponseDocument.failedResponse(new IllegalStateException("EntityMatchGATenant doesn't support to " +
                    "create Marketing template."));
        }
        log.info("systemName = {}.", systemName);
        if (StringUtils.isEmpty(systemName)) {
            return ResponseDocument.failedResponse(new IllegalArgumentException("systemName cannot be null."));
        }
        Preconditions.checkNotNull(simpleTemplateMetadata);
        if (!dataFeedTaskTemplateService.validationMarketing(customerSpace, systemName, systemType,
                simpleTemplateMetadata.getEntityType())) {
            return ResponseDocument.failedResponse(new IllegalStateException("Marketing by ActivityType cannot be " +
                    "created as the corresponding Contact object does not have a Unique ID"));
        }
        try {
            return ResponseDocument.successResponse(dataFeedTaskTemplateService.createMarketingTemplate(customerSpace, systemName,
                     systemType, simpleTemplateMetadata.getEntityType(), simpleTemplateMetadata));
        } catch (Exception e) {
            log.error("Create Marketing template failed with error: {}", e.toString());
            String stacktrace = ExceptionUtils.getStackTrace(e);
            log.error("Stack trace is: {}", stacktrace);
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/setup/defaultDnbIntentData")
    @ResponseBody
    @ApiOperation(value = "Create a default DnbIntentData template")
    public ResponseDocument<Boolean> createDefaultDnbIntentDataTemplate(@PathVariable String customerSpace,
                                                                      @RequestParam(value = "enableGA", required = false, defaultValue = "false") boolean enableGA) {
        if (!dataFeedTaskTemplateService.validateGAEnabled(customerSpace, enableGA)) {
            return ResponseDocument.failedResponse(new IllegalStateException("EntityMatchGATenant doesn't support to " +
                    "create DnbIntentData template."));
        }
        try {
            Boolean result = dataFeedTaskTemplateService.createDefaultDnbIntentDataTemplate(customerSpace);
            return ResponseDocument.successResponse(result);
        } catch (Exception e) {
            log.error("Create Default DnbIntentData template failed with error: {}", e.toString());
            String stacktrace = ExceptionUtils.getStackTrace(e);
            log.error("Stack trace is: {}", stacktrace);
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/setup/dnbIntentData")
    @ResponseBody
    @ApiOperation(value = "Create a DnbIntentData template")
    public ResponseDocument<Boolean> createDnbIntentDataTemplate(@PathVariable String customerSpace,
                                                               @RequestBody(required = false) SimpleTemplateMetadata simpleTemplateMetadata,
                                                               @RequestParam(value = "enableGA", required =
                                                                       false, defaultValue = "false") boolean enableGA) {
        if (!dataFeedTaskTemplateService.validateGAEnabled(customerSpace, enableGA)) {
            return ResponseDocument.failedResponse(new IllegalStateException("EntityMatchGATenant doesn't support to " +
                    "create DnbIntentData template."));
        }
        Preconditions.checkNotNull(simpleTemplateMetadata);
        try {
            return ResponseDocument.successResponse(dataFeedTaskTemplateService.createDnbIntentDataTemplate(customerSpace,
                    simpleTemplateMetadata.getEntityType(), simpleTemplateMetadata));
        } catch (Exception e) {
            log.error("Create DnbIntentData template failed with error: {}", e.toString());
            String stacktrace = ExceptionUtils.getStackTrace(e);
            log.error("Stack trace is: {}", stacktrace);
            return ResponseDocument.failedResponse(e);
        }
    }
}
