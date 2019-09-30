package com.latticeengines.apps.cdl.controller;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.apps.cdl.util.EntityExportUtils;
import com.latticeengines.apps.cdl.util.PAValidationUtils;
import com.latticeengines.apps.cdl.workflow.CDLEntityMatchMigrationWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.ConvertBatchStoreToImportWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.EntityExportWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.OrphanRecordsExportWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.ProcessAnalyzeWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreToImportRequest;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.AtlasExportType;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "datafeeds", description = "Controller of data feed operations.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection/datafeed")
public class DataFeedController {

    private static final Logger log = LoggerFactory.getLogger(DataFeedController.class);

    private final ProcessAnalyzeWorkflowSubmitter processAnalyzeWorkflowSubmitter;
    private final OrphanRecordsExportWorkflowSubmitter orphanRecordExportWorkflowSubmitter;
    private final EntityExportWorkflowSubmitter entityExportWorkflowSubmitter;
    private final ConvertBatchStoreToImportWorkflowSubmitter convertBatchStoreToImportWorkflowSubmitter;
    private final CDLEntityMatchMigrationWorkflowSubmitter cdlEntityMatchMigrationWorkflowSubmitter;
    private final DataFeedService dataFeedService;
    private final PAValidationUtils paValidationUtils;
    private final AtlasExportService atlasExportService;
    private final ServingStoreService servingStoreService;

    @Value("${cdl.processAnalyze.retry.expired.time}")
    private long retryExpiredTime;

    @Inject
    public DataFeedController(ProcessAnalyzeWorkflowSubmitter processAnalyzeWorkflowSubmitter,
                              OrphanRecordsExportWorkflowSubmitter orphanRecordExportWorkflowSubmitter,
                              EntityExportWorkflowSubmitter entityExportWorkflowSubmitter,
                              ConvertBatchStoreToImportWorkflowSubmitter convertBatchStoreToImportWorkflowSubmitter,
                              CDLEntityMatchMigrationWorkflowSubmitter cdlEntityMatchMigrationWorkflowSubmitter,
                              DataFeedService dataFeedService, PAValidationUtils paValidationUtils,
                              AtlasExportService atlasExportService, ServingStoreService servingStoreService) {
        this.processAnalyzeWorkflowSubmitter = processAnalyzeWorkflowSubmitter;
        this.orphanRecordExportWorkflowSubmitter = orphanRecordExportWorkflowSubmitter;
        this.entityExportWorkflowSubmitter = entityExportWorkflowSubmitter;
        this.convertBatchStoreToImportWorkflowSubmitter = convertBatchStoreToImportWorkflowSubmitter;
        this.cdlEntityMatchMigrationWorkflowSubmitter = cdlEntityMatchMigrationWorkflowSubmitter;
        this.dataFeedService = dataFeedService;
        this.paValidationUtils = paValidationUtils;
        this.atlasExportService = atlasExportService;
        this.servingStoreService = servingStoreService;
    }

    @PostMapping(value = "/processanalyze", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke profile workflow. Returns the job id.")
    public ResponseDocument<String> processAnalyze(@PathVariable String customerSpace,
                                                   @RequestParam(value = "runNow", required = false, defaultValue = "false") boolean runNow,
                                                   @RequestBody(required = false) ProcessAnalyzeRequest request) {
        customerSpace = MultiTenantContext.getCustomerSpace().toString();
        if (request == null) {
            request = defaultProcessAnalyzeRequest();
        }
        try {
            if (runNow) {
                ApplicationId appId = processAnalyzeWorkflowSubmitter.submit(customerSpace, request,
                        new WorkflowPidWrapper(-1L));
                return ResponseDocument.successResponse(appId.toString());
            } else {
                DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
                if (dataFeed != null && !dataFeed.isScheduleNow()) {
                    paValidationUtils.checkStartPAValidations(customerSpace, dataFeed);
                    dataFeedService.updateDataFeedScheduleTime(customerSpace, true, request);
                }
                return ResponseDocument.successResponse("");
            }
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/processanalyze/restart", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Restart a previous failed processanalyze execution")
    public ResponseDocument<String> restart(@PathVariable String customerSpace,
                                            @ApiParam(value = "Memory in MB", required = false)
                                            @RequestParam(value = "memory", required = false) Integer memory,
                                            @RequestParam(value = "autoRetry", required = false, defaultValue = "false") Boolean autoRetry,
                                            @RequestParam(value = "skipMigrationCheck", required = false, defaultValue = "false") Boolean skipMigrationTrack) {
        customerSpace = MultiTenantContext.getCustomerSpace().toString();
        paValidationUtils.checkRetry(customerSpace);
        ApplicationId appId = processAnalyzeWorkflowSubmitter.retryLatestFailed(customerSpace, memory, autoRetry, skipMigrationTrack);
        return ResponseDocument.successResponse(appId.toString());
    }

    @PostMapping(value = "/exportorphanrecords", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke orphanRecordExport workflow. Returns the job id.")
    public ResponseDocument<String> orphanRecordExport(@PathVariable String customerSpace,
                                                       @RequestBody OrphanRecordsExportRequest request) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        try {
            ApplicationId appId = orphanRecordExportWorkflowSubmitter.submit(
                    customerSpace, request, new WorkflowPidWrapper(-1L));
            if (appId == null) {
                return null;
            }
            return ResponseDocument.successResponse(appId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/convertbatchstoretoimport", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke convert batch store to import workflow. Returns the job id.")
    public ResponseDocument<String> convertBatchStoreToImport(@PathVariable String customerSpace,
                                                              @RequestBody ConvertBatchStoreToImportRequest request) {
        try {
            ApplicationId applicationId =
                    convertBatchStoreToImportWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace),
                            request.getUserId(), request.getEntity(), request.getTemplateName(),
                            request.getFeedType(), request.getSubType(), request.getRenameMap(),
                            request.getDuplicateMap(),
                            new WorkflowPidWrapper(-1L));
            if (applicationId == null) {
                return null;
            }
            return ResponseDocument.successResponse(applicationId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/migrateimport", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke convert batch store to import workflow. Returns the job id.")
    public ResponseDocument<String> migrateImport(@PathVariable String customerSpace, @RequestBody String userId) {
        try {
            ApplicationId applicationId =
                    cdlEntityMatchMigrationWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace),
                            userId, new WorkflowPidWrapper(-1L));
            if (applicationId == null) {
                return null;
            }
            return ResponseDocument.successResponse(applicationId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    private ApplicationId submitWorkflow(String customerSpace, EntityExportRequest request, AtlasExportType exportType) {
        AtlasExport atlasExport = atlasExportService.createAtlasExport(customerSpace, exportType);
        request.setAtlasExportId(atlasExport.getUuid());
        ApplicationId appId = entityExportWorkflowSubmitter.submit(customerSpace, request,
                new WorkflowPidWrapper(-1L));
        return appId;
    }

    @PostMapping(value = "/entityexport", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke profile workflow. Returns the job id.")
    public ResponseDocument<String> entityExport(@PathVariable String customerSpace,
                                                 @RequestBody EntityExportRequest request) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        try {
            ApplicationId appId;
            // if export id doesn't have value, we should check attribute count and setup atlas report
            if (StringUtils.isEmpty(request.getAtlasExportId())) {
                request.setSaveToDropfolder(true);
                AtlasExportType exportType = request.getExportType();
                if (exportType != null) {
                    EntityExportUtils.checkExportAttribute(exportType, customerSpace, request.getDataCollectionVersion(), servingStoreService);
                    appId = submitWorkflow(customerSpace, request, exportType);
                    return ResponseDocument.successResponse(appId.toString());
                } else {
                    // empty export type means export both account and contact
                    List<String> responseMessages = new ArrayList();
                    for (AtlasExportType atlasExportType : AtlasExportType.UI_EXPORT_TYPES) {
                        try {
                            EntityExportUtils.checkExportAttribute(atlasExportType, customerSpace, request.getDataCollectionVersion(), servingStoreService);
                        } catch (RuntimeException e) {
                            responseMessages.add(e.getMessage());
                            continue;
                        }
                        appId = submitWorkflow(customerSpace, request, atlasExportType);
                        responseMessages.add(appId.toString());
                    }
                    return ResponseDocument.successResponse(StringUtils.join(responseMessages.toArray(), ","));
                }
            } else {
                // already have atlas report, just submit workflow
                appId = entityExportWorkflowSubmitter.submit(customerSpace, request, new WorkflowPidWrapper(-1L));
                return ResponseDocument.successResponse(appId.toString());
            }
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    private ProcessAnalyzeRequest defaultProcessAnalyzeRequest() {
        return new ProcessAnalyzeRequest();
    }

}
