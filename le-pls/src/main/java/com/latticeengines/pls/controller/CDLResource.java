package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.proxy.exposed.cdl.CDLJobProxy;
import com.latticeengines.db.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdl consolidate and profile", description = "REST resource for cdl")
@RestController
@RequestMapping("/cdl")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class CDLResource {

    @Inject
    private CDLJobProxy cdlJobProxy;

    @Inject
    private CDLService cdlService;

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
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (request == null) {
            request = new ProcessAnalyzeRequest();
        }
        request.setUserId(MultiTenantContext.getEmailAddress());
        ApplicationId result = cdlService.processAnalyze(customerSpace.toString(), request);
        return ResponseDocument.successResponse(result.toString());
    }

    @RequestMapping(value = "/import/csv", method = RequestMethod.POST)
    @ApiOperation(value = "Start import job")
    public ResponseDocument<String> startImportCSV(@RequestParam(value = "templateFileName") String templateFileName,
                                                   @RequestParam(value = "dataFileName") String dataFileName,
                                                   @RequestParam(value = "source") String source, //
                                                   @RequestParam(value = "entity") String entity, //
                                                   @RequestParam(value = "feedType") String feedType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace.toString(), templateFileName, dataFileName, source,
                entity, feedType);
        return ResponseDocument.successResponse(applicationId.toString());
    }

    @RequestMapping(value = "/cleanupbyupload", method = RequestMethod.POST)
    @ApiOperation(value = "Start cleanup job")
    public ResponseDocument<String> cleanup(@RequestParam(value = "fileName") String fileName,
                                            @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation,
                                            @RequestParam(value = "cleanupOperationType") CleanupOperationType type) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        ApplicationId applicationId = cdlService.cleanup(customerSpace.toString(), fileName, schemaInterpretation, type);
        return ResponseDocument.successResponse(applicationId.toString());
    }

    @RequestMapping(value = "/cleanupbyrange", method = RequestMethod.POST)
    @ApiOperation(value = "Start cleanup job")
    public ResponseDocument<String> cleanupByRange(@RequestParam(value = "startTime") String startTime,
                                            @RequestParam(value = "endTime") String endTime,
                                            @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        ApplicationId applicationId = cdlService.cleanupByTimeRange(customerSpace.toString(), startTime, endTime, schemaInterpretation);
        return ResponseDocument.successResponse(applicationId.toString());
    }

    @RequestMapping(value = "/cleanupall", method = RequestMethod.POST)
    @ApiOperation(value = "Start cleanup job")
    public ResponseDocument<String> cleanupAll(@RequestParam(value = "schema") SchemaInterpretation schemaInterpretation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        ApplicationId applicationId = cdlService.cleanupAllData(customerSpace.toString(), schemaInterpretation);
        return ResponseDocument.successResponse(applicationId.toString());
    }
}
