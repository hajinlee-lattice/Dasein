package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttrConfigStateOverview;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.proxy.exposed.cdl.CDLJobProxy;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.db.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdl consolidate and profile", description = "REST resource for cdl")
@RestController
@RequestMapping("/cdl")
@PreAuthorize("hasRole('Edit_PLS_CDL_Data')")
public class CDLResource {

    private static final Logger log = LoggerFactory.getLogger(CDLResource.class);

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
        try {
            ApplicationId result = cdlService.processAnalyze(customerSpace.toString(), request);
            return ResponseDocument.successResponse(result.toString());
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit processAnalyze job: %s", e.getMessage()));
            return ResponseDocument.failedResponse(
                    new LedpException(LedpCode.LEDP_18182, new String[] {"ProcessAnalyze", e.getMessage()}));
        }
    }

    @RequestMapping(value = "/import/csv", method = RequestMethod.POST)
    @ApiOperation(value = "Start import job")
    public ResponseDocument<String> startImportCSV(@RequestParam(value = "templateFileName") String templateFileName,
                                                   @RequestParam(value = "dataFileName") String dataFileName,
                                                   @RequestParam(value = "source") String source, //
                                                   @RequestParam(value = "entity") String entity, //
                                                   @RequestParam(value = "feedType") String feedType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        try {
            ApplicationId applicationId = cdlService.submitCSVImport(customerSpace.toString(), templateFileName, dataFileName, source,
                    entity, feedType);
            return ResponseDocument.successResponse(applicationId.toString());
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit import job: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[] {"ImportFile", e.getMessage()});
        }
    }

    @RequestMapping(value = "/cleanupbyupload", method = RequestMethod.POST)
    @ApiOperation(value = "Start cleanup job")
    public ModelAndView cleanup(@RequestParam(value = "fileName") String fileName,
                                            @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation,
                                            @RequestParam(value = "cleanupOperationType") CleanupOperationType type) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        UIAction uiAction = cdlService.cleanup(customerSpace.toString(), fileName, schemaInterpretation, type);
        return new ModelAndView(jsonView, ImmutableMap.of(UIAction.class.getSimpleName(), uiAction));
    }

    @RequestMapping(value = "/cleanupbyrange", method = RequestMethod.POST)
    @ApiOperation(value = "Start cleanup job")
    public ResponseDocument<String> cleanupByRange(@RequestParam(value = "startTime") String startTime,
                                            @RequestParam(value = "endTime") String endTime,
                                            @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        try {
            ApplicationId applicationId = cdlService.cleanupByTimeRange(customerSpace.toString(), startTime, endTime, schemaInterpretation);
            return ResponseDocument.successResponse(applicationId.toString());
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit cleanup by range job: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[] {"Cleanup", e.getMessage()});
        }
    }

    @RequestMapping(value = "/cleanupall", method = RequestMethod.POST)
    @ApiOperation(value = "Start cleanup job")
    public ResponseDocument<String> cleanupAll(@RequestParam(value = "schema") SchemaInterpretation schemaInterpretation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        try {
            ApplicationId applicationId = cdlService.cleanupAllData(customerSpace.toString(), schemaInterpretation);
            return ResponseDocument.successResponse(applicationId.toString());
        } catch (RuntimeException e) {
            log.error(String.format("Failed to submit cleanup all job: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18182, new String[] {"Cleanup", e.getMessage()});
        }
    }

    @GetMapping(value = "/s3import/template")
    @ResponseBody
    @ApiOperation("get template table fields")
    public List<S3ImportTemplateDisplay> getActivationOverview() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return cdlService.getS3ImportTemplate(customerSpace.toString());
    }
}
