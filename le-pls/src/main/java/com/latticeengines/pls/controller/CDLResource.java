package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.proxy.exposed.cdl.CDLJobProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

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
    private CDLProxy cdlProxy;

    @RequestMapping(value = "/consolidateAndProfile", method = RequestMethod.POST)
    @ApiOperation(value = "Start Consolidate And Profile job")
    public ResponseDocument<String> startConsolidateAndProfileJob() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        ApplicationId result = cdlJobProxy.createConsolidateJob(customerSpace.toString());
        return ResponseDocument.successResponse(result.toString());
    }

    @RequestMapping(value = "/processanalyze", method = RequestMethod.POST)
    @ApiOperation(value = "Start Process And Analyze job")
    public ResponseDocument<String> processAnalyze() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        ApplicationId result = cdlProxy.processAnalyze(customerSpace.toString(), null);
        return ResponseDocument.successResponse(result.toString());
    }
}
