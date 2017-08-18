package com.latticeengines.pls.controller;


import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.pls.service.CDLImportService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdl import csv", description = "REST resource for uploading csv files for modeling")
@RestController
@RequestMapping("/cdl/import")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class CDLImportResource {



    @Autowired
    private CDLImportService cdlImportService;

    @RequestMapping(value = "csv", method = RequestMethod.POST)
    @ApiOperation(value = "Start import job")
    public ResponseDocument<String> startImportCSV(@RequestParam(value = "templateFileName") String templateFileName,
                                                   @RequestParam(value = "dataFileName") String dataFileName,
                                                   @RequestParam(value = "source") String source, //
                                                   @RequestParam(value = "entity") String entity, //
                                                   @RequestParam(value = "feedType") String feedType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        ApplicationId applicationId = cdlImportService.submitCSVImport(customerSpace.toString(), templateFileName, dataFileName, source,
                entity, feedType);
        return ResponseDocument.successResponse(applicationId.toString());
    }
}
