package com.latticeengines.eai.controller;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.network.exposed.eai.EaiInterface;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "eaijobs", description = "REST resource for importing/exporting data into/from Lattice")
@RestController
@RequestMapping("")
public class EaiResource implements EaiInterface {

    @Autowired
    private EaiService eaiService;

    @Autowired
    private JobService jobService;

    @RequestMapping(value = "/importjobs", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create an import data job")
    public AppSubmission createImportDataJob(@RequestBody ImportConfiguration importConfig) {
        return new AppSubmission(Collections.singletonList(eaiService.extractAndImport(importConfig)));
    }

    @RequestMapping(value = "/exportjobs", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create an export data job")
    public AppSubmission createExportDataJob(@RequestBody ExportConfiguration exportConfig) {
        return new AppSubmission(Collections.singletonList(eaiService.exportDataFromHdfs(exportConfig)));
    }
}
