package com.latticeengines.eai.controller;

import java.util.Arrays;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.eai.exposed.service.EaiService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "importjobs", description = "REST resource for importing data into Lattice")
@RestController
@RequestMapping("/importjobs")
public class EaiResource {

    @Autowired
    private EaiService eaiService;

    @Autowired
    private JobService jobService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create an import data job")
    public AppSubmission createImportDataJob(@RequestBody ImportConfiguration importConfig) {
        return new AppSubmission(Arrays.<ApplicationId> asList(eaiService.extractAndImport(importConfig)));
    }

    @RequestMapping(value = "/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status for submitted import job")
    public JobStatus getImportDataJobStatus(@PathVariable String applicationId) {
        return jobService.getJobStatus(applicationId);
    }
}
