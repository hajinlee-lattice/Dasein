package com.latticeengines.apps.cdl.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "jobs", description = "REST resource for CDL jobs")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/jobs")
public class JobResource {

    @Autowired
    private JobProxy jobProxy;

    @RequestMapping(value = "/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted job")
    public JobStatus getJobStatus(@PathVariable String customerSpace, @PathVariable String applicationId) {
        //todo: use customerSpace to filter(waiting for workflow api support);
        return jobProxy.getJobStatus(applicationId);
    }
}
