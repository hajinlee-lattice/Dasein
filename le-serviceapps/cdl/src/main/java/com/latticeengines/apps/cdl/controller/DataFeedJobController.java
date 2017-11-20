package com.latticeengines.apps.cdl.controller;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeedjob", description = "Controller of data feed job operations.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection/datafeedjob")
public class DataFeedJobController {

    @Autowired
    private CDLJobService cdlJobService;

    @RequestMapping(value = "/createconsolidatejob", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke data feed consolidate workflow. Returns the job id.")
    public ResponseDocument<String> createConsolidateJob(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        ApplicationId id = cdlJobService.createConsolidateJob(customerSpace);
        return ResponseDocument.successResponse(id.toString());
    }

    @RequestMapping(value = "/createprofilejob", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a CDLJobDetail")
    public ResponseDocument<String> createProfileJob(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        ApplicationId id = cdlJobService.createProfileJob(customerSpace);
        return ResponseDocument.successResponse(id.toString());
    }
}
