package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.domain.exposed.ResponseDocument;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeedjob", description = "Controller of data feed job operations.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection/datafeedjob")
public class DataFeedJobController {

    @Inject
    private CDLJobService cdlJobService;

    @RequestMapping(value = "/createconsolidatejob", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke data feed consolidate workflow. Returns the job id.")
    public ResponseDocument<String> createConsolidateJob(@PathVariable String customerSpace) {
        return ResponseDocument.successResponse("");
    }

    @RequestMapping(value = "/createprofilejob", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a CDLJobDetail and launch profile workflow.")
    public ResponseDocument<String> createProfileJob(@PathVariable String customerSpace) {
        return ResponseDocument.successResponse("");
    }
}
