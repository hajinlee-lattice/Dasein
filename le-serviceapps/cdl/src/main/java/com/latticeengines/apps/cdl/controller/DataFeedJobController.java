package com.latticeengines.apps.cdl.controller;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeedjob", description = "Controller of data feed job operations.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection/datafeedjob")
public class DataFeedJobController {

    @PostMapping("/createconsolidatejob")
    @ResponseBody
    @ApiOperation(value = "Invoke data feed consolidate workflow. Returns the job id.")
    public ResponseDocument<String> createConsolidateJob(@PathVariable String customerSpace) {
        return ResponseDocument.successResponse("");
    }

    @PostMapping("/createprofilejob")
    @ResponseBody
    @ApiOperation(value = "Create a CDLJobDetail and launch profile workflow.")
    public ResponseDocument<String> createProfileJob(@PathVariable String customerSpace) {
        return ResponseDocument.successResponse("");
    }
}
