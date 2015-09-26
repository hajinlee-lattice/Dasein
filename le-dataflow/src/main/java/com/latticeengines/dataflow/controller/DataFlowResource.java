package com.latticeengines.dataflow.controller;

import java.util.Arrays;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dataflow.service.DataFlowService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "dataflow", description = "REST resource for transformations")
@RestController
@RequestMapping("/dataflows")
public class DataFlowResource {
    
    @Autowired
    private DataFlowService dataFlowService;

    @RequestMapping(value = "/", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data flow submission")
    public AppSubmission submitDataFlowExecution(@RequestBody DataFlowConfiguration dataFlowConfig) {
        return new AppSubmission(Arrays.<ApplicationId>asList(new ApplicationId[] { dataFlowService.submitDataFlow(dataFlowConfig) }));
    }
}
