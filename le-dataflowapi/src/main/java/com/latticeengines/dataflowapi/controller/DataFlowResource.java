package com.latticeengines.dataflowapi.controller;

import java.util.Arrays;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dataflowapi.service.DataFlowService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.network.exposed.dataflowapi.DataFlowInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dataflowapi", description = "REST resource for transformations")
@RestController
@RequestMapping("/dataflows")
public class DataFlowResource implements DataFlowInterface {

    @Inject
    private DataFlowService dataFlowService;

    @RequestMapping(value = "/", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data flow submission")
    public AppSubmission submitDataFlowExecution(@RequestBody DataFlowConfiguration dataFlowConfig) {
        return new AppSubmission(Arrays.<ApplicationId> asList(new ApplicationId[] { dataFlowService
                .submitDataFlow(dataFlowConfig) }));
    }
}
