package com.latticeengines.dataflowapi.controller;

import java.util.Collections;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
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

    @PostMapping
    @ResponseBody
    @ApiOperation(value = "Create a data flow submission")
    public AppSubmission submitDataFlowExecution(@RequestBody DataFlowConfiguration dataFlowConfig) {
        return new AppSubmission(Collections.singletonList(dataFlowService.submitDataFlow(dataFlowConfig)));
    }
}
