package com.latticeengines.dataflow.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "dataflow", description = "REST resource for transformations")
@RestController
@RequestMapping("/tenants/{tenantId}")
public class DataFlowResource {
    
    @Autowired
    private DataTransformationService dataTransformationService;

    @RequestMapping(value = "/dataflows/{dataFlowBeanName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data flow submission")
    public AppSubmission submitDataFlowExecution(@PathVariable String dataFlowBeanName) {
        return null;
    }
}
