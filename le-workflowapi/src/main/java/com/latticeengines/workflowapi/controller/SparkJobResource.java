package com.latticeengines.workflowapi.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.RunSparkWorkflowRequest;
import com.latticeengines.workflowapi.service.SparkWorkflowService;

import io.swagger.annotations.Api;

@Api(value = "Spark Job")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/spark-jobs")
public class SparkJobResource {

    @Inject
    private SparkWorkflowService sparkWorkflowService;

    @PostMapping
    @ResponseBody
    public AppSubmission submitSparkJob(@PathVariable String customerSpace, @RequestBody RunSparkWorkflowRequest request) {
        return sparkWorkflowService.submitSparkJob(customerSpace, request);
    }

}
