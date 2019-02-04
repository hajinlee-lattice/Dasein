package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.workflow.BulkEntityMatchWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.BulkEntityMatchRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "entity match", description = "REST resource for entity match")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/match/entity")
public class EntityMatchResource {

    private static final Logger log = LoggerFactory.getLogger(EntityMatchResource.class);

    @Inject
    private BulkEntityMatchWorkflowSubmitter bulkEntityMatchWorkflowSubmitter;

    @PostMapping(value = "/bulk", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Submit a bulk match job for entity match")
    public ResponseDocument<String> restart(@PathVariable String customerSpace,
            @RequestBody BulkEntityMatchRequest request) {
        try {
            ApplicationId appId = bulkEntityMatchWorkflowSubmitter.submit(customerSpace, request, new WorkflowPidWrapper(-1L));
            return ResponseDocument.successResponse(appId.toString());
        } catch (Exception e) {
            log.error("Failed to submit bulk entity match workflow", e);
            return ResponseDocument.failedResponse(e);
        }
    }

}
