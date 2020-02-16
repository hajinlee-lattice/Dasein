package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.workflow.MigrateDynamoWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MigrateDynamoRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "migratetable")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/migratetable")
public class MigrateTableResource {

    @Inject
    private MigrateDynamoWorkflowSubmitter migrateDynamoWorkflowSubmitter;

    @PostMapping(value = "/dynamo")
    @ApiOperation(value = "migrate dynamo table")
    @SuppressWarnings("unchecked")
    public ResponseDocument<String> migrateDynamo(@PathVariable String customerSpace, @RequestBody MigrateDynamoRequest migrateDynamoRequest) {
        try {
            ApplicationId appId;
            appId = migrateDynamoWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), new WorkflowPidWrapper(-1L), migrateDynamoRequest);
            return ResponseDocument.successResponse(appId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

}
