package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.workflow.PublishTableRoleToESWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.PublishTableRoleWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.PublishTableToElasticSearchWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.PublishVIDataWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.PublishTableRoleRequest;
import com.latticeengines.domain.exposed.cdl.PublishVIDataRequest;
import com.latticeengines.domain.exposed.elasticsearch.PublishESRequest;
import com.latticeengines.domain.exposed.elasticsearch.PublishTableToESRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import joptsimple.internal.Strings;

@Api(value = "publish")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/publish")
public class PublishTableResource {

    private static Logger log = LoggerFactory.getLogger(PublishTableResource.class);

    @Inject
    private PublishTableRoleWorkflowSubmitter workflowSubmitter;
    @Inject
    private PublishVIDataWorkflowSubmitter publishVIDataWorkflowSubmitter;
    @Inject
    private PublishTableRoleToESWorkflowSubmitter esWorkflowSubmitter;

    @Inject
    private PublishTableToElasticSearchWorkflowSubmitter publishTableToElasticSearchWorkflowSubmitter;

    @PostMapping("/dynamo")
    @ApiOperation(value = "publish dynamo table")
    public ResponseDocument<String> publishDynamo(@PathVariable String customerSpace, //
                                                  @RequestBody PublishTableRoleRequest request) {
        try {
            ApplicationId appId = workflowSubmitter.submitPublishDynamo(customerSpace, //
                    request.getTableRoles(), request.getVersion(),
                    new WorkflowPidWrapper(-1L));
            return ResponseDocument.successResponse(appId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping("/es")
    @ApiOperation(value = "publish elasticsearch table")
    public ResponseDocument<String> publishES(@PathVariable String customerSpace, //
                                                  @RequestBody PublishESRequest request) {
        try {
            ApplicationId appId = esWorkflowSubmitter.submitPublishES(customerSpace, //
                    request, new WorkflowPidWrapper(-1L));
            return ResponseDocument.successResponse(appId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping("/es/tables")
    @ApiOperation(value = "publish table to elastic search")
    public String publishTableToES(@PathVariable String customerSpace, //
                                   @RequestBody PublishTableToESRequest request) {
        try {
            ApplicationId appId = publishTableToElasticSearchWorkflowSubmitter.submit(customerSpace, //
                    request, new WorkflowPidWrapper(-1L));
            return appId.toString();
        } catch (RuntimeException e) {
            log.error(e.toString());
            return Strings.EMPTY;
        }
    }

    @PostMapping("/vidata")
    @ApiOperation(value = "publish VIData to elasticsearch")
    public ResponseDocument<String> publishVIData(@PathVariable String customerSpace, //
                                                  @RequestBody(required = false) PublishVIDataRequest request) {
        if (request == null) {
            request = getDefaultPublishViDataRequest();
        }
        try {
            ApplicationId appId = publishVIDataWorkflowSubmitter.submitPublishViData(customerSpace, //
                     request.getVersion(), new WorkflowPidWrapper(-1L));
            return ResponseDocument.successResponse(appId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    private PublishVIDataRequest getDefaultPublishViDataRequest() {
        return new PublishVIDataRequest();
    }

}
