package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ListSegmentImportRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.ImportListSegmentWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component
public class ImportListSegmentWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(ImportListSegmentWorkflowSubmitter.class);

    @Value("${aws.s3.data.stage.bucket}")
    private String dateStageBucket;

    @Value("${aws.customer.s3.bucket}")
    private String customerBucket;

    private String FILE_KEY = "FILE_KEY";

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull String customerSpace, @NotNull ListSegmentImportRequest request, @NotNull WorkflowPidWrapper pidWrapper) {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "importListSegmentWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.SEGMENT_NAME, request.getSegmentName());
        inputProperties.put(FILE_KEY, request.getS3FileKey());
        ImportListSegmentWorkflowConfiguration configuration = new ImportListSegmentWorkflowConfiguration.Builder()
                .customer(CustomerSpace.parse(customerSpace))
                .sourceBucket(dateStageBucket)
                .sourceKey(request.getS3FileKey())
                .destBucket(customerBucket)
                .segmentName(request.getSegmentName())
                .inputProperties(inputProperties)
                .build();
        ApplicationId applicationId = workflowJobService.submit(configuration, pidWrapper.getPid());
        return applicationId;
    }

}
