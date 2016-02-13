package com.latticeengines.pls.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.leadprioritization.workflow.ImportEventTableWorkflowConfiguration;

@Component
public class ImportEventTableWorkflowSubmitter extends WorkflowSubmitter {
    public ApplicationId submit(SourceFile sourceFile) {
        ImportEventTableWorkflowConfiguration configuration = new ImportEventTableWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .sourceFileName(sourceFile.getName()) //
                .sourceType(SourceType.FILE) //
                .internalResourceHostPort(internalResourceHostPort) //
                .reportName(sourceFile.getName() + "_Report") //
                .build();
        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
        return YarnUtils.appIdFromString(submission.getApplicationIds().get(0));
    }
}
