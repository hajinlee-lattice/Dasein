package com.latticeengines.pls.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.leadprioritization.workflow.ImportEventTableWorkflowConfiguration;
import com.latticeengines.workflow.exposed.service.SourceFileService;

@Component
public class ImportEventTableWorkflowSubmitter extends WorkflowSubmitter {
    @Autowired
    private SourceFileService sourceFileService;

    public ApplicationId submit(SourceFile sourceFile) {
        if (hasRunningWorkflow(sourceFile)) {
            throw new LedpException(LedpCode.LEDP_18081, new String[] { sourceFile.getName() });
        }
        ImportEventTableWorkflowConfiguration configuration = new ImportEventTableWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .sourceFileName(sourceFile.getName()) //
                .sourceType(SourceType.FILE) //
                .internalResourceHostPort(internalResourceHostPort) //
                .reportName(sourceFile.getName() + "_Report") //
                .build();
        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
        String applicationId = submission.getApplicationIds().get(0);
        sourceFile.setApplicationId(applicationId);
        sourceFileService.update(sourceFile);
        return YarnUtils.appIdFromString(applicationId);
    }
}
