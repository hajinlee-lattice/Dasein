package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;

@Component("importContactData")
public class ImportContactData extends BaseWorkflowStep<ImportContactStepConfiguration> {
    
    @Autowired
    private ImportData importData;

    @Override
    public void execute() {
        if (configuration.getSourceFileName() == null) {
            return;
        }
        importData.setConfiguration(configuration);
        importData.setExecutionContext(executionContext);
        importData.execute();
    }

}
