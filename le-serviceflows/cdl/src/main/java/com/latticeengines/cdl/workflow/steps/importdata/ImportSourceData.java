package com.latticeengines.cdl.workflow.steps.importdata;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportSourceDataConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;

@Component("importSourceData")
public class ImportSourceData extends BaseWorkflowStep<ImportSourceDataConfiguration> {
    
    @Autowired
    private ImportData importData;

    @Override
    public void execute() {
        importData.setExecutionContext(executionContext);
        importData.setConfiguration(configuration);
        importData.execute();
    }
}
