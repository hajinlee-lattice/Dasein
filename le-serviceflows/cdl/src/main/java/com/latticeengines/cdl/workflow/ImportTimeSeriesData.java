package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("importTimeSeriesData")
public class ImportTimeSeriesData extends BaseWorkflowStep<ImportTimeSeriesConfiguration> {
    
    @Autowired
    private ImportListOfEntities importListOfEntities;

    @Override
    public void execute() {
        importListOfEntities.setConfiguration(configuration);
        importListOfEntities.setExecutionContext(executionContext);
        importListOfEntities.execute();
    }

}
