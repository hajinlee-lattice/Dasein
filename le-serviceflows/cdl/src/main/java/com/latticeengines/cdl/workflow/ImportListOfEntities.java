package com.latticeengines.cdl.workflow;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;

@Component("importListOfEntities")
public class ImportListOfEntities extends BaseWorkflowStep<ImportListOfEntitiesConfiguration> {
    
    @Autowired
    private ImportData importData;

    @Override
    public void execute() {
        Map<String, ImportStepConfiguration> configs = configuration.getImportConfigs();
        
        importData.setExecutionContext(executionContext);
        for (Map.Entry<String, ImportStepConfiguration> entry : configs.entrySet()) {
            importData.setConfiguration(entry.getValue());
            importData.execute();
        }
    }

}
