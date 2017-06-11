package com.latticeengines.cdl.workflow.steps.importdata;

import java.util.Map;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportListOfEntitiesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportSourceDataConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("importListOfEntities")
public class ImportListOfEntities extends BaseWorkflowStep<ImportListOfEntitiesConfiguration> {
    
    @Autowired
    private ImportSourceData importSourceData;

    @Override
    public void execute() {
        Map<String, ImportSourceDataConfiguration> configs = configuration.getImportConfigs();
        
        importSourceData.setExecutionContext(executionContext);
        for (Map.Entry<String, ImportSourceDataConfiguration> entry : configs.entrySet()) {
            importSourceData.setConfiguration(entry.getValue());
            importSourceData.execute();
        }
    }

}
