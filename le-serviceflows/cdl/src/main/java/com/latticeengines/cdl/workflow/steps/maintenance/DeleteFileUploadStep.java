package com.latticeengines.cdl.workflow.steps.maintenance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.DeleteFileToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.DeleteFileUploadStepConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("deleteFileUploadStep")
public class DeleteFileUploadStep extends BaseWorkflowStep<DeleteFileUploadStepConfiguration> {

    @Autowired
    private EaiProxy eaiProxy;

    @Override
    public void execute() {
        //execute import.
    }

    private ImportConfiguration generateImportConfiguration() {
        DeleteFileToHdfsConfiguration importConfig = new DeleteFileToHdfsConfiguration();
//        importConfig.setTableName();
        importConfig.setCustomerSpace(configuration.getCustomerSpace());
        SourceImportConfiguration sourceImportConfiguration = new SourceImportConfiguration();
        sourceImportConfiguration.setSourceType(SourceType.FILE);
        importConfig.addSourceConfiguration(sourceImportConfiguration);
        return importConfig;
    }
}
