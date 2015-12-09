package com.latticeengines.serviceflows.workflow.importdata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("importData")
public class ImportData extends BaseWorkflowStep<ImportStepConfiguration> {

    private static final Log log = LogFactory.getLog(ImportData.class);

    @Override
    public void execute() {
        log.info("Inside ImportData execute()");
        importData();
    }

    private void importData() {
        ImportConfiguration importConfig = setupSalesforceImportConfig();
        String url = configuration.getMicroServiceHostPort() + "/eai/importjobs";
        AppSubmission submission = restTemplate.postForObject(url, importConfig, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString(), configuration.getMicroServiceHostPort());
    }

    private ImportConfiguration setupSalesforceImportConfig() {
        ImportConfiguration importConfig = new ImportConfiguration();
        SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
        sourceImportConfig.setSourceType(configuration.getSourceType());

        importConfig.addSourceConfiguration(sourceImportConfig);
        importConfig.setCustomerSpace(configuration.getCustomerSpace());
        return importConfig;
    }

}
