package com.latticeengines.workflowapi.steps.prospectdiscovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;

@Component("importData")
public class ImportData extends BaseFitModelStep<BaseFitModelStepConfiguration> {

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
        SourceImportConfiguration salesforceConfig = new SourceImportConfiguration();
        salesforceConfig.setSourceType(SourceType.SALESFORCE);

        importConfig.addSourceConfiguration(salesforceConfig);
        importConfig.setCustomer(configuration.getCustomerSpace());
        return importConfig;
    }

}
