package com.latticeengines.serviceflows.workflow.importdata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

@Component("importData")
public class ImportData extends BaseWorkflowStep<ImportStepConfiguration> {

    private static final Log log = LogFactory.getLog(ImportData.class);

    @Override
    public void execute() {
        log.info("Inside ImportData execute()");
        importData();
    }

    private void importData() {
        ImportConfiguration importConfig = setupImportConfig();
        String url = configuration.getMicroServiceHostPort() + "/eai/importjobs";
        AppSubmission submission = restTemplate.postForObject(url, importConfig, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString(), configuration.getMicroServiceHostPort());
    }

    private ImportConfiguration setupImportConfig() {
        ImportConfiguration importConfig = new ImportConfiguration();
        SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
        sourceImportConfig.setSourceType(configuration.getSourceType());

        importConfig.addSourceConfiguration(sourceImportConfig);
        importConfig.setCustomerSpace(configuration.getCustomerSpace());

        if (sourceImportConfig.getSourceType() == SourceType.FILE) {
            SourceFile sourceFile = retrieveSourceFile();
            importConfig.setProperty(ImportProperty.HDFSFILE, sourceFile.getPath());
            importConfig.setProperty(ImportProperty.METADATA, JsonUtils.serialize(retrieveMetadata(sourceFile)));
        }
        return importConfig;
    }

    private SourceFile retrieveSourceFile() {
        CustomerSpace space = getConfiguration().getCustomerSpace();

        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy( //
                getConfiguration().getMicroServiceHostPort());
        return proxy.findSourceFileByName(getConfiguration().getSourceFileName(), space.toString());
    }

    private ModelingMetadata retrieveMetadata(SourceFile sourceFile) {
        if (sourceFile.getTable() == null) {
            throw new RuntimeException(String.format("No metadata has been associated with source file %s",
                    sourceFile.getName()));
        }
        return sourceFile.getTable().getModelingMetadata();
    }
}
