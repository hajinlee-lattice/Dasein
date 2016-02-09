package com.latticeengines.serviceflows.workflow.importdata;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

@Component("importData")
public class ImportData extends BaseWorkflowStep<ImportStepConfiguration> {

    private static final Log log = LogFactory.getLog(ImportData.class);

    @Autowired
    private MetadataProxy metadataProxy;

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
            Table metadata = retrieveMetadata(sourceFile);
            sourceImportConfig.setProperty(ImportProperty.HDFSFILE, sourceFile.getPath());
            sourceImportConfig.setProperty(ImportProperty.METADATA, //
                    JsonUtils.serialize(metadata.getModelingMetadata()));
            sourceImportConfig.setTables(Arrays.asList(metadata));
        }
        return importConfig;
    }

    private SourceFile retrieveSourceFile() {
        CustomerSpace space = getConfiguration().getCustomerSpace();

        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy( //
                getConfiguration().getInternalResourceHostPort());
        return proxy.findSourceFileByName(getConfiguration().getSourceFileName(), space.toString());
    }

    private Table retrieveMetadata(SourceFile sourceFile) {
        if (sourceFile.getTableName() == null) {
            throw new RuntimeException(String.format("No metadata has been associated with source file %s",
                    sourceFile.getName()));
        }

        Table table = metadataProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                sourceFile.getTableName());
        if (table == null) {
            throw new RuntimeException(String.format("No metadata available for source file %s", sourceFile.getName()));
        }
        return table;
    }
}
