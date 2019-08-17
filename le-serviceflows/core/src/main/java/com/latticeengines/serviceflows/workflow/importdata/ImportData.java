package com.latticeengines.serviceflows.workflow.importdata;

import java.util.Collections;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportStepConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("importData")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportData extends BaseWorkflowStep<ImportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportData.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private EaiProxy eaiProxy;

    @Override
    public void execute() {
        log.info("Inside ImportData execute()");
        importData();
    }

    private void importData() {
        EaiJobConfiguration importConfig = setupImportConfig();
        AppSubmission submission = eaiProxy.submitEaiJob(importConfig);
        putStringValueInContext(IMPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0));
        waitForAppId(submission.getApplicationIds().get(0));
        if (getConfiguration().getSourceType() == SourceType.FILE) {
            updateSourceFile();
        }
    }

    private ImportConfiguration setupImportConfig() {
        ImportConfiguration importConfig = new ImportConfiguration();
        SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
        sourceImportConfig.setSourceType(configuration.getSourceType());

        importConfig.addSourceConfiguration(sourceImportConfig);
        importConfig.setCustomerSpace(configuration.getCustomerSpace());

        if (sourceImportConfig.getSourceType() == SourceType.FILE) {
            SourceFile sourceFile = retrieveSourceFile(getConfiguration().getCustomerSpace(), //
                    getConfiguration().getSourceFileName());
            Table metadata = retrieveMetadata(sourceFile);
            putStringValueInContext(SOURCE_FILE_PATH, sourceFile.getPath());
            sourceImportConfig.setProperty(ImportProperty.HDFSFILE, sourceFile.getPath());
            sourceImportConfig.setProperty(ImportProperty.METADATA, //
                    JsonUtils.serialize(metadata.getModelingMetadata()));
            sourceImportConfig.setProperty(ImportProperty.ID_COLUMN_NAME, InterfaceName.Id.name());
            sourceImportConfig.setTables(Collections.singletonList(metadata));
        }
        return importConfig;
    }

    private void updateSourceFile() {
        CustomerSpace space = getConfiguration().getCustomerSpace();
        SourceFile sourceFile = retrieveSourceFile(getConfiguration().getCustomerSpace(), //
                getConfiguration().getSourceFileName());
        sourceFile.setState(SourceFileState.Imported);
        plsInternalProxy.updateSourceFile(sourceFile, space.toString());

        Table table = metadataProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                sourceFile.getTableName());
        putObjectInContext(SOURCE_IMPORT_TABLE, table);
    }

    private Table retrieveMetadata(SourceFile sourceFile) {
        if (sourceFile.getTableName() == null) {
            throw new RuntimeException(
                    String.format("No metadata has been associated with source file %s", sourceFile.getName()));
        }

        Table table = metadataProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                sourceFile.getTableName());
        if (table == null) {
            throw new RuntimeException(String.format("No metadata available for source file %s", sourceFile.getName()));
        }
        return table;
    }
}
