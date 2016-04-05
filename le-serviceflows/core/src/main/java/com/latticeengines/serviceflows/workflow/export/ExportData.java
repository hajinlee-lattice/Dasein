package com.latticeengines.serviceflows.workflow.export;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.WorkflowContextConstants;

@Component("exportData")
public class ExportData extends BaseWorkflowStep<ExportStepConfiguration> {

    private static final Log log = LogFactory.getLog(ExportData.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private EaiProxy eaiProxy;

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");
        exportData();
    }

    private void exportData() {
        ExportConfiguration exportConfig = setupExportConfig();
        AppSubmission submission = eaiProxy.createExportDataJob(exportConfig);
        putObjectInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0).toString());
        waitForAppId(submission.getApplicationIds().get(0).toString(), configuration.getMicroServiceHostPort());
        saveToContext();
    }

    private ExportConfiguration setupExportConfig() {
        ExportConfiguration exportConfig = new ExportConfiguration();
        exportConfig.setExportFormat(configuration.getExportFormat());
        exportConfig.setExportDestination(configuration.getExportDestination());
        exportConfig.setCustomerSpace(configuration.getCustomerSpace());
        exportConfig.setTable(retrieveTable());
        for (String propertyName : configuration.getProperties().keySet()) {
            exportConfig.setProperty(propertyName, configuration.getProperties().get(propertyName));
        }
        return exportConfig;
    }

    private Table retrieveTable() {
        String tableName = getTableName();
        return metadataProxy.getTable(configuration.getCustomerSpace().toString(), tableName);
    }

    private String getTableName() {
        String tableName = executionContext.getString(EXPORT_TABLE_NAME);
        if (tableName == null) {
            tableName = configuration.getTableName();
        }
        return tableName;
    }

    private void saveToContext() {
        Map<String, String> properties = configuration.getProperties();
        if (properties.containsKey(ExportProperty.TARGETPATH)) {
            putOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH,
                    PathBuilder
                            .buildDataFileExportPath(CamilleEnvironment.getPodId(), configuration.getCustomerSpace())
                            .append(properties.get(ExportProperty.TARGETPATH)).toString());
        }
    }
}
