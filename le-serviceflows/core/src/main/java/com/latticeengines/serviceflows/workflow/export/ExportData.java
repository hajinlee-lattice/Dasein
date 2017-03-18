package com.latticeengines.serviceflows.workflow.export;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

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
        EaiJobConfiguration exportConfig = setupExportConfig();
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
        putStringValueInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0).toString());
        waitForAppId(submission.getApplicationIds().get(0).toString());
        // saveToContext();
    }

    private ExportConfiguration setupExportConfig() {
        ExportConfiguration exportConfig = new ExportConfiguration();
        exportConfig.setExportFormat(configuration.getExportFormat());
        exportConfig.setExportDestination(configuration.getExportDestination());
        exportConfig.setCustomerSpace(configuration.getCustomerSpace());
        exportConfig.setUsingDisplayName(configuration.getUsingDisplayName());
        exportConfig.setTable(retrieveTable());
        if (StringUtils.isNotEmpty(getStringValueFromContext(EXPORT_INPUT_PATH))) {
            exportConfig.setExportInputPath(getStringValueFromContext(EXPORT_INPUT_PATH));
        }
        Map<String, String> properties = configuration.getProperties();
        if (StringUtils.isNotEmpty(getStringValueFromContext(EXPORT_OUTPUT_PATH))) {
            exportConfig.setExportTargetPath(getStringValueFromContext(EXPORT_OUTPUT_PATH));
        } else if (properties.containsKey(ExportProperty.TARGET_FILE_NAME)) {
            String targetPath = PathBuilder
                    .buildDataFileExportPath(CamilleEnvironment.getPodId(), configuration.getCustomerSpace())
                    .append(properties.get(ExportProperty.TARGET_FILE_NAME)).toString();
            exportConfig.setExportTargetPath(targetPath);
            saveOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH, targetPath);
        }
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
        String tableName = getStringValueFromContext(EXPORT_TABLE_NAME);
        if (tableName == null) {
            tableName = configuration.getTableName();
        }
        return tableName;
    }
}
