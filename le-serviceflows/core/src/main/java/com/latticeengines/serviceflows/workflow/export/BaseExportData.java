package com.latticeengines.serviceflows.workflow.export;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseExportData<T extends ExportStepConfiguration> extends BaseWorkflowStep<T> {

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private EaiProxy eaiProxy;

    protected void exportData() {
        EaiJobConfiguration exportConfig = setupExportConfig();
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
        putStringValueInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0));
        waitForAppId(submission.getApplicationIds().get(0));
    }

    private ExportConfiguration setupExportConfig() {
        ExportConfiguration exportConfig = new ExportConfiguration();
        exportConfig.setExportFormat(configuration.getExportFormat());
        exportConfig.setExportDestination(configuration.getExportDestination());
        exportConfig.setCustomerSpace(configuration.getCustomerSpace());
        exportConfig.setUsingDisplayName(configuration.getUsingDisplayName());
        exportConfig.setExclusionColumns(getExclusionColumns());
        exportConfig.setInclusionColumns(getInclusionColumns());
        exportConfig.setTable(retrieveTable());
        exportConfig.setExportInputPath(getExportInputPath());
        Map<String, String> properties = configuration.getProperties();
        if (StringUtils.isNotBlank(getExportOutputPath())) {
            exportConfig.setExportTargetPath(getExportOutputPath());
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

    protected String getExclusionColumns() {
        return null;
    }

    protected String getInclusionColumns() {
        return null;
    }

    protected abstract String getTableName();
    protected abstract String getExportInputPath();
    protected abstract String getExportOutputPath();

}
