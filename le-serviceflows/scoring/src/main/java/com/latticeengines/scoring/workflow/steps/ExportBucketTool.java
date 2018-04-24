package com.latticeengines.scoring.workflow.steps;


import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ExportBucketToolStepConfiguration;
import com.latticeengines.serviceflows.workflow.export.BaseExportData;

@Component("exportBucketTool")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportBucketTool extends BaseExportData<ExportBucketToolStepConfiguration> {

    @Override
    public void execute() {
        configuration.setUsingDisplayName(false);
        exportData();
    }

    protected String getTableName() {
        String tableName = getStringValueFromContext(EXPORT_BUCKET_TOOL_TABLE_NAME);
        if (tableName == null) {
            tableName = configuration.getTableName();
        }
        return tableName;
    }

    protected String getExportInputPath() {
        return null;
    }

    protected String getExportOutputPath() {
        String outputPath = getStringValueFromContext(EXPORT_BUCKET_TOOL_OUTPUT_PATH);
        return StringUtils.isNotBlank(outputPath) ? outputPath : null;
    }

}
