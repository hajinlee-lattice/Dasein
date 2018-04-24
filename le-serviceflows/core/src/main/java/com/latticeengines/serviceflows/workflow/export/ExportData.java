package com.latticeengines.serviceflows.workflow.export;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;

@Component("exportData")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportData extends BaseExportData<ExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportData.class);

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");
        exportData();
    }

    protected String getTableName() {
        String tableName = getStringValueFromContext(EXPORT_TABLE_NAME);
        if (tableName == null) {
            tableName = configuration.getTableName();
        }
        return tableName;
    }

    protected String getExportInputPath() {
        String inputPath = getStringValueFromContext(EXPORT_INPUT_PATH);
        return StringUtils.isNotBlank(inputPath) ? inputPath : null;
    }

    protected String getExportOutputPath() {
        String outputPath = getStringValueFromContext(EXPORT_OUTPUT_PATH);
        return StringUtils.isNotBlank(outputPath) ? outputPath : null;
    }

}
