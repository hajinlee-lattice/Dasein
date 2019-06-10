package com.latticeengines.scoring.workflow.steps;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ExportScoreTrainingFileStepConfiguration;
import com.latticeengines.serviceflows.workflow.export.BaseExportData;

@Component("exportScoreTrainingFile")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportScoreTrainingFile extends BaseExportData<ExportScoreTrainingFileStepConfiguration> {

    @Override
    public void execute() {
        exportData();
    }

    protected String getTableName() {
        String tableName = getStringValueFromContext(EXPORT_SCORE_TRAINING_FILE_TABLE_NAME);
        if (tableName == null) {
            tableName = configuration.getTableName();
        }
        return tableName;
    }

    protected String getExportInputPath() {
        return null;
    }

    protected String getExportOutputPath() {
        String outputPath = getStringValueFromContext(EXPORT_SCORE_TRAINING_FILE_OUTPUT_PATH);
        return StringUtils.isNotBlank(outputPath) ? outputPath : null;
    }

    protected String getExclusionColumns() {
        return ScoreResultField.Probability.displayName + ";" + ScoreResultField.NormalizedScore.displayName + ";"
                + ScoreResultField.PredictedRevenuePercentile.displayName + ";"
                + ScoreResultField.ExpectedRevenuePercentile.displayName;
    }

}
