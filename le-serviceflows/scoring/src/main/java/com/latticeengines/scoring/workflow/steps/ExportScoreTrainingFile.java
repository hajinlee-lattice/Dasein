package com.latticeengines.scoring.workflow.steps;

import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ExportScoreTrainingFileStepConfiguration;
import com.latticeengines.serviceflows.workflow.export.BaseExportData;

@Component("exportScoreTrainingFile")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportScoreTrainingFile extends BaseExportData<ExportScoreTrainingFileStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportScoreTrainingFile.class);

    @Inject
    private BatonService batonService;

    @Override
    public void execute() {
        String exportPath = exportData();
        log.info("Export score training file path = {}", exportPath);

        if (configuration.getExportFormat().equals(ExportFormat.CSV)
                && StringUtils.isNotBlank(exportPath)) {
            putStringValueInContext(EXPORT_MERGE_FILE_PATH, exportPath);
            // get export file name prefix and use it as merged csv filename
            String filename = FilenameUtils.getName(exportPath);
            filename = StringUtils.appendIfMissing(filename, ".csv");
            log.info("Merge file path = {}, filename = {}", exportPath, filename);
            putStringValueInContext(EXPORT_MERGE_FILE_NAME, filename);
            mergeCSVFiles(true);
        }
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

    protected String getInclusionColumns() {

        String existingColumns = getConfiguration().getExportInclusionColumns();

        Set<String> features = getSetObjectFromContext(SCORE_TRAINING_FILE_INCLUDED_FEATURES, String.class);
        if (CollectionUtils.isEmpty(features))
            return existingColumns;

        String featureListStr = StringUtils.join(features, ';');
        if (!StringUtils.isEmpty(existingColumns))
            return existingColumns + ";" + featureListStr;
        else
            return featureListStr;

    }

    protected String getExclusionColumns() {
        String exclusionColumns = ScoreResultField.NormalizedScore.displayName + ";" //
                + ScoreResultField.PredictedRevenuePercentile.displayName + ";" //
                + ScoreResultField.ExpectedRevenuePercentile.displayName;
        if (!getConfiguration().isExpectedValue()) {
            exclusionColumns += ";" + ScoreResultField.PredictedRevenue.displayName + ";"
                    + ScoreResultField.ExpectedRevenue.displayName;
        }
        if (batonService.isEntityMatchEnabled(getConfiguration().getCustomerSpace())) {
            exclusionColumns += ";" //
                    + InterfaceName.AccountId.name() + ";" //
                    + InterfaceName.EntityId.name() + ";" //
                    + InterfaceName.PeriodId.name();
        }
        return  exclusionColumns;
    }

}
