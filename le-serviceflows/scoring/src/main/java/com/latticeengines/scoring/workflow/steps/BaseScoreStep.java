package com.latticeengines.scoring.workflow.steps;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.scoring.ScoringProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseScoreStep<T extends ScoreStepConfiguration> extends BaseWorkflowStep<T> {
    private static final Logger log = LoggerFactory.getLogger(Score.class);

    @Autowired
    private ScoringProxy scoringProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Inside Score execute()");
        Map.Entry<ScoringConfiguration, String> scoringConfigAndTableName = buildScoringConfig();
        ScoringConfiguration scoringConfig = scoringConfigAndTableName.getKey();
        AppSubmission submission = scoringProxy.createScoringJob(scoringConfig);
        waitForAppId(submission.getApplicationIds().get(0));
        copyImportErrors(scoringConfig.getTargetResultDir());

        if (configuration.isRegisterScoredTable()) {
            try {
                registerTable(scoringConfigAndTableName.getValue(), scoringConfig.getTargetResultDir());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void copyImportErrors(String targetDir) {
        String sourceImportTable = getStringValueFromContext(SOURCE_IMPORT_TABLE);
        String importErrorPath = "";
        if (StringUtils.isNotEmpty(sourceImportTable)) {
            Table table = JsonUtils.deserialize(sourceImportTable, Table.class);
            importErrorPath = table.getExtractsDirectory() + "/error.csv";

            try {
                if (HdfsUtils.fileExists(yarnConfiguration, importErrorPath)) {
                    HdfsUtils.copyFiles(yarnConfiguration, importErrorPath, targetDir);

                    saveOutputValue(WorkflowContextConstants.Outputs.ERROR_OUTPUT_PATH,
                            targetDir + "/error.csv");
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw new LedpException(LedpCode.LEDP_00002);
            }
        } else {
            log.info("Context is not set, so looking for extract from input table for scoring training table.");
        }
    }

    private void registerTable(String tableName, String targetDir) throws Exception {
        Table eventTable = MetadataConverter.getTable(yarnConfiguration, targetDir, null, null, true);
        eventTable.setName(tableName);
        metadataProxy.createTable(configuration.getCustomerSpace().toString(), tableName, eventTable);
        putStringValueInContext(SCORING_RESULT_TABLE_NAME, tableName);
    }

    private Map.Entry<ScoringConfiguration, String> buildScoringConfig() {
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(configuration.getCustomerSpace().toString());
        String[] modelIds = getModelId().split("\\|");
        log.info("Get model ids: " + StringUtils.join(modelIds, ", "));
        scoringConfig.setModelGuids(Arrays.asList(modelIds));
        scoringConfig.setSourceDataDir(getSourceDir());
        scoringConfig.setUniqueKeyColumn(getUniqueKeyColumn());
        scoringConfig.setUseScorederivation(configuration.getUseScorederivation());
        scoringConfig.setModelIdFromRecord(configuration.getReadModelIdFromRecord());
        Path targetPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(), //
                configuration.getCustomerSpace());
        String tableName = String.format("ScoreResult_%s_%d", modelIds[0].replaceAll("-", "_"),
                System.currentTimeMillis());
        scoringConfig.setTargetResultDir(targetPath.toString() + "/" + tableName);
        if (getScoringInputType() != null) {
            scoringConfig.setScoreInputType(getScoringInputType());
        }
        return new AbstractMap.SimpleEntry<>(scoringConfig, tableName);
    }

    private String getModelId() {
        String modelId = getStringValueFromContext(SCORING_MODEL_ID);
        if (modelId == null) {
            modelId = configuration.getModelId();
        }
        return modelId;
    }

    private String getSourceDir() {
        String sourceDir = getStringValueFromContext(SCORING_SOURCE_DIR);
        if (sourceDir == null) {
            sourceDir = configuration.getSourceDir();
        }
        return sourceDir;
    }

    private String getUniqueKeyColumn() {
        String uniqueKeyColumn = getStringValueFromContext(SCORING_UNIQUEKEY_COLUMN);
        if (StringUtils.isNotEmpty(configuration.getUniqueKeyColumn()))
            uniqueKeyColumn = configuration.getUniqueKeyColumn();
        return uniqueKeyColumn;
    }

    protected ScoringConfiguration.ScoringInputType getScoringInputType() {
        return null;
    }

}
