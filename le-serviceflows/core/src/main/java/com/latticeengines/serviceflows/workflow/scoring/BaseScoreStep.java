package com.latticeengines.serviceflows.workflow.scoring;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.scoring.ScoringProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

public abstract class BaseScoreStep<T extends ScoreStepConfiguration> extends BaseWorkflowStep<T> {
    private static final Log log = LogFactory.getLog(Score.class);

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
        waitForAppId(submission.getApplicationIds().get(0), configuration.getMicroServiceHostPort());
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

                    saveOutputValue(WorkflowContextConstants.Outputs.ERROR_OUTPUT_PATH.toString(), targetDir
                            + "/error.csv");
                }
            } catch (IOException e) {
                log.error(ExceptionUtils.getFullStackTrace(e));
                throw new LedpException(LedpCode.LEDP_00002);
            }
        } else {
            log.info("Context is not set, so looking for extract from input table for scoring training table.");
        }
    }

    private void registerTable(String tableName, String targetDir) throws Exception {
        Table eventTable = MetadataConverter.getTable(yarnConfiguration, targetDir, null, null);
        eventTable.setName(tableName);
        metadataProxy.createTable(configuration.getCustomerSpace().toString(), tableName, eventTable);
        putStringValueInContext(SCORING_RESULT_TABLE_NAME, tableName);
    }

    private Map.Entry<ScoringConfiguration, String> buildScoringConfig() {
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(configuration.getCustomerSpace().toString());
        String modelId = getModelId();
        scoringConfig.setModelGuids(Arrays.asList(new String[] { modelId }));
        scoringConfig.setSourceDataDir(getSourceDir());
        scoringConfig.setUniqueKeyColumn(getUniqueKeyColumn());
        Path targetPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(), //
                configuration.getCustomerSpace());
        String tableName = String.format("ScoreResult_%s_%d", modelId.replaceAll("-", "_"), System.currentTimeMillis());
        scoringConfig.setTargetResultDir(targetPath.toString() + "/" + tableName);
        return new AbstractMap.SimpleEntry<ScoringConfiguration, String>(scoringConfig, tableName);
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
        if (uniqueKeyColumn == null) {
            uniqueKeyColumn = configuration.getUniqueKeyColumn();
        }
        return uniqueKeyColumn;
    }
}
