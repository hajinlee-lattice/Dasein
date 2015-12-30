package com.latticeengines.serviceflows.workflow.scoring;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("score")
public class Score extends BaseWorkflowStep<ScoreStepConfiguration> {

    private static final Log log = LogFactory.getLog(Score.class);

    @Override
    public void execute() {
        log.info("Inside Score execute()");
        Map.Entry<ScoringConfiguration, String> scoringConfigAndTableName = buildScoringConfig();
        ScoringConfiguration scoringConfig = scoringConfigAndTableName.getKey();
        String url = configuration.getMicroServiceHostPort() + "/scoring/scoringjobs";
        AppSubmission submission = restTemplate.postForObject(url, scoringConfig,
                AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString(), //
                configuration.getMicroServiceHostPort());

        if (configuration.isRegisterScoredTable()) {
            try {
                registerTable(scoringConfigAndTableName.getValue(), scoringConfig.getTargetResultDir());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void registerTable(String tableName, String targetDir) throws Exception {
        List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, targetDir,
                new HdfsUtils.HdfsFilenameFilter() {

                    @Override
                    public boolean accept(String fileName) {
                        return fileName.endsWith(".avro");
                    }
                });

        Table eventTable = MetadataConverter.getTable(yarnConfiguration,
                avroFiles.get(0), null, null);
        eventTable.setName(tableName);
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s",
                configuration.getMicroServiceHostPort(), configuration.getCustomerSpace(),
                tableName);
        restTemplate.postForLocation(url, eventTable);
        executionContext.putString(SCORING_RESULT_TABLE, tableName);
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
