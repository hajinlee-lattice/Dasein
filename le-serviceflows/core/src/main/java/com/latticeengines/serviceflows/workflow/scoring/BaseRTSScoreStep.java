package com.latticeengines.serviceflows.workflow.scoring;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.scoring.ScoringProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

public abstract class BaseRTSScoreStep<T extends RTSScoreStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Log log = LogFactory.getLog(BaseRTSScoreStep.class);

    @Autowired
    private ScoringProxy scoringProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {

        log.info("Inside RTS Bulk Score execute()");
        Map.Entry<RTSBulkScoringConfiguration, String> scoringConfigAndTableName = buildScoringConfig();
        RTSBulkScoringConfiguration scoringConfig = scoringConfigAndTableName.getKey();
        String appId = scoringProxy.submitBulkScoreJob(scoringConfig).getApplicationIds().get(0);
        waitForAppId(appId, configuration.getMicroServiceHostPort());

        if (configuration.isRegisterScoredTable()) {
            try {
                registerTable(scoringConfigAndTableName.getValue(), scoringConfig.getTargetResultDir());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Map.Entry<RTSBulkScoringConfiguration, String> buildScoringConfig() {
        RTSBulkScoringConfiguration scoringConfig = new RTSBulkScoringConfiguration();
        String modelId = getModelId();
        scoringConfig.setModelGuids(Arrays.asList(new String[] { modelId }));
        Path targetPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(),
                configuration.getCustomerSpace());
        String tableName = String.format("RTSBulkScoreResult_%s_%d", modelId.replaceAll("-", "_"),
                System.currentTimeMillis());
        scoringConfig.setTargetResultDir(targetPath.toString() + "/" + tableName);
        String inputTableName = configuration.getInputTableName();
        Table metadataTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), inputTableName);
        scoringConfig.setMetadataTable(metadataTable);
        scoringConfig.setCustomerSpace(configuration.getCustomerSpace());
        return new AbstractMap.SimpleEntry<RTSBulkScoringConfiguration, String>(scoringConfig, tableName);
    }

    private String getModelId() {
        String modelId = getStringValueFromContext(SCORING_MODEL_ID);
        if (modelId == null) {
            modelId = configuration.getModelId();
        }
        return modelId;
    }

    private void registerTable(String tableName, String targetDir) throws Exception {
        Table eventTable = MetadataConverter.getTable(yarnConfiguration, targetDir, null, null);
        eventTable.setName(tableName);
        metadataProxy.createTable(configuration.getCustomerSpace().toString(), tableName, eventTable);
        executionContext.putString(SCORING_RESULT_TABLE_NAME, tableName);
    }

}
