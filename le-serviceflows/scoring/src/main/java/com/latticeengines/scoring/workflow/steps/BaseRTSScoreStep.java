package com.latticeengines.scoring.workflow.steps;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RTSScoreStepConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.proxy.exposed.scoring.ScoringProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseRTSScoreStep<T extends RTSScoreStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseRTSScoreStep.class);

    @Autowired
    private ScoringProxy scoringProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private BatonService batonService;

    @Autowired
    private PlsInternalProxy plsInternalProxy;

    @Override
    public void execute() {

        log.info("Inside RTS Bulk Score execute()");
        Map.Entry<RTSBulkScoringConfiguration, String> scoringConfigAndTableName = buildScoringConfig();
        RTSBulkScoringConfiguration scoringConfig = scoringConfigAndTableName.getKey();
        CustomerSpace customerSpace = scoringConfig.getCustomerSpace();

        boolean enrichmentEnabledForInternalAttributes = batonService.isEnabled(customerSpace,
                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);

        List<String> internalAttributes = getSelectedInternalEnrichmentAttributes(customerSpace,
                enrichmentEnabledForInternalAttributes);
        String appId = scoringProxy.submitBulkScoreJob(scoringConfig).getApplicationIds().get(0);

        if (!internalAttributes.isEmpty()) {
            executionContext.put(//
                    WorkflowContextConstants.Outputs//
                            .ENRICHMENT_FOR_INTERNAL_ATTRIBUTES_ATTEMPTED, //
                    Boolean.TRUE);
            executionContext.put(//
                    WorkflowContextConstants.Outputs//
                            .INTERNAL_ENRICHMENT_ATTRIBUTES_LIST, //
                    JsonUtils.serialize(internalAttributes));
        }

        waitForAppId(appId);

        saveOutputValue(WorkflowContextConstants.Outputs.ERROR_OUTPUT_PATH.toString(),
                scoringConfig.getTargetResultDir() + "/error.csv");
        if (configuration.isRegisterScoredTable()) {
            try {
                registerTable(scoringConfigAndTableName.getValue(), scoringConfig.getTargetResultDir());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<String> getSelectedInternalEnrichmentAttributes(CustomerSpace customerSpace,
            boolean enrichmentEnabledForInternalAttributes) {
        List<LeadEnrichmentAttribute> leadEnrichmentAttributeList = plsInternalProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, Boolean.TRUE,
                        enrichmentEnabledForInternalAttributes);

        List<String> internalAttributes = new ArrayList<>();

        for (LeadEnrichmentAttribute attribute : leadEnrichmentAttributeList) {
            if (attribute.getIsInternal()) {
                internalAttributes.add(attribute.getFieldName());
            }
        }

        return internalAttributes;
    }

    private Map.Entry<RTSBulkScoringConfiguration, String> buildScoringConfig() {
        RTSBulkScoringConfiguration scoringConfig = new RTSBulkScoringConfiguration();
        String modelId = getModelId();
        scoringConfig.setEnableLeadEnrichment(configuration.getEnableLeadEnrichment());
        scoringConfig.setEnableDebug(configuration.getEnableDebug());
        scoringConfig.setScoreTestFile(configuration.getScoreTestFile());
        scoringConfig.setModelGuids(Arrays.asList(modelId));
        scoringConfig.setModelType(getModelType());
        scoringConfig.setIdColumnName(configuration.getIdColumnName());
        Path targetPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(),
                configuration.getCustomerSpace());
        String tableName = String.format("RTSBulkScoreResult_%s_%d", modelId.replaceAll("-", "_"),
                System.currentTimeMillis());
        scoringConfig.setTargetResultDir(targetPath.toString() + "/" + tableName);
        String inputTableName = configuration.getInputTableName();
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        if (eventTable != null && StringUtils.isNotEmpty(eventTable.getName())) {
            inputTableName = eventTable.getName();
        }
        Table metadataTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), inputTableName);
        scoringConfig.setMetadataTable(metadataTable);
        scoringConfig.setCustomerSpace(configuration.getCustomerSpace());
        scoringConfig.setInternalResourceHostPort(configuration.getInternalResourceHostPort());
        scoringConfig.setEnableMatching(configuration.getEnableMatching());

        String sourceImportTable = getStringValueFromContext(SOURCE_IMPORT_TABLE);
        String importErrorPath = "";
        if (StringUtils.isNotEmpty(sourceImportTable)) {
            Table table = JsonUtils.deserialize(sourceImportTable, Table.class);
            importErrorPath = table.getExtractsDirectory() + "/error.csv";
        } else {
            log.info("Context is not set, so looking for extract from input table for scoring training table.");
            importErrorPath = metadataTable.getExtractsDirectory() + "/error.csv";
        }
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, importErrorPath)) {
                scoringConfig.setImportErrorPath(importErrorPath);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_00002);
        }

        return new AbstractMap.SimpleEntry<RTSBulkScoringConfiguration, String>(scoringConfig, tableName);
    }

    private String getModelId() {
        String modelId = getStringValueFromContext(SCORING_MODEL_ID);
        if (modelId == null) {
            modelId = configuration.getModelId();
        }
        return modelId;
    }

    private String getModelType() {
        String modelId = getStringValueFromContext(SCORING_MODEL_TYPE);
        if (modelId == null) {
            modelId = configuration.getModelType();
        }
        return modelId;
    }

    private void registerTable(String tableName, String targetDir) throws Exception {
        Table eventTable = MetadataConverter.getTable(yarnConfiguration, targetDir, null, null);
        eventTable.setName(tableName);
        metadataProxy.createTable(configuration.getCustomerSpace().toString(), tableName, eventTable);
        putStringValueInContext(SCORING_RESULT_TABLE_NAME, tableName);
    }

}
