package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.ConsolidateDataBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDeltaTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateDataConfiguration;

@Component("consolidateAccountData")
public class ConsolidateAccountData extends ConsolidateDataBase<ConsolidateDataConfiguration> {

    private static final Log log = LogFactory.getLog(ConsolidateAccountData.class);

    private String srcIdField;
    private Map<MatchKey, List<String>> keyMap = null;

    private int mergeStep;
    private int matchStep;
    private int upsertMasterStep;
    private int consolidatedStep;
    private int bucketStep;
    private int sortStep;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        srcIdField = configuration.getIdField();
        keyMap = configuration.getMatchKeyMap();
    }

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidatePipeline");

            mergeStep = 0;
            matchStep = 1;
            upsertMasterStep = 2;
            consolidatedStep = 3;
            bucketStep = 4;
            sortStep = 5;

            TransformationStepConfig merge = createMergeStep();
            TransformationStepConfig match = createMatchStep();
            TransformationStepConfig upsertMaster = createUpsertMasterStep();
            TransformationStepConfig consolidate = createConsolidatedStep();
            TransformationStepConfig bucket = createBucketStep();
            TransformationStepConfig sort = createSorterStep();

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(match);
            steps.add(upsertMaster);
            steps.add(consolidate);
            if (isBucketing()) {
                steps.add(bucket);
                steps.add(sort);
            }
            request.setSteps(steps);
            if (isBucketing()) {
                request.setContainerMemMB(workflowMemMb);
            } else {
                request.setContainerMemMB(workflowMemMbMax);
            }
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig createMergeStep() {
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = inputTableNames;
        step1.setBaseSources(baseSources);

        Map<String, SourceTable> baseTables = new HashMap<>();
        for (String inputTableName : inputTableNames) {
            baseTables.put(inputTableName, new SourceTable(inputTableName, customerSpace));
        }
        step1.setBaseTables(baseTables);
        step1.setTransformer("consolidateDataTransformer");
        step1.setConfiguration(getConsolidateDataConfig());
        return step1;
    }

    private TransformationStepConfig createMatchStep() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        // step 1 output
        step2.setInputSteps(Collections.singletonList(mergeStep));
        step2.setTransformer(TRANSFORMER_MATCH);
        step2.setConfiguration(getMatchConfig());
        return step2;
    }

    private TransformationStepConfig createUpsertMasterStep() {
        List<String> baseSources;
        Map<String, SourceTable> baseTables;
        TargetTable targetTable;
        TransformationStepConfig step3 = new TransformationStepConfig();
        if (StringUtils.isNotBlank(inputMasterTableName)) {
            Table masterTable = metadataProxy.getTable(customerSpace.toString(), inputMasterTableName);
            if (masterTable != null && !masterTable.getExtracts().isEmpty()) {
                baseSources = Collections.singletonList(inputMasterTableName);
                baseTables = new HashMap<>();
                SourceTable sourceMasterTable = new SourceTable(inputMasterTableName, customerSpace);
                baseTables.put(inputMasterTableName, sourceMasterTable);
                step3.setBaseSources(baseSources);
                step3.setBaseTables(baseTables);
            }
        }
        step3.setInputSteps(Collections.singletonList(matchStep));
        step3.setTransformer("consolidateDataTransformer");
        step3.setConfiguration(getConsolidateDataConfig());

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(outputMasterTablePrefix);
        targetTable.setPrimaryKey(primaryKey);
        step3.setTargetTable(targetTable);
        return step3;
    }

    private TransformationStepConfig createConsolidatedStep() {
        TransformationStepConfig step4 = new TransformationStepConfig();
        step4.setInputSteps(Arrays.asList(matchStep, upsertMasterStep));
        step4.setTransformer("consolidateDeltaTransformer");
        step4.setConfiguration(getConsolidateDeltaConfig());
        if (!isBucketing()) {
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(consolidatedTablePrefix);
            targetTable.setPrimaryKey(primaryKey);
            step4.setTargetTable(targetTable);
        }
        return step4;
    }

    private TransformationStepConfig createBucketStep() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step5 = new TransformationStepConfig();
        String tableSourceName = "CustomerProfile";
        SourceTable sourceTable = new SourceTable(profileTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step5.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step5.setBaseTables(baseTables);
        // consolidate diff
        step5.setInputSteps(Collections.singletonList(consolidatedStep));
        step5.setTransformer(TRANSFORMER_BUCKETER);
        step5.setConfiguration(emptyStepConfig(lightEngineConfig()));
        return step5;
    }

    private TransformationStepConfig createSorterStep() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step6 = new TransformationStepConfig();
        step6.setInputSteps(Collections.singletonList(bucketStep));
        step6.setTransformer(TRANSFORMER_SORTER);
        step6.setConfiguration(sortStepConfiguration());

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(consolidatedTablePrefix);
        // targetTable.setPrimaryKey(accountId);
        step6.setTargetTable(targetTable);
        return step6;
    }

    private String sortStepConfiguration() {
        try {
            SorterConfig config = new SorterConfig();
            config.setPartitions(100);
            config.setSortingField(masterTableSortKeys.get(0));
            config.setCompressResult(false);
            return appendEngineConf(config, lightEngineConfig());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private String getConsolidateDataConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        return appendEngineConf(config, lightEngineConfig());
    }

    private String getConsolidateDeltaConfig() {
        ConsolidateDeltaTransformerConfig config = new ConsolidateDeltaTransformerConfig();
        config.setSrcIdField(srcIdField);
        return appendEngineConf(config, lightEngineConfig());
    }

    private String getMatchConfig() {
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setPredefinedSelection(Predefined.ID);
        if (keyMap == null) {
            matchInput.setKeyMap(getKeyMap());
        } else {
            matchInput.setKeyMap(keyMap);
        }
        matchInput.setExcludeUnmatchedWithPublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(true);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(false);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        config.setMatchInput(matchInput);
        return JsonUtils.serialize(config);
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(InterfaceName.LatticeAccountId.name()));
        return keyMap;
    }

    @Override
    public BusinessEntity getBusinessEntity() {
        return BusinessEntity.Account;
    }

    @Override
    public boolean isBucketing() {
        return Boolean.TRUE.equals(isActive);
    }

}
