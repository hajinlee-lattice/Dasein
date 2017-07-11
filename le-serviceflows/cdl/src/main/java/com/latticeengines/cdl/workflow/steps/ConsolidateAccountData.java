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

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateAccountDataStepConfiguration;

@Component("consolidateAccountData")
public class ConsolidateAccountData extends ConsolidateDataBase<ConsolidateAccountDataStepConfiguration> {

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

            TransformationStepConfig merge = mergeInputs();
            TransformationStepConfig match = match();
            TransformationStepConfig upsertMaster = mergeMaster();
            TransformationStepConfig diff = diff();
            TransformationStepConfig bucket = bucketDiff();
            TransformationStepConfig sort = sortBucketedDiff();

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(match);
            steps.add(upsertMaster);
            if (isBucketing()) {
                steps.add(diff);
                steps.add(bucket);
                steps.add(sort);
            }
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig mergeInputs() {
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

    private TransformationStepConfig match() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        // step 1 output
        step2.setInputSteps(Collections.singletonList(mergeStep));
        step2.setTransformer(TRANSFORMER_MATCH);
        step2.setConfiguration(getMatchConfig());
        return step2;
    }

    private TransformationStepConfig mergeMaster() {
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
        targetTable.setNamePrefix(batchStoreTablePrefix);
        targetTable.setPrimaryKey(batchStorePrimaryKey);
        step3.setTargetTable(targetTable);
        return step3;
    }

    private TransformationStepConfig diff() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step4 = new TransformationStepConfig();
        step4.setInputSteps(Arrays.asList(matchStep, upsertMasterStep));
        step4.setTransformer("consolidateDeltaTransformer");
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        step4.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step4;
    }

    private TransformationStepConfig bucketDiff() {
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

    private TransformationStepConfig sortBucketedDiff() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step6 = new TransformationStepConfig();
        step6.setInputSteps(Collections.singletonList(bucketStep));
        step6.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(true);
        step6.setTargetTable(targetTable);

        SorterConfig config = new SorterConfig();
        config.setPartitions(100);
        config.setSortingField(servingStoreSortKeys.get(0));
        config.setCompressResult(true);
        step6.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step6;
    }

    private String getConsolidateDataConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
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
    public boolean isBucketing() {
        return Boolean.TRUE.equals(isActive);
    }

}
