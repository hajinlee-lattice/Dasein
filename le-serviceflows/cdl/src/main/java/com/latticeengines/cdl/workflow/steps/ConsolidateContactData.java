package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateContactDataStepConfiguration;

@Component("consolidateContactData")
public class ConsolidateContactData extends ConsolidateDataBase<ConsolidateContactDataStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateContactData.class);

    private String srcIdField;

    private int mergeStep;
    private int upsertMasterStep;
    private int diffStep;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        srcIdField = configuration.getIdField();
    }

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidatePipeline");

            mergeStep = 0;
            upsertMasterStep = 1;
            diffStep = 2;
            TransformationStepConfig merge = mergeInputs();
            TransformationStepConfig upsertMaster = mergeMaster();
            TransformationStepConfig diff = diff();
            TransformationStepConfig sort = sortDiff();

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(upsertMaster);
            if (isBucketing()) {
                steps.add(diff);
                steps.add(sort);
            }
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig mergeMaster() {
        List<String> baseSources;
        Map<String, SourceTable> baseTables;
        TargetTable targetTable;
        TransformationStepConfig step2 = new TransformationStepConfig();
        if (StringUtils.isNotBlank(inputMasterTableName)) {
            Table masterTable = metadataProxy.getTable(customerSpace.toString(), inputMasterTableName);
            if (masterTable != null && !masterTable.getExtracts().isEmpty()) {
                baseSources = Collections.singletonList(inputMasterTableName);
                baseTables = new HashMap<>();
                SourceTable sourceMasterTable = new SourceTable(inputMasterTableName, customerSpace);
                baseTables.put(inputMasterTableName, sourceMasterTable);
                step2.setBaseSources(baseSources);
                step2.setBaseTables(baseTables);
            }
        }
        step2.setInputSteps(Collections.singletonList(mergeStep));
        step2.setTransformer("consolidateDataTransformer");
        step2.setConfiguration(getConsolidateDataConfig(false));

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(batchStoreTablePrefix);
        targetTable.setPrimaryKey(batchStorePrimaryKey);
        step2.setTargetTable(targetTable);
        return step2;
    }

    private TransformationStepConfig diff() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step3 = new TransformationStepConfig();
        step3.setInputSteps(Arrays.asList(mergeStep, upsertMasterStep));
        step3.setTransformer("consolidateDeltaTransformer");
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        step3.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step3;
    }

    private TransformationStepConfig sortDiff() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step4 = new TransformationStepConfig();
        step4.setInputSteps(Collections.singletonList(diffStep));
        step4.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(true);
        step4.setTargetTable(targetTable);

        SorterConfig config = new SorterConfig();
        config.setPartitions(100);
        config.setSortingField(servingStoreSortKeys.get(0));
        config.setCompressResult(true);
        step4.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step4;
    }

    @Override
    protected String getConsolidateDataConfig(boolean isDedupeSource) {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        config.setMasterIdField(TableRoleInCollection.ConsolidatedContact.getPrimaryKey().name());
        config.setDedupeSource(isDedupeSource);
        return appendEngineConf(config, lightEngineConfig());
    }

}
