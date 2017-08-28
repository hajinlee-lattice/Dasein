package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateAggregateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidatePartitionConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateTransactionDataStepConfiguration;

@Component("consolidateTransactionData")
public class ConsolidateTransactionData extends ConsolidateDataBase<ConsolidateTransactionDataStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateTransactionData.class);

    private String srcIdField;

    private int inputMergeStep;
    private int partitionAndAggregateStep;
    private int bucketStep;
    
    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        srcIdField = configuration.getIdField();
    }

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidatePipeline");

            inputMergeStep = 0;
            partitionAndAggregateStep = 1;
            bucketStep = 2;
            TransformationStepConfig inputMerge = mergeInputs();
            TransformationStepConfig partitionAggr = partitionAndAggregate();
            TransformationStepConfig bucket = bucket();

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(inputMerge);
            steps.add(partitionAggr);
            if (isBucketing()) {
                steps.add(bucket);
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
    
    private TransformationStepConfig partitionAndAggregate() {
        /* Step 2: Partition and Aggregate */
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setInputSteps(Collections.singletonList(inputMergeStep));
        step2.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_PARTITION);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(batchStoreTablePrefix);
        targetTable.setPrimaryKey(batchStorePrimaryKey);
        step2.setTargetTable(targetTable);
        step2.setConfiguration(getPartitionConfig());
        return step2;
    }

    private TransformationStepConfig bucket() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerProfile";
        SourceTable sourceTable = new SourceTable(profileTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        // consolidate diff
        step.setInputSteps(Collections.singletonList(partitionAndAggregateStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(heavyEngineConfig()));
        return step;
    }
    
    private String getPartitionConfig() {
        ConsolidatePartitionConfig config = new ConsolidatePartitionConfig();
        config.setNamePrefix(TableRoleInCollection.ConsolidatedTransaction.name());
        config.setTimeField("TransactionTime");
        config.setConsolidateDateConfig(getConsolidateDataConfig());
        config.setAggregateConfig(getAggregateConfig());
        return appendEngineConf(config, lightEngineConfig());
    }


    private String getAggregateConfig() {
        ConsolidateAggregateConfig config = new ConsolidateAggregateConfig();
        config.setCountField("Quantity");
        config.setSumField("Amount");
        config.setTrxDateField("TransactionDate");
        config.setGoupByFields(Arrays.asList("AccountId", "ContactId", "EntityType", "ProductName", "TransactionType",
                "TransactionDate"));
        return appendEngineConf(config, lightEngineConfig());
    }

    private String getConsolidateDataConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        config.setMasterIdField(TableRoleInCollection.ConsolidatedTransaction.getPrimaryKey().name());
        config.setCreateTimestampColumn(true);
        config.setColumnsFromRight(new HashSet<String>(Arrays.asList("CREATION_DATE")));
        config.setCompositeKeys(Arrays.asList("AccountId", "ContactId", "EntityType", "ProductName", "TransactionType",
                "TransactionTime"));
        return JsonUtils.serialize(config);
    }

    @Override
    public boolean isBucketing() {
        return Boolean.TRUE.equals(isActive);
    }

}
