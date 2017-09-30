package com.latticeengines.cdl.workflow.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateAggregateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidatePartitionConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateTransactionDataStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component("consolidateTransactionData")
public class ConsolidateTransactionData extends ConsolidateDataBase<ConsolidateTransactionDataStepConfiguration> {

    private static final String TRANSACTION_DATE = "TransactionDate";
    private static final String AGGREGATE_TABLE_KEY = "Aggregate";
    private static final String MASTER_TABLE_KEY = "Master";
    private static final String DELTA_TABLE_KEY = "Delta";

    private static final Logger log = LoggerFactory.getLogger(ConsolidateTransactionData.class);

    private static String regex = "^" + BusinessEntity.Transaction.name()
            + "_(?:[0-9]{2})?[0-9]{2}-[0-3]?[0-9]-[0-3]?[0-9]$";
    protected static Pattern pattern = Pattern.compile(regex);

    @Autowired
    TransactionTableBuilder transactionTableBuilder;
    private int inputMergeStep;
    private int partitionAndAggregateStep;

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
            TransformationStepConfig inputMerge = mergeInputs(true);
            TransformationStepConfig partitionAggr = partitionAndAggregate();

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(inputMerge);
            steps.add(partitionAggr);
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig partitionAndAggregate() {
        /* Step 2: Partition and Aggregate */
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setInputSteps(Collections.singletonList(inputMergeStep));
        step2.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_PARTITION);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        step2.setTargetTable(targetTable);
        step2.setConfiguration(getPartitionConfig());
        return step2;
    }

    private String getPartitionConfig() {
        ConsolidatePartitionConfig config = new ConsolidatePartitionConfig();
        config.setNamePrefix(batchStoreTablePrefix);
        config.setTimeField(InterfaceName.TransactionTime.name());
        config.setConsolidateDateConfig(getConsolidateDataConfig(false));
        config.setAggregateConfig(getAggregateConfig());
        return appendEngineConf(config, lightEngineConfig());
    }

    private String getAggregateConfig() {
        ConsolidateAggregateConfig config = new ConsolidateAggregateConfig();
        config.setCountField(InterfaceName.Quantity.name());
        config.setSumField(InterfaceName.Amount.name());
        config.setTrxDateField(TRANSACTION_DATE);
        config.setGoupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name(),
                InterfaceName.ProductId.name(), InterfaceName.TransactionType.name(), TRANSACTION_DATE));
        return appendEngineConf(config, lightEngineConfig());
    }

    @Override
    protected String getConsolidateDataConfig(boolean isDedupeSource) {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        config.setMasterIdField(TableRoleInCollection.ConsolidatedTransaction.getPrimaryKey().name());
        config.setCreateTimestampColumn(true);
        config.setColumnsFromRight(new HashSet<String>(Arrays.asList(CREATION_DATE)));
        config.setCompositeKeys(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name(),
                InterfaceName.ProductId.name(), InterfaceName.TransactionType.name(),
                InterfaceName.TransactionTime.name()));
        config.setDedupeSource(isDedupeSource);
        return JsonUtils.serialize(config);
    }

    @Override
    protected void onPostTransformationCompleted() {
        String aggrTableName = TableUtils.getFullTableName(servingStoreTablePrefix, pipelineVersion);
        Table aggregateTable = metadataProxy.getTable(customerSpace.toString(), aggrTableName);
        putObjectInContext(AGGREGATE_TABLE_KEY, aggregateTable);
        Table masterTable = transactionTableBuilder.setupMasterTable(batchStoreTablePrefix, pipelineVersion, aggregateTable);
        putObjectInContext(MASTER_TABLE_KEY, masterTable);
        if (isBucketing()) {
            Table deltaTable = transactionTableBuilder.setupDeltaTable(servingStoreTablePrefix, pipelineVersion, aggregateTable);
            putObjectInContext(DELTA_TABLE_KEY, deltaTable);
        }
        super.onPostTransformationCompleted();
        metadataProxy.deleteTable(customerSpace.toString(),
                TableUtils.getFullTableName(getMergeTableName(), pipelineVersion));

    }

    @Override
    protected void setupConfig(ConsolidateDataTransformerConfig config) {
        config.setMasterIdField(TableRoleInCollection.ConsolidatedTransaction.getPrimaryKey().name());
    }
}
