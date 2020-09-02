package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details;
import com.latticeengines.domain.exposed.spark.cdl.CountProductTypeConfig;
import com.latticeengines.domain.exposed.spark.cdl.SplitTransactionConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.spark.exposed.job.cdl.CountProductTypeJob;
import com.latticeengines.spark.exposed.job.cdl.SplitTransactionJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class SplitTransactionStep extends BaseProcessAnalyzeSparkStep<ProcessTransactionStepConfiguration> {
    // split rollup txn table by product type (analytic, spending)
    // add __StreamDate and StreamDateId column

    private static final Logger log = LoggerFactory.getLogger(SplitTransactionStep.class);

    private static final String RAW_TXN_ACTIVITY_FORMAT = "RAW_%s_STREAM"; // product type

    @Override
    public void execute() {
        bootstrap();
        Map<String, Table> splitTables = getTablesFromMapCtxKey(customerSpaceStr, Raw_TXN_STREAMS);
        if (isShortcutMode(splitTables) && hasKeyInContext(RETAIN_PRODUCT_TYPE)) {
            Map<String, String> signatureTableNames = splitTables.entrySet().stream().map(entry -> {
                String productType = entry.getKey();
                Table table = entry.getValue();
                return Pair.of(productType, table.getName());
            }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            log.info("Found raw transaction streams {}. Going through shortcut mode.", signatureTableNames);
            log.info("Retaining transactions of product type: {}",
                    getListObjectFromContext(RETAIN_PRODUCT_TYPE, String.class));
            dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableNames,
                    TableRoleInCollection.RawTransactionStream, inactive);
            return;
        }
        SparkJobResult countResult = runSparkJob(CountProductTypeJob.class, createCountTypeConfig());
        log.info("Count product type result: {}", countResult.getOutput());
        Map<String, Long> countMap = JsonUtils.deserializeByTypeRef(countResult.getOutput(),
                new TypeReference<HashMap<String, Long>>() {
                });
        List<String> typesToRetain = countMap.entrySet().stream().filter(entry -> entry.getValue() > 0L)
                .map(Map.Entry::getKey).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(typesToRetain)) {
            throw new IllegalStateException("No transaction found for both spending and analytic product types");
        }
        log.info("Retaining transactions of product type: {}", typesToRetain);
        putObjectInContext(RETAIN_PRODUCT_TYPE, typesToRetain);
        SparkJobResult splitResult = runSparkJob(SplitTransactionJob.class,
                createSplitTransactionConfig(typesToRetain));
        finishing(splitResult);
    }

    private void finishing(SparkJobResult result) {
        log.info("Output metadata: {}", result.getOutput());
        List<HdfsDataUnit> outputs = result.getTargets();
        Map<String, Details> outputMetadata = JsonUtils
                .deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class).getMetadata();
        Map<String, Table> splitTables = new HashMap<>(); // product type -> table
        outputMetadata.forEach((productType, details) -> {
            String prefixFormat = String.format(RAW_TXN_ACTIVITY_FORMAT, productType);
            String tableName = TableUtils.getFullTableName(prefixFormat,
                    HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
            Table table = dirToTable(tableName, outputs.get(details.getStartIdx()));
            metadataProxy.createTable(customerSpaceStr, tableName, table);
            splitTables.put(productType, table);
        });
        saveSplitTables(splitTables);
    }

    private void saveSplitTables(Map<String, Table> splitTables) {
        // using product type as signature
        Map<String, String> signatureTableNames = exportToS3AndAddToContext(splitTables, Raw_TXN_STREAMS);
        log.info("Split transaction tables: {}", signatureTableNames);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableNames,
                TableRoleInCollection.RawTransactionStream, inactive);
    }

    private SplitTransactionConfig createSplitTransactionConfig(List<String> retainTypes) {
        SplitTransactionConfig config = new SplitTransactionConfig();
        config.retainProductType = retainTypes;
        Table txnTable = getTableSummaryFromKey(customerSpaceStr, ROLLUP_PRODUCT_TABLE);
        config.setInput(Collections.singletonList(txnTable.toHdfsDataUnit(null)));
        return config;
    }

    private CountProductTypeConfig createCountTypeConfig() {
        CountProductTypeConfig config = new CountProductTypeConfig();
        config.types = Arrays.asList(ProductType.Spending.name(), ProductType.Analytic.name());
        Table txnTable = getTableSummaryFromKey(customerSpaceStr, ROLLUP_PRODUCT_TABLE);
        config.setInput(Collections.singletonList(txnTable.toHdfsDataUnit(null)));
        return config;
    }
}
