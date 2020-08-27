package com.latticeengines.cdl.workflow.steps;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details;
import com.latticeengines.domain.exposed.spark.cdl.SplitTransactionConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.spark.exposed.job.cdl.SplitTransactionJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class SplitTransactionStep extends BaseProcessAnalyzeSparkStep<ProcessTransactionStepConfiguration> {
    // split rollup txn table by product type (analytic, spending)
    // add __StreamDate and StreamDateId column

    private static final Logger log = LoggerFactory.getLogger(SplitTransactionStep.class);

    private static final String RAW_TXN_ACTIVITY_FORMAT = "SPLIT_TXN_%s"; // product type

    @Override
    public void execute() {
        bootstrap();
        Map<String, Table> splitTables = getTablesFromMapCtxKey(customerSpaceStr, Raw_TXN_STREAMS);
        if (isShortcutMode(splitTables)) {
            Map<String, String> signatureTableNames = splitTables.entrySet().stream().map(entry -> {
                String productType = entry.getKey();
                Table table = entry.getValue();
                return Pair.of(productType, table.getName());
            }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            log.info("Found raw transaction streams {}. Going through shortcut mode.", signatureTableNames);
            dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableNames,
                    TableRoleInCollection.RawTransactionStream, inactive);
            return;
        }
        SparkJobResult result = runSparkJob(SplitTransactionJob.class, createSplitTransactionConfig());
        finishing(result);
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
        // using txn type as signature
        Map<String, String> signatureTableNames = exportToS3AndAddToContext(splitTables, Raw_TXN_STREAMS);
        log.info("Split transaction tables: {}", signatureTableNames);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableNames,
                TableRoleInCollection.RawTransactionStream, inactive);
    }

    private SplitTransactionConfig createSplitTransactionConfig() {
        SplitTransactionConfig config = new SplitTransactionConfig();
        Table txnTable = getTableSummaryFromKey(customerSpaceStr, ROLLUP_PRODUCT_TABLE);
        config.setInput(Collections.singletonList(txnTable.toHdfsDataUnit(null)));
        return config;
    }
}
