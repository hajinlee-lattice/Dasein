package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
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
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.AggDailyActivityConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.spark.exposed.job.cdl.AggDailyActivityJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class AggDailyTransactionStep extends BaseProcessAnalyzeSparkStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AggDailyTransactionStep.class);

    private static final String DAILY_TXN_FORMAT = "DAILY_TXN_%s"; // product type type

    @Override
    public void execute() {
        bootstrap();
        Map<String, Table> dailyTxnStreams = getTablesFromMapCtxKey(customerSpaceStr, DAILY_TXN_STREAMS);
        if (isShortcutMode(dailyTxnStreams)) {
            Map<String, String> signatureTableNames = dailyTxnStreams.entrySet().stream().map(entry -> {
                String productType = entry.getKey();
                Table table = entry.getValue();
                return Pair.of(productType, table.getName());
            }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            log.info("Found daily transaction streams {}. Going through shortcut mode.", signatureTableNames);
            dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableNames,
                    TableRoleInCollection.DailyTransactionStream, inactive);
            return;
        }
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, getSparkConfig());
        processOutputMetadata(result);
    }

    private void processOutputMetadata(SparkJobResult result) {
        log.info("Output metadata: {}", result.getOutput());
        List<HdfsDataUnit> outputs = result.getTargets();
        Map<String, ActivityStoreSparkIOMetadata.Details> outputMetadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class).getMetadata();
        Map<String, Table> dailyStores = new HashMap<>(); // type -> table
        outputMetadata.forEach((productType, details) -> {
            String prefix = String.format(DAILY_TXN_FORMAT, productType);
            String tableName = TableUtils.getFullTableName(prefix, HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
            Table table = dirToTable(tableName, outputs.get(details.getStartIdx()));
            metadataProxy.createTable(customerSpaceStr, tableName, table);
            dailyStores.put(productType, table);
        });
        saveDailyTxnTables(dailyStores);
    }

    private void saveDailyTxnTables(Map<String, Table> dailyStores) {
        Map<String, String> signatureTableNames = exportToS3AndAddToContext(dailyStores, DAILY_TXN_STREAMS);
        log.info("Daily transaction tables: {}", signatureTableNames);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableNames, TableRoleInCollection.DailyTransactionStream, inactive);
    }

    private AggDailyActivityConfig getSparkConfig() {
        // using txn type as streamId
        String analyticStreamId = ProductType.Analytic.name();
        String spendingStreamId = ProductType.Spending.name();

        AggDailyActivityConfig config = new AggDailyActivityConfig();

        config.streamDateAttrs.put(analyticStreamId, InterfaceName.TransactionTime.name());
        config.streamDateAttrs.put(spendingStreamId, InterfaceName.TransactionTime.name());

        List<StreamAttributeDeriver> derivers = getDerivers();
        config.attrDeriverMap.put(analyticStreamId, derivers);
        config.attrDeriverMap.put(spendingStreamId, derivers);

        List<String> additionalAttrs = getAdditionalAttrs();
        config.additionalDimAttrMap.put(analyticStreamId, additionalAttrs);
        config.additionalDimAttrMap.put(spendingStreamId, additionalAttrs);

        config.currentEpochMilli = getLongValueFromContext(PA_TIMESTAMP);

        Map<String, Table> rawTransactionTables = getTablesFromMapCtxKey(customerSpaceStr, Raw_TXN_STREAMS);
        Table analyticStream = rawTransactionTables.get(ProductType.Analytic.name());
        Table spendingStream = rawTransactionTables.get(ProductType.Spending.name());
        log.info("Retrieved analytic stream {} and spending stream {}", analyticStream.getName(),
                spendingStream.getName());
        config.inputMetadata = getMetadata();
        config.setInput(Arrays.asList(
                analyticStream.partitionedToHdfsDataUnit(null,
                        Collections.singletonList(InterfaceName.StreamDateId.name())),
                spendingStream.partitionedToHdfsDataUnit(null,
                        Collections.singletonList(InterfaceName.StreamDateId.name()))));
        return config;
    }

    private ActivityStoreSparkIOMetadata getMetadata() {
        ActivityStoreSparkIOMetadata metadataWrapper = new ActivityStoreSparkIOMetadata();
        Map<String, ActivityStoreSparkIOMetadata.Details> metadata = new HashMap<>();
        metadata.put(ProductType.Analytic.name(), getIdxDetils(0));
        metadata.put(ProductType.Spending.name(), getIdxDetils(1));
        metadataWrapper.setMetadata(metadata);
        return metadataWrapper;
    }

    private ActivityStoreSparkIOMetadata.Details getIdxDetils(int idx) {
        ActivityStoreSparkIOMetadata.Details details = new ActivityStoreSparkIOMetadata.Details();
        details.setStartIdx(idx);
        return details;
    }

    private List<StreamAttributeDeriver> getDerivers() {
        return Arrays.asList( //
                constructSumDeriver(InterfaceName.Amount.name()), //
                constructSumDeriver(InterfaceName.Quantity.name()), //
                constructSumDeriver(InterfaceName.Cost.name()) //
        );
    }

    private StreamAttributeDeriver constructSumDeriver(String attrName) {
        StreamAttributeDeriver deriver = new StreamAttributeDeriver();
        deriver.setSourceAttributes(Collections.singletonList(attrName));
        deriver.setTargetAttribute(attrName);
        deriver.setCalculation(StreamAttributeDeriver.Calculation.SUM);
        return deriver;
    }

    private List<String> getAdditionalAttrs() {
        return Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ContactId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.ProductType.name()
        );
    }
}
