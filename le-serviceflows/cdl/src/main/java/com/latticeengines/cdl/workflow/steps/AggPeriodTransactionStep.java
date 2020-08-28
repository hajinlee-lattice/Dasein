package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

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
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.DailyStoreToPeriodStoresJobConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.spark.exposed.job.cdl.PeriodStoresGenerator;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class AggPeriodTransactionStep extends BaseProcessAnalyzeSparkStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AggPeriodTransactionStep.class);

    private static final String PERIOD_TXN_PREFIX_FMT = "PERIOD_TXN_%s_%s"; // type, period

    @Inject
    private PeriodProxy periodProxy;

    private BusinessCalendar businessCalendar;

    private List<String> periodStrategies;

    private String evaluationDate;

    @Override
    public void execute() {
        bootstrap();
        Map<String, Table> periodStreams = getTablesFromMapCtxKey(customerSpaceStr, PERIOD_TXN_STREAMS);
        if (isShortcutMode(periodStreams)) {
            Map<String, String> signatureTableNames = periodStreams.entrySet().stream().map(entry -> {
                        String productType = entry.getKey();
                        Table table = entry.getValue();
                        return Pair.of(productType, table.getName());
            }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            log.info("Found period transaction streams {}. Going through shortcut mode.", signatureTableNames);
            dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableNames,
                    TableRoleInCollection.PeriodTransactionStream, inactive);
            return;
        }
        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, getSparkConfig());
        processOutputMetadata(result);
    }

    private void processOutputMetadata(SparkJobResult result) {
        log.info("Output metadata: {}", result.getOutput()); // type -> [periods]
        List<HdfsDataUnit> outputs = result.getTargets();
        Map<String, ActivityStoreSparkIOMetadata.Details> outputMetadata = JsonUtils
                .deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class).getMetadata();
        Map<String, Table> periodStores = new HashMap<>(); // type_period -> table
        outputMetadata.forEach((productType, details) -> {
            for (int offset = 0; offset < details.getLabels().size(); offset++) {
                String period = details.getLabels().get(offset);
                String prefix = String.format(PERIOD_TXN_PREFIX_FMT, productType, period);
                String tableName = TableUtils.getFullTableName(prefix,
                        HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
                Table table = dirToTable(tableName, outputs.get(details.getStartIdx() + offset));
                metadataProxy.createTable(customerSpaceStr, tableName, table);
                periodStores.put(prefix, table);
            }
        });
        savePeriodTxnTables(periodStores);
    }

    private void savePeriodTxnTables(Map<String, Table> periodStores) {
        Map<String, String> signatureTableNames = exportToS3AndAddToContext(periodStores, PERIOD_TXN_STREAMS);
        log.info("Period transaction tables: {}", signatureTableNames);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableNames,
                TableRoleInCollection.PeriodTransactionStream, inactive);
    }

    private DailyStoreToPeriodStoresJobConfig getSparkConfig() {
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.streams = constructTransactionStreams();
        config.evaluationDate = evaluationDate;
        config.businessCalendar = businessCalendar;
        config.inputMetadata = constructInputMetadata();

        Map<String, Table> dailyTransactionTables = getTablesFromMapCtxKey(customerSpaceStr, DAILY_TXN_STREAMS);
        Table analyticDailyTable = dailyTransactionTables.get(ProductType.Analytic.name());
        Table spendingDailyTable = dailyTransactionTables.get(ProductType.Spending.name());
        config.setInput(Arrays.asList(
                analyticDailyTable.partitionedToHdfsDataUnit(null,
                        Collections.singletonList(InterfaceName.StreamDateId.name())),
                spendingDailyTable.partitionedToHdfsDataUnit(null,
                        Collections.singletonList(InterfaceName.StreamDateId.name()))));
        return config;
    }

    private ActivityStoreSparkIOMetadata constructInputMetadata() {
        ActivityStoreSparkIOMetadata metadataWrapper = new ActivityStoreSparkIOMetadata();
        Map<String, ActivityStoreSparkIOMetadata.Details> inputMetadata = new HashMap<>();
        ActivityStoreSparkIOMetadata.Details analytic = new ActivityStoreSparkIOMetadata.Details();
        analytic.setStartIdx(0);
        analytic.setLabels(periodStrategies);
        ActivityStoreSparkIOMetadata.Details spending = new ActivityStoreSparkIOMetadata.Details();
        spending.setStartIdx(1);
        spending.setLabels(periodStrategies);
        inputMetadata.put(ProductType.Analytic.name(), analytic);
        inputMetadata.put(ProductType.Spending.name(), spending);
        metadataWrapper.setMetadata(inputMetadata);
        return metadataWrapper;
    }

    private List<AtlasStream> constructTransactionStreams() {
        return Arrays.asList( //
                constructStream(ProductType.Analytic.name()), //
                constructStream(ProductType.Spending.name()) //
        );
    }

    private AtlasStream constructStream(String type) {
        AtlasStream stream = new AtlasStream();
        stream.setStreamId(type);
        stream.setPeriods(periodStrategies);
        stream.setDimensions(Arrays.asList( //
                prepareDimension(InterfaceName.ProductId.name()), //
                prepareDimension(InterfaceName.TransactionType.name()), //
                prepareDimension(InterfaceName.ProductType.name()) //
        ));
        stream.setAggrEntities(Collections.singletonList(BusinessEntity.Contact.name()));
        stream.setAttributeDerivers(Arrays.asList( //
                constructSumDeriver(InterfaceName.Amount.name()), //
                constructSumDeriver(InterfaceName.Quantity.name()), //
                constructSumDeriver(InterfaceName.Cost.name()) //
        ));
        return stream;
    }

    private StreamDimension prepareDimension(String name) {
        StreamDimension dim = new StreamDimension();
        dim.setName(name);
        return dim;
    }

    private StreamAttributeDeriver constructSumDeriver(String attrName) {
        StreamAttributeDeriver deriver = new StreamAttributeDeriver();
        deriver.setSourceAttributes(Collections.singletonList(attrName));
        deriver.setTargetAttribute(attrName);
        deriver.setCalculation(StreamAttributeDeriver.Calculation.SUM);
        return deriver;
    }

    @Override
    protected void bootstrap() {
        super.bootstrap();
        businessCalendar = periodProxy.getBusinessCalendar(customerSpaceStr);
        periodStrategies = periodProxy.getPeriodNames(customerSpaceStr);
        evaluationDate = periodProxy.getEvaluationDate(customerSpaceStr);
    }
}
