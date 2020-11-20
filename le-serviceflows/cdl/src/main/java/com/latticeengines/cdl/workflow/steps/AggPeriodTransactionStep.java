package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.DailyStoreToPeriodStoresJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.PeriodTxnStreamPostAggregationConfig;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.spark.exposed.job.cdl.PeriodStoresGenerator;
import com.latticeengines.spark.exposed.job.cdl.PeriodTxnStreamPostAggregationJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class AggPeriodTransactionStep extends BaseProcessAnalyzeSparkStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AggPeriodTransactionStep.class);

    public static final String PERIOD_TXN_PREFIX_FMT = "PERIOD_TXN_%s_%s"; // type, period

    @Inject
    private PeriodProxy periodProxy;

    private BusinessCalendar businessCalendar;

    private String evaluationDate;

    private String rollingPeriod;

    @Override
    public void execute() {
        bootstrap();
        Map<String, Table> periodStreams = getTablesFromMapCtxKey(customerSpaceStr, PERIOD_TXN_STREAMS);
        if (tableExist(periodStreams) && tableInHdfs(periodStreams, true)) {
            Map<String, String> signatureTableNames = periodStreams.entrySet().stream().map(entry -> {
                String productType = entry.getKey();
                Table table = entry.getValue();
                return Pair.of(productType, table.getName());
            }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            log.info("Found period transaction streams {}. Going through shortcut mode.", signatureTableNames);
            log.info("Retaining transactions of product type: {}",
                    getListObjectFromContext(RETAIN_PRODUCT_TYPE, String.class));
            dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableNames,
                    TableRoleInCollection.PeriodTransactionStream, inactive);
            return;
        }

        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, getSparkConfig());
        Map<String, HdfsDataUnit> outputs = processOutputMetadata(result);
        String analyticRollingPeriodKey = String.format(PERIOD_TXN_PREFIX_FMT, ProductType.Analytic.name(),
                rollingPeriod);
        if (outputs.containsKey(analyticRollingPeriodKey)) {
            outputs.put(analyticRollingPeriodKey, fillMissingPeriods(outputs.get(analyticRollingPeriodKey)));
        }
        savePeriodTxnTables(outputs.entrySet().stream().map(entry -> {
            String prefix = entry.getKey();
            String tableName = NamingUtils.timestamp(prefix);
            Table table = dirToTable(tableName, entry.getValue());
            metadataProxy.createTable(customerSpaceStr, tableName, table);
            return Pair.of(prefix, table);
        }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight)));
    }

    private HdfsDataUnit fillMissingPeriods(HdfsDataUnit rawPeriodStream) {
        log.info("Filling period gaps for analytic rolling period transaction: {}", rollingPeriod);
        PeriodTxnStreamPostAggregationConfig config = new PeriodTxnStreamPostAggregationConfig();
        config.setInput(Collections.singletonList(rawPeriodStream));
        SparkJobResult result = runSparkJob(PeriodTxnStreamPostAggregationJob.class, config);
        return result.getTargets().get(0);
    }

    private Map<String, HdfsDataUnit> processOutputMetadata(SparkJobResult result) {
        log.info("Output metadata: {}", result.getOutput()); // type -> [periods]
        List<HdfsDataUnit> outputs = result.getTargets();
        Map<String, SparkIOMetadataWrapper.Partition> outputMetadata = JsonUtils
                .deserialize(result.getOutput(), SparkIOMetadataWrapper.class).getMetadata();
        Map<String, HdfsDataUnit> rawPeriodStreams = new HashMap<>(); // type_period -> table
        outputMetadata.forEach((productType, details) -> {
            for (int offset = 0; offset < details.getLabels().size(); offset++) {
                String period = details.getLabels().get(offset);
                String prefix = String.format(PERIOD_TXN_PREFIX_FMT, productType, period);
                rawPeriodStreams.put(prefix, outputs.get(details.getStartIdx() + offset));
            }
        });
        return rawPeriodStreams;
    }

    private void savePeriodTxnTables(Map<String, Table> periodStores) {
        Map<String, String> signatureTableNames = exportToS3AndAddToContext(periodStores, PERIOD_TXN_STREAMS);
        log.info("Period transaction tables: {}", signatureTableNames);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableNames,
                TableRoleInCollection.PeriodTransactionStream, inactive);
    }

    private DailyStoreToPeriodStoresJobConfig getSparkConfig() {
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        List<String> retainTypes = getListObjectFromContext(RETAIN_PRODUCT_TYPE, String.class);
        log.info("Retaining transactions of product type: {}", retainTypes);
        if (CollectionUtils.isEmpty(retainTypes)) {
            throw new IllegalStateException("No retain types found in context");
        }
        config.streams = constructTransactionStreams(retainTypes);
        config.evaluationDate = evaluationDate;
        config.businessCalendar = Collections.singletonMap(ProductType.Analytic.name(), businessCalendar);
        Map<String, Table> dailyTransactionTables = getTablesFromMapCtxKey(customerSpaceStr, DAILY_TXN_STREAMS);
        List<DataUnit> inputs = new ArrayList<>();
        SparkIOMetadataWrapper metadataWrapper = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> partitions = new HashMap<>();
        config.streams.forEach(stream -> {
            String type = stream.getStreamId();
            SparkIOMetadataWrapper.Partition partition = new SparkIOMetadataWrapper.Partition();
            partition.setStartIdx(inputs.size());
            partition.setLabels(stream.getPeriods());
            partitions.put(type, partition);
            Table table = dailyTransactionTables.get(type);
            inputs.add(table.partitionedToHdfsDataUnit(null,
                    Collections.singletonList(InterfaceName.StreamDateId.name())));
        });
        config.setInput(inputs);
        metadataWrapper.setMetadata(partitions);
        config.inputMetadata = metadataWrapper;
        config.repartition = true;
        return config;
    }

    private List<AtlasStream> constructTransactionStreams(List<String> retainTypes) {
        return retainTypes.stream().map(this::constructStream).collect(Collectors.toList());
    }

    private AtlasStream constructStream(String type) {
        AtlasStream stream = new AtlasStream();
        stream.setStreamId(type);
        stream.setPeriods(periodProxy.getPeriodNames(customerSpaceStr));
        stream.setDimensions(Arrays.asList( //
                prepareDimension(InterfaceName.ProductId.name()), //
                prepareDimension(InterfaceName.TransactionType.name()), //
                prepareDimension(InterfaceName.ProductType.name()) //
        ));
        stream.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));
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
        evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
        rollingPeriod = configuration.getApsRollingPeriod();
    }
}
