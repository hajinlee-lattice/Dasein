package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.period.PeriodBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AttrHasPurchasedFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.MetricsMarginAgg;
import com.latticeengines.dataflow.runtime.cascading.propdata.MetricsShareOfWalletFunc;
import com.latticeengines.dataflow.runtime.cascading.propdata.MetricsSpendChangeFunc;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ActivityMetricsCuratorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

@Component(ActivityMetricsCurateFlow.BEAN_NAME)
public class ActivityMetricsCurateFlow extends ConfigurableFlowBase<ActivityMetricsCuratorConfig> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ActivityMetricsCurateFlow.class);

    public static final String BEAN_NAME = "activityMetricsCurateFlow";

    private ActivityMetricsCuratorConfig config;
    private TimeFilterTranslator timeFilterTranslator;
    private Map<String, PeriodBuilder> periodBuilders;
    private Map<String, List<ActivityMetrics>> periodMetrics;
    private Map<String, Node> periodTables;
    private Map<InterfaceName, Class<?>> metricsClasses;

    /*
     * For PurchaseHistory, groupByFields = (AccountId, ProductId)
     */
    @Override
    public Node construct(TransformationFlowParameters parameters) {

        config = getTransformerConfig(parameters);

        List<Node> periodTableList = new ArrayList<>();
        for (int i = 0; i < parameters.getBaseTables().size(); i++) {
            periodTableList.add(addSource(parameters.getBaseTables().get(i)));
        }

        init();
        validateMetrics();
        validatePeriodTables(periodTableList);

        Node base = getBaseNode();

        List<Node> toJoin = new ArrayList<>();
        for (Map.Entry<String, List<ActivityMetrics>> ent : periodMetrics.entrySet()) {
            Node periodTable = periodTables.get(ent.getKey());
            for (ActivityMetrics metrics : ent.getValue()) {
                switch (metrics.getMetrics()) {
                case Margin:
                    toJoin.add(margin(periodTable, metrics));
                    break;
                case ShareOfWallet:
                    toJoin.add(shareOfWallet(periodTable, metrics));
                    break;
                case SpendChange:
                    toJoin.add(spendChange(base, periodTable, metrics));
                    break;
                case TotalSpendOvertime:
                    toJoin.add(totalSpendOvertime(periodTable, metrics));
                    break;
                case AvgSpendOvertime:
                    toJoin.add(avgSpendOvertime(periodTable, metrics));
                    break;
                case HasPurchased:
                    toJoin.add(hasPurchased(periodTable, metrics));
                    break;
                default:
                    throw new UnsupportedOperationException(metrics.getMetrics() + " metrics is not supported");
                }
            }
        }

        return join(base, toJoin);
    }

    private void init() {
        periodBuilders = new HashMap<>();
        config.getPeriodStrategies().forEach(strategy -> {
            periodBuilders.put(strategy.getName(), PeriodBuilderFactory.build(strategy));
        });
        timeFilterTranslator = new TimeFilterTranslator(config.getPeriodStrategies(), config.getMaxTxnDate());
        metricsClasses = new HashMap<>();
        metricsClasses.put(InterfaceName.Margin, Integer.class);
        metricsClasses.put(InterfaceName.ShareOfWallet, Integer.class);
        metricsClasses.put(InterfaceName.SpendChange, Integer.class);
        metricsClasses.put(InterfaceName.TotalSpendOvertime, Double.class);
        metricsClasses.put(InterfaceName.AvgSpendOvertime, Double.class);
        metricsClasses.put(InterfaceName.HasPurchased, Boolean.class);
    }

    private void validateMetrics() {
        periodMetrics = new HashMap<>();
        for (ActivityMetrics metrics : config.getMetrics()) {
            String period = metrics.getPeriodsConfig().get(0).getPeriod();
            if (!periodBuilders.containsKey(period)) {
                throw new RuntimeException("ActivityMetrics " + JsonUtils.serialize(metrics) + " has invalid period");
            }
            metrics.getPeriodsConfig().forEach(ent -> {
                if (!ent.getPeriod().equals(period)) {
                    throw new RuntimeException(
                            "ActivityMetrics in ActivityMetricsCuratorConfig should use same period");
                }
            });
            if (!periodMetrics.containsKey(period)) {
                periodMetrics.put(period, new ArrayList<>());
            }
            periodMetrics.get(period).add(metrics);
            switch (metrics.getMetrics()) {
            case Margin:
                if (metrics.getPeriodsConfig().get(0).getRelation() != ComparisonType.WITHIN) {
                    throw new UnsupportedOperationException("Margin metrics only support WITHIN comparison type");
                }
                break;
            case ShareOfWallet:
                if (metrics.getPeriodsConfig().get(0).getRelation() != ComparisonType.WITHIN) {
                    throw new UnsupportedOperationException(
                            "ShareOfWallet metrics only support WITHIN comparison type");
                }
                break;
            case SpendChange:
                if (metrics.getPeriodsConfig().size() != 2) {
                    throw new RuntimeException("SpendChange metrics should have 2 period config");
                }
                if (!(metrics.getPeriodsConfig().get(0).getRelation() == ComparisonType.WITHIN
                        && metrics.getPeriodsConfig().get(1).getRelation() == ComparisonType.BETWEEN)
                        || (metrics.getPeriodsConfig().get(0).getRelation() == ComparisonType.BETWEEN
                                && metrics.getPeriodsConfig().get(1).getRelation() == ComparisonType.WITHIN)) {
                    throw new UnsupportedOperationException(
                            "SpendChange metrics should have one comparison type as WITHIN and the other one as BETWEEN");
                }
                break;
            case TotalSpendOvertime:
                if (metrics.getPeriodsConfig().get(0).getRelation() != ComparisonType.WITHIN) {
                    throw new UnsupportedOperationException(
                            "TotalSpendOvertime metrics only support WITHIN comparison type");
                }
                break;
            case AvgSpendOvertime:
                if (metrics.getPeriodsConfig().get(0).getRelation() != ComparisonType.WITHIN) {
                    throw new UnsupportedOperationException(
                            "AvgSpendOvertime metrics only support WITHIN comparison type");
                }
                break;
            case HasPurchased:
                if (metrics.getPeriodsConfig().get(0).getRelation() != ComparisonType.EVER) {
                    throw new UnsupportedOperationException("HasPurchased metrics only support EVER comparison type");
                }
                break;
            default:
                throw new UnsupportedOperationException(metrics.getMetrics() + " metrics is not supported");
            }
        }
    }

    private void validatePeriodTables(List<Node> periodTableList) {
        periodTables = new HashMap<>();
        for (Node periodTable : periodTableList) {
            Extract extract = periodTable.getSourceSchema().getExtracts().get(0);
            Iterator<GenericRecord> recordIterator = AvroUtils.iterator(periodTable.getHadoopConfig(),
                    extract.getPath());
            if (recordIterator.hasNext()) {
                GenericRecord record = recordIterator.next();
                if (periodMetrics.containsKey(String.valueOf(record.get(InterfaceName.PeriodName.name())))) {
                    periodTables.put(String.valueOf(record.get(InterfaceName.PeriodName.name())), periodTable);
                }
            }
        }
        if (periodTables.size() != periodMetrics.size()) {
            throw new RuntimeException("Period tables are not enough");
        }
    }

    private Node getBaseNode() {
        Node base = periodTables.values().iterator().next();
        base = base.retain(new FieldList(config.getGroupByFields()))
                .groupByAndLimit(new FieldList(config.getGroupByFields()), 1).renamePipe("_base_node_");
        return base;
    }

    private Node filterPeriod(Node node, TimeFilter timeFilter) {
        PeriodBuilder periodBuilder = periodBuilders.get(timeFilter.getPeriod());
        List<Object> translatedTxnDateRange = timeFilterTranslator.translate(timeFilter).getValues();
        String minTxnDate = translatedTxnDateRange.get(0).toString();
        String maxTxnDate = translatedTxnDateRange.get(1).toString();
        int minPeriodId = periodBuilder.toPeriodId(minTxnDate);
        int maxPeriodId = periodBuilder.toPeriodId(maxTxnDate);
        node = node.filter(
                String.format("%s != null && %s >= %d && %s <= %d", InterfaceName.PeriodId, InterfaceName.PeriodId,
                        minPeriodId, InterfaceName.PeriodId, maxPeriodId),
                new FieldList(InterfaceName.PeriodId.name()));
        return node;
    }


    @SuppressWarnings("rawtypes")
    private Node margin(Node node, ActivityMetrics metrics) {
        node = filterPeriod(node, metrics.getPeriodsConfig().get(0));

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(metrics.getFullMetricsName());

        Aggregator agg = new MetricsMarginAgg(new Fields(retainFields.toArray(new String[retainFields.size()])),
                metrics.getFullMetricsName());
        List<FieldMetadata> fms = prepareFms(node, metrics, true);
        node = node.groupByAndAggregate(new FieldList(config.getGroupByFields()), agg, fms)
                .retain(new FieldList(retainFields));
        return node.renamePipe("_margin_node_");
    }

    private Node shareOfWallet(Node node, ActivityMetrics metrics) {
        node = filterPeriod(node, metrics.getPeriodsConfig().get(0));

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(metrics.getFullMetricsName());

        List<Aggregation> accountProductSpendAgg = Collections.singletonList(
                new Aggregation(InterfaceName.TotalAmount.name(), "_APSpend_", AggregationType.SUM));
        Node apSpend = node.groupBy(new FieldList(InterfaceName.AccountId.name(), InterfaceName.ProductId.name(),
                InterfaceName.SpendAnalyticsSegment.name()), accountProductSpendAgg).renamePipe("_apspend_node_");

        List<Aggregation> accountTotalSpendAgg = Collections.singletonList(
                new Aggregation("_APSpend_", "_ATSpend_", AggregationType.SUM));
        Node atSpend = apSpend.groupBy(new FieldList(InterfaceName.AccountId.name()), accountTotalSpendAgg)
                .renamePipe("_atspend_node_");

        List<Aggregation> segmentProductSpendAgg = Collections
                .singletonList(new Aggregation("_APSpend_", "_SPSpend_", AggregationType.SUM));
        Node spSpend = apSpend.groupBy(new FieldList(InterfaceName.SpendAnalyticsSegment.name(), InterfaceName.ProductId.name()),
                segmentProductSpendAgg).renamePipe("_spspend_node_");

        List<Aggregation> semgentTotalSpendAgg = Collections
                .singletonList(new Aggregation("_SPSpend_", "_STSpend_", AggregationType.SUM));
        Node stSpend = spSpend.groupBy(new FieldList(InterfaceName.SpendAnalyticsSegment.name()), semgentTotalSpendAgg).renamePipe("_stspend_node_");

        node = apSpend.join(new FieldList(InterfaceName.AccountId.name()), atSpend,
                new FieldList(InterfaceName.AccountId.name()), JoinType.LEFT)
                .join(new FieldList(InterfaceName.SpendAnalyticsSegment.name(), InterfaceName.ProductId.name()),
                        spSpend,
                        new FieldList(InterfaceName.SpendAnalyticsSegment.name(), InterfaceName.ProductId.name()),
                        JoinType.LEFT)
                .join(new FieldList(InterfaceName.SpendAnalyticsSegment.name()), stSpend,
                        new FieldList(InterfaceName.SpendAnalyticsSegment.name()), JoinType.LEFT);

        node = node.apply(
                new MetricsShareOfWalletFunc(metrics.getFullMetricsName(), "_APSpend_", "_ATSpend_", "_SPSpend_",
                        "_STSpend_"),
                new FieldList(node.getFieldNames()), prepareFms(node, metrics, false).get(0))
                .retain(new FieldList(retainFields));
        return node.renamePipe("_shareofwallet_node_");
    }

    private Node spendChange(Node base, Node node, ActivityMetrics metrics) {
        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(metrics.getFullMetricsName());

        TimeFilter withinFilter = metrics.getPeriodsConfig().get(0).getRelation() == ComparisonType.WITHIN
                ? metrics.getPeriodsConfig().get(0)
                : metrics.getPeriodsConfig().get(1);
        TimeFilter betweenFilter = metrics.getPeriodsConfig().get(0).getRelation() == ComparisonType.BETWEEN
                ? metrics.getPeriodsConfig().get(0)
                : metrics.getPeriodsConfig().get(1);
        Node last = filterPeriod(node, withinFilter).renamePipe("_LastPeriod_");
        Node previous = filterPeriod(node, betweenFilter).renamePipe("_PreviousPeriod_");
        List<Aggregation> lastAvgAgg = Collections.singletonList(
                new Aggregation(InterfaceName.TotalAmount.name(), "_LastPeriodAvgSpend_", AggregationType.AVG));
        List<Aggregation> previousAvgAgg = Collections.singletonList(
                new Aggregation(InterfaceName.TotalAmount.name(), "_PreviousPeriodAvgSpend_", AggregationType.AVG));
        last = last.groupBy(new FieldList(config.getGroupByFields()), lastAvgAgg);
        previous = previous.groupBy(new FieldList(config.getGroupByFields()), previousAvgAgg);

        FieldList joinFields = new FieldList(config.getGroupByFields());
        List<FieldList> joinFieldsList = new ArrayList<FieldList>(Collections.nCopies(2, joinFields));
        node = base.coGroup(joinFields, Arrays.asList(new Node[] { last, previous }), joinFieldsList, JoinType.OUTER)
                .apply(new MetricsSpendChangeFunc(metrics.getFullMetricsName(), "_LastPeriodAvgSpend_",
                        "_PreviousPeriodAvgSpend_"), new FieldList("_LastPeriodAvgSpend_", "_PreviousPeriodAvgSpend_"),
                        prepareFms(node, metrics, false).get(0))
                .retain(new FieldList(retainFields));
        return node.renamePipe("_spendchange_node_");
    }

    private Node totalSpendOvertime(Node node, ActivityMetrics metrics) {
        node = filterPeriod(node, metrics.getPeriodsConfig().get(0));

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(metrics.getFullMetricsName());

        List<Aggregation> totalSpendAgg = Collections.singletonList(new Aggregation(InterfaceName.TotalAmount.name(),
                metrics.getFullMetricsName(), AggregationType.SUM));
        node = node.groupBy(new FieldList(config.getGroupByFields()), totalSpendAgg)
                .apply(String.format("%s != null && %s == 0.0 ? null : %s", metrics.getFullMetricsName(),
                        metrics.getFullMetricsName(), metrics.getFullMetricsName()),
                        new FieldList(metrics.getFullMetricsName()), prepareFms(node, metrics, false).get(0))
                .retain(new FieldList(retainFields));
        return node.renamePipe("_totalspendovertime_node_");
    }

    private Node avgSpendOvertime(Node node, ActivityMetrics metrics) {
        node = filterPeriod(node, metrics.getPeriodsConfig().get(0));

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(metrics.getFullMetricsName());

        List<Aggregation> avgSpendAgg = Collections.singletonList(
                new Aggregation(InterfaceName.TotalAmount.name(), metrics.getFullMetricsName(), AggregationType.AVG));
        node = node
                .groupBy(new FieldList(config.getGroupByFields()), avgSpendAgg)
                .apply(String.format("%s != null && %s == 0.0 ? null : %s", metrics.getFullMetricsName(),
                        metrics.getFullMetricsName(), metrics.getFullMetricsName()),
                        new FieldList(metrics.getFullMetricsName()), prepareFms(node, metrics, false).get(0))
                .retain(new FieldList(retainFields));
        return node.renamePipe("_avgspendovertime_node_");
    }

    private Node hasPurchased(Node node, ActivityMetrics metrics) {
        // Period range is ever, no need to filter by period
        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(metrics.getFullMetricsName());

        List<Aggregation> totalSpendAgg = Collections.singletonList(
                new Aggregation(InterfaceName.TotalAmount.name(), InterfaceName.TotalAmount.name(),
                        AggregationType.SUM));
        node = node.groupBy(new FieldList(config.getGroupByFields()), totalSpendAgg)
                .apply(new AttrHasPurchasedFunction(metrics.getFullMetricsName()),
                        new FieldList(InterfaceName.TotalAmount.name()), prepareFms(node, metrics, false).get(0))
                .retain(new FieldList(retainFields));
        return node.renamePipe("_haspurchased_node_");
    }

    private Node join(Node base, List<Node> toJoin) {
        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        for (ActivityMetrics metrics : config.getMetrics()) {
            retainFields.add(metrics.getFullMetricsName());
        }
        FieldList joinFields = new FieldList(config.getGroupByFields());
        List<FieldList> joinFieldsList = new ArrayList<FieldList>(Collections.nCopies(toJoin.size(), joinFields));
        return base.coGroup(joinFields, toJoin, joinFieldsList, JoinType.OUTER).retain(new FieldList(retainFields));
    }

    private List<FieldMetadata> prepareFms(Node source, ActivityMetrics metrics, boolean includeGroupBy) {
        List<FieldMetadata> fms = new ArrayList<>();
        if (includeGroupBy) {
            config.getGroupByFields().forEach(field -> {
                fms.add(source.getSchema(field));
            });
        }
        fms.add(new FieldMetadata(metrics.getFullMetricsName(), metricsClasses.get(metrics.getMetrics())));
        return fms;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ActivityMetricsCuratorConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return ActivityMetricsCurateFlow.BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.ACTIVITY_METRICS_CURATOR;
    }
}
