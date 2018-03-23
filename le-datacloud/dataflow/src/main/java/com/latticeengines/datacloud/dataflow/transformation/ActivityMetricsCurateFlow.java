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
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ActivityMetricsCuratorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.period.PeriodBuilder;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
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
    private Map<String, PeriodStrategy> periodStrategies;  //period name -> period strategy
    private Map<String, PeriodBuilder> periodBuilders;  //period name -> period builder
    private Map<String, List<ActivityMetrics>> periodMetrics;   // period name -> metrics list
    private Map<String, Node> periodTables; // period name -> period table
    private Map<InterfaceName, Class<?>> metricsClasses;

    /*
     * For PurchaseHistory, groupByFields = (AccountId, ProductId)
     */
    @Override
    public Node construct(TransformationFlowParameters parameters) {

        config = getTransformerConfig(parameters);

        // TODO: Need to fix after merging business calendar period builder to period service
        // Not always pick all the period tables. If some of them are not used, cascading will fail
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
                    toJoin.add(totalSpendOvertime(base, periodTable, metrics));
                    break;
                case AvgSpendOvertime:
                    toJoin.add(avgSpendOvertime(base, periodTable, metrics));
                    break;
                case HasPurchased:
                    toJoin.add(hasPurchased(periodTable, metrics));
                    break;
                default:
                    throw new UnsupportedOperationException(metrics.getMetrics() + " metrics is not supported");
                }
            }
        }

        Node toReturn = join(base, toJoin);
        toReturn = assignCompositeKey(toReturn);
        return toReturn;
    }

    private void init() {
        periodBuilders = new HashMap<>();
        periodStrategies = new HashMap<>();
        config.getPeriodStrategies().forEach(strategy -> {
            periodBuilders.put(strategy.getName(), PeriodBuilderFactory.build(strategy));
            periodStrategies.put(strategy.getName(), strategy);
        });
        timeFilterTranslator = new TimeFilterTranslator(config.getPeriodStrategies(), config.getCurrentDate());
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
            String hdfsPath = extract.getPath();
            if (!hdfsPath.endsWith("*.avro")) {
                if (hdfsPath.endsWith("/")) {
                    hdfsPath += "*.avro";
                } else {
                    hdfsPath += "/*.avro";
                }
            }
            Iterator<GenericRecord> recordIterator = AvroUtils.iterator(periodTable.getHadoopConfig(), hdfsPath);
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

    private Node filterPeriod(Node node, TimeFilter timeFilter, List<Integer> periodIdRange) {
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
        if (periodIdRange != null) {
            periodIdRange.add(minPeriodId);
            periodIdRange.add(maxPeriodId);
        }
        return node;
    }


    @SuppressWarnings("rawtypes")
    private Node margin(Node node, ActivityMetrics metrics) {
        node = filterPeriod(node, metrics.getPeriodsConfig().get(0), null);

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(ActivityMetricsUtils.getNameWithPeriod(metrics));

        Aggregator agg = new MetricsMarginAgg(new Fields(retainFields.toArray(new String[retainFields.size()])),
                ActivityMetricsUtils.getNameWithPeriod(metrics));
        List<FieldMetadata> fms = prepareFms(node, metrics, true);
        node = node.groupByAndAggregate(new FieldList(config.getGroupByFields()), agg, fms)
                .retain(new FieldList(retainFields));
        return node.renamePipe("_margin_node_");
    }

    private Node shareOfWallet(Node node, ActivityMetrics metrics) {
        node = filterPeriod(node, metrics.getPeriodsConfig().get(0), null)
                .filter(String.format("%s != null", InterfaceName.SpendAnalyticsSegment.name()),
                        new FieldList(InterfaceName.SpendAnalyticsSegment.name()));

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(ActivityMetricsUtils.getNameWithPeriod(metrics));

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
                new MetricsShareOfWalletFunc(ActivityMetricsUtils.getNameWithPeriod(metrics), "_APSpend_", "_ATSpend_",
                        "_SPSpend_", "_STSpend_"),
                new FieldList(node.getFieldNames()), prepareFms(node, metrics, false).get(0))
                .retain(new FieldList(retainFields));
        return node.renamePipe("_shareofwallet_node_");
    }

    private Node spendChange(Node base, Node node, ActivityMetrics metrics) {
        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(ActivityMetricsUtils.getNameWithPeriod(metrics));

        TimeFilter withinFilter = metrics.getPeriodsConfig().get(0).getRelation() == ComparisonType.WITHIN
                ? metrics.getPeriodsConfig().get(0)
                : metrics.getPeriodsConfig().get(1);
        List<Integer> lastPeriodIdRange = new ArrayList<>();
        Node last = filterPeriod(node, withinFilter, lastPeriodIdRange).renamePipe("_LastPeriod_");
        List<Aggregation> lastTotalAgg = Collections.singletonList(
                new Aggregation(InterfaceName.TotalAmount.name(), "_LastTotalSpend_", AggregationType.SUM));
        last = last.groupBy(new FieldList(config.getGroupByFields()), lastTotalAgg)
                .apply(String.format("%s == null || %s == 0 ? Double.valueOf(0) : Double.valueOf(%s/%d)",
                        "_LastTotalSpend_", "_LastTotalSpend_", "_LastTotalSpend_",
                        (lastPeriodIdRange.get(1) - lastPeriodIdRange.get(0) + 1)),
                new FieldList("_LastTotalSpend_"), new FieldMetadata("_LastAvgSpend_", Double.class));

        TimeFilter betweenFilter = metrics.getPeriodsConfig().get(0).getRelation() == ComparisonType.BETWEEN
                ? metrics.getPeriodsConfig().get(0)
                : metrics.getPeriodsConfig().get(1);
        List<Integer> priorPeriodIdRange = new ArrayList<>();
        Node prior = filterPeriod(node, betweenFilter, priorPeriodIdRange).renamePipe("_PriorPeriod_");
        List<Aggregation> priorTotalAgg = Collections.singletonList(
                new Aggregation(InterfaceName.TotalAmount.name(), "_PriorTotalSpend_", AggregationType.SUM));
        prior = prior.groupBy(new FieldList(config.getGroupByFields()), priorTotalAgg) //
                .apply(String.format("%s == null || %s == 0 ? Double.valueOf(0) : Double.valueOf(%s/%d)",
                        "_PriorTotalSpend_", "_PriorTotalSpend_", "_PriorTotalSpend_",
                        (priorPeriodIdRange.get(1) - priorPeriodIdRange.get(0) + 1)),
                        new FieldList("_PriorTotalSpend_"), new FieldMetadata("_PriorAvgSpend_", Double.class));

        FieldList joinFields = new FieldList(config.getGroupByFields());
        List<FieldList> joinFieldsList = new ArrayList<FieldList>(Collections.nCopies(2, joinFields));
        node = base.coGroup(joinFields, Arrays.asList(new Node[] { last, prior }), joinFieldsList, JoinType.OUTER)
                .apply(new MetricsSpendChangeFunc(ActivityMetricsUtils.getNameWithPeriod(metrics),
                        "_LastAvgSpend_", "_PriorAvgSpend_"), new FieldList("_LastAvgSpend_", "_PriorAvgSpend_"),
                        prepareFms(node, metrics, false).get(0))
                .retain(new FieldList(retainFields));
        return node.renamePipe("_spendchange_node_");
    }

    private Node totalSpendOvertime(Node base, Node node, ActivityMetrics metrics) {
        node = filterPeriod(node, metrics.getPeriodsConfig().get(0), null);

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(ActivityMetricsUtils.getNameWithPeriod(metrics));

        List<Aggregation> totalSpendAgg = Collections.singletonList(new Aggregation(InterfaceName.TotalAmount.name(),
                ActivityMetricsUtils.getNameWithPeriod(metrics), AggregationType.SUM));
        node = node.groupBy(new FieldList(config.getGroupByFields()), totalSpendAgg);
        node = base
                .join(new FieldList(config.getGroupByFields()), node, new FieldList(config.getGroupByFields()),
                        JoinType.LEFT)//
                .apply(String.format("%s == null  ? Double.valueOf(0) : %s",
                        ActivityMetricsUtils.getNameWithPeriod(metrics),
                        ActivityMetricsUtils.getNameWithPeriod(metrics)),
                        new FieldList(ActivityMetricsUtils.getNameWithPeriod(metrics)),
                        prepareFms(node, metrics, false).get(0))
                .retain(new FieldList(retainFields));
        return node.renamePipe("_totalspendovertime_node_");
    }

    private Node avgSpendOvertime(Node base, Node node, ActivityMetrics metrics) {
        List<Integer> periodIdRange = new ArrayList<>();
        node = filterPeriod(node, metrics.getPeriodsConfig().get(0), periodIdRange);

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(ActivityMetricsUtils.getNameWithPeriod(metrics));

        List<Aggregation> totalSpendAgg = Collections
                .singletonList(new Aggregation(InterfaceName.TotalAmount.name(), "_TotalSpend_", AggregationType.SUM));
        node = node.groupBy(new FieldList(config.getGroupByFields()), totalSpendAgg) //
                .apply(String.format("%s == null ? Double.valueOf(0) : Double.valueOf(%s/%d)", "_TotalSpend_",
                        "_TotalSpend_", (periodIdRange.get(1) - periodIdRange.get(0) + 1)),
                        new FieldList("_TotalSpend_"),
                        prepareFms(node, metrics, false).get(0));

        node = base
                .join(new FieldList(config.getGroupByFields()), node, new FieldList(config.getGroupByFields()),
                        JoinType.LEFT)//
                .apply(String.format("%s == null  ? Double.valueOf(0) : %s",
                        ActivityMetricsUtils.getNameWithPeriod(metrics),
                        ActivityMetricsUtils.getNameWithPeriod(metrics)),
                        new FieldList(ActivityMetricsUtils.getNameWithPeriod(metrics)),
                        prepareFms(node, metrics, false).get(0))
                .retain(new FieldList(retainFields));
        return node.renamePipe("_avgspendovertime_node_");
    }

    private Node hasPurchased(Node node, ActivityMetrics metrics) {
        // Period range is ever, no need to filter by period
        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(ActivityMetricsUtils.getNameWithPeriod(metrics));

        List<Aggregation> totalSpendAgg = Collections.singletonList(
                new Aggregation(InterfaceName.TotalAmount.name(), InterfaceName.TotalAmount.name(),
                        AggregationType.SUM));
        node = node.groupBy(new FieldList(config.getGroupByFields()), totalSpendAgg)
                .apply(new AttrHasPurchasedFunction(ActivityMetricsUtils.getNameWithPeriod(metrics), false),
                        new FieldList(InterfaceName.TotalAmount.name()), prepareFms(node, metrics, false).get(0))
                .retain(new FieldList(retainFields));
        return node.renamePipe("_haspurchased_node_");
    }

    private Node join(Node base, List<Node> toJoin) {
        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        for (ActivityMetrics metrics : config.getMetrics()) {
            retainFields.add(ActivityMetricsUtils.getNameWithPeriod(metrics));
        }
        FieldList joinFields = new FieldList(config.getGroupByFields());
        List<FieldList> joinFieldsList = new ArrayList<FieldList>(Collections.nCopies(toJoin.size(), joinFields));
        return base.coGroup(joinFields, toJoin, joinFieldsList, JoinType.OUTER).retain(new FieldList(retainFields));
    }

    private Node assignCompositeKey(Node node) {
        node = node.apply(
                String.format("%s + \"_\" + %s", InterfaceName.AccountId.name(), InterfaceName.ProductId.name()),
                new FieldList(config.getGroupByFields()),
                new FieldMetadata(InterfaceName.__Composite_Key__.name(), String.class));
        return node;
    }

    private List<FieldMetadata> prepareFms(Node source, ActivityMetrics metrics, boolean includeGroupBy) {
        List<FieldMetadata> fms = new ArrayList<>();
        if (includeGroupBy) {
            config.getGroupByFields().forEach(field -> {
                fms.add(source.getSchema(field));
            });
        }
        fms.add(new FieldMetadata(ActivityMetricsUtils.getNameWithPeriod(metrics),
                metricsClasses.get(metrics.getMetrics())));
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
