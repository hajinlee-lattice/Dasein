package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.dataflow.runtime.cascading.propdata.ActivityMetricsNullImputeFunc;
import com.latticeengines.dataflow.runtime.cascading.propdata.MetricsShareOfWalletFunc;
import com.latticeengines.dataflow.runtime.cascading.propdata.StandardActivityMetricsAgg;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ActivityMetricsCuratorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.period.PeriodBuilder;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

@Component(ActivityMetricsCurateFlow.BEAN_NAME)
public class ActivityMetricsCurateFlow extends ActivityMetricsBaseFlow<ActivityMetricsCuratorConfig> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ActivityMetricsCurateFlow.class);

    public static final String BEAN_NAME = "activityMetricsCurateFlow";

    private ActivityMetricsCuratorConfig config;
    private TimeFilterTranslator timeFilterTranslator;
    private Map<String, PeriodStrategy> periodStrategies;  //period name -> period strategy
    private Map<String, PeriodBuilder> periodBuilders;  //period name -> period builder
    private Map<String, List<ActivityMetrics>> periodMetrics;   // period name -> metrics list
    private Map<String, Node> periodTables; // period name -> period table

    private Node account = null, product = null;

    /*
     * For PurchaseHistory, groupByFields = (AccountId, ProductId)
     */
    @Override
    public Node construct(TransformationFlowParameters parameters) {

        config = getTransformerConfig(parameters);

        List<Node> periodTableList = new ArrayList<>();
        for (int i = 0; i < config.getPeriodTableCnt(); i++) {
            periodTableList.add(addSource(parameters.getBaseTables().get(i)));
        }
        if (shouldLoadAccount()) {
            account = addSource(parameters.getBaseTables().get(config.getPeriodTableCnt()));
        }
        if (shouldLoadProduct()) {
            product = addSource(parameters.getBaseTables().get(config.getPeriodTableCnt() + 1));
            product = product.filter(String.format("\"%s\".equalsIgnoreCase(%s)", ProductType.Analytic.name(),
                    InterfaceName.ProductType.name()), new FieldList(InterfaceName.ProductType.name()));
        }

        init();
        validateMetrics();
        validatePeriodTables(periodTableList);

        Node base = getBaseNode();

        List<Node> toJoin = new ArrayList<>();
        for (Map.Entry<String, List<ActivityMetrics>> ent : periodMetrics.entrySet()) {
            Node periodTable = periodTables.get(ent.getKey());
            toJoin.addAll(generateMetrics(periodTable, ent.getValue()));
        }

        Node toReturn = join(base, toJoin);
        toReturn = imputeNull(toReturn);
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
        prepareMetricsMetadata(config.getMetrics(), null);
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
        }
        ActivityMetricsUtils.isValidMetrics(config.getMetrics());
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
                // Read one row to identify which period table it is
                GenericRecord record = recordIterator.next();
                if (periodMetrics.containsKey(String.valueOf(record.get(InterfaceName.PeriodName.name())))) {
                    // Transaction table has both AnalyticProduct and SpendProduct. Should only leave with AnalyticProduct
                    if (config.getType() == ActivityType.PurchaseHistory) {
                        List<String> retainFields = new ArrayList<>(periodTable.getFieldNames());
                        periodTable = periodTable
                                .join(new FieldList(InterfaceName.ProductId.name()), product,
                                        new FieldList(InterfaceName.ProductId.name()), JoinType.INNER)
                                .retain(new FieldList(retainFields));
                    }
                    periodTables.put(String.valueOf(record.get(InterfaceName.PeriodName.name())), periodTable);
                }
            }
        }
        if (periodTables.size() != periodMetrics.size()) {
            throw new RuntimeException("Period tables are not enough");
        }
    }

    private Node getBaseNode() {
        Node base = null;
        if (config.getType() == ActivityType.PurchaseHistory && !config.isReduced()) {
            account = account.addColumnWithFixedValue("_DUMMY_", null, String.class);
            product = product.addColumnWithFixedValue("_DUMMY_", null, String.class);
            base = account.join(new FieldList("_DUMMY_"), product, new FieldList("_DUMMY_"), JoinType.INNER)
                    .retain(new FieldList(config.getGroupByFields()));
        } else {
            base = findSmallestPeriodTable();
            if (config.getType() == ActivityType.PurchaseHistory) {
                base = base.retain(new FieldList(InterfaceName.AccountId.name()))
                        .groupByAndLimit(new FieldList(InterfaceName.AccountId.name()), 1)
                        .addColumnWithFixedValue("_DUMMY_", null, String.class);
                product = product.addColumnWithFixedValue("_DUMMY_", null, String.class);
                base = base.join(new FieldList("_DUMMY_"), product, new FieldList("_DUMMY_"), JoinType.INNER)
                        .retain(new FieldList(config.getGroupByFields()));
            } else {
                base = base.retain(new FieldList(config.getGroupByFields()))
                        .groupByAndLimit(new FieldList(config.getGroupByFields()), 1);
            }
        }
        return base.renamePipe("_base_node_");
    }

    private Node findSmallestPeriodTable() {
        List<String> periods = Arrays.asList(PeriodStrategy.Template.Year.name(),
                PeriodStrategy.Template.Quarter.name(), PeriodStrategy.Template.Month.name(),
                PeriodStrategy.Template.Week.name());
        for (String period : periods) {
            if (periodTables.containsKey(period)) {
                return periodTables.get(period);
            }
        }
        throw new RuntimeException("Fail to find smallest period table");
    }

    private List<Node> generateMetrics(Node periodTable, List<ActivityMetrics> metrics) {
        List<ActivityMetrics> standardMetrics = new ArrayList<>();
        List<ActivityMetrics> specialMetrics = new ArrayList<>();
        for (ActivityMetrics m : metrics) {
            switch (m.getMetrics()) {
            case ShareOfWallet:
                specialMetrics.add(m);
                break;
            default:
                standardMetrics.add(m);
                break;
            }
        }
        List<Node> res = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(standardMetrics)) {
            res.addAll(generateStandardMetrics(periodTable, standardMetrics));
        }
        if (CollectionUtils.isNotEmpty(specialMetrics)) {
            res.addAll(generateSpecialMetrics(periodTable, specialMetrics));
        }
        return res;
    }

    @SuppressWarnings("rawtypes")
    private List<Node> generateStandardMetrics(Node periodTable, List<ActivityMetrics> metrics) {
        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        metrics.forEach(m -> {
            retainFields.add(ActivityMetricsUtils.getNameWithPeriod(m));
        });
        Aggregator agg = new StandardActivityMetricsAgg(
                new Fields(retainFields.toArray(new String[retainFields.size()])), config.getGroupByFields(), metrics,
                preparePeriodRanges(metrics));
        List<FieldMetadata> fms = prepareFms(periodTable, metrics);
        periodTable = periodTable.groupByAndAggregate(new FieldList(config.getGroupByFields()), agg, fms)
                .retain(new FieldList(retainFields));
        return Arrays.asList(periodTable.renamePipe("_standard_metrics_node_" + periodTable.getPipeName()));
    }

    private List<Node> generateSpecialMetrics(Node periodTable, List<ActivityMetrics> metrics) {
        List<Node> res = new ArrayList<>();
        for (ActivityMetrics m : metrics) {
            switch (m.getMetrics()) {
            case ShareOfWallet:
                res.add(shareOfWallet(periodTable, m));
                break;
            default:
                throw new IllegalStateException("Unknown special metrics " + m.getMetrics());
            }
        }
        return res;
    }

    private Node shareOfWallet(Node periodTable, ActivityMetrics metrics) {
        periodTable = appendSegment(periodTable);
        periodTable = filterPeriod(periodTable, metrics.getPeriodsConfig().get(0), null)
                .filter(String.format("%s != null", InterfaceName.SpendAnalyticsSegment.name()),
                        new FieldList(InterfaceName.SpendAnalyticsSegment.name()));

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        retainFields.add(ActivityMetricsUtils.getNameWithPeriod(metrics));

        List<Aggregation> accountProductSpendAgg = Collections.singletonList(
                new Aggregation(InterfaceName.TotalAmount.name(), "_APSpend_", AggregationType.SUM));
        Node apSpend = periodTable.groupBy(new FieldList(InterfaceName.AccountId.name(), InterfaceName.ProductId.name(),
                InterfaceName.SpendAnalyticsSegment.name()), accountProductSpendAgg)
                .renamePipe("_ap_" + ActivityMetricsUtils.getNameWithPeriod(metrics));

        List<Aggregation> accountTotalSpendAgg = Collections.singletonList(
                new Aggregation("_APSpend_", "_ATSpend_", AggregationType.SUM));
        Node atSpend = apSpend
                .groupBy(new FieldList(InterfaceName.AccountId.name(), InterfaceName.SpendAnalyticsSegment.name()),
                        accountTotalSpendAgg)
                .renamePipe("_at_" + ActivityMetricsUtils.getNameWithPeriod(metrics));

        List<Aggregation> segmentProductSpendAgg = Collections
                .singletonList(new Aggregation("_APSpend_", "_SPSpend_", AggregationType.SUM));
        Node spSpend = apSpend.groupBy(new FieldList(InterfaceName.SpendAnalyticsSegment.name(), InterfaceName.ProductId.name()),
                        segmentProductSpendAgg)
                .renamePipe("_sp_" + ActivityMetricsUtils.getNameWithPeriod(metrics));

        List<Aggregation> semgentTotalSpendAgg = Collections
                .singletonList(new Aggregation("_SPSpend_", "_STSpend_", AggregationType.SUM));
        Node stSpend = spSpend.groupBy(new FieldList(InterfaceName.SpendAnalyticsSegment.name()), semgentTotalSpendAgg)
                .renamePipe("_st_" + ActivityMetricsUtils.getNameWithPeriod(metrics));
        
        Node base = atSpend
                .join(new FieldList(InterfaceName.SpendAnalyticsSegment.name()), spSpend,
                        new FieldList(InterfaceName.SpendAnalyticsSegment.name()), JoinType.INNER) //
                .retain(InterfaceName.AccountId.name(), InterfaceName.ProductId.name(),
                        InterfaceName.SpendAnalyticsSegment.name());

        Node sw = base
                .join(new FieldList(config.getGroupByFields()), apSpend, new FieldList(config.getGroupByFields()),
                        JoinType.LEFT) //
                .join(new FieldList(InterfaceName.AccountId.name()), atSpend,
                        new FieldList(InterfaceName.AccountId.name()), JoinType.INNER) //
                .join(new FieldList(InterfaceName.SpendAnalyticsSegment.name(), InterfaceName.ProductId.name()),
                        spSpend,
                        new FieldList(InterfaceName.SpendAnalyticsSegment.name(), InterfaceName.ProductId.name()),
                        JoinType.INNER) //
                .join(new FieldList(InterfaceName.SpendAnalyticsSegment.name()), stSpend,
                        new FieldList(InterfaceName.SpendAnalyticsSegment.name()), JoinType.INNER);

        sw = sw.apply(
                new MetricsShareOfWalletFunc(ActivityMetricsUtils.getNameWithPeriod(metrics), "_APSpend_", "_ATSpend_",
                        "_SPSpend_", "_STSpend_"),
                new FieldList(sw.getFieldNames()), metricsMetadata.get(metrics))
                .retain(new FieldList(retainFields));
        return sw.renamePipe("_sw_" + ActivityMetricsUtils.getNameWithPeriod(metrics));
    }

    private Node appendSegment(Node periodTable) {
        if (!config.isAccountHasSegment()) {
            return periodTable.addColumnWithFixedValue(InterfaceName.SpendAnalyticsSegment.name(), null, String.class);
        }
        List<String> retainFields = new ArrayList<>(periodTable.getFieldNames());
        retainFields.add(InterfaceName.SpendAnalyticsSegment.name());
        return periodTable.join(InterfaceName.AccountId.name(), account, InterfaceName.AccountId.name(), JoinType.INNER)
                .retain(new FieldList(retainFields));
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

    private Node join(Node base, List<Node> toJoin) {
        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(config.getGroupByFields());
        for (ActivityMetrics metrics : config.getMetrics()) {
            retainFields.add(ActivityMetricsUtils.getNameWithPeriod(metrics));
        }
        FieldList joinFields = new FieldList(config.getGroupByFields());
        List<FieldList> joinFieldsList = new ArrayList<FieldList>(Collections.nCopies(toJoin.size(), joinFields));
        base = base.coGroup(joinFields, toJoin, joinFieldsList, JoinType.OUTER).retain(new FieldList(retainFields));
        StringBuilder sb = new StringBuilder();
        config.getGroupByFields().forEach(groupByField -> {
            sb.append(groupByField + " != null && ");
        });
        return base.filter(sb.substring(0, sb.length() - 4), new FieldList(config.getGroupByFields()));
    }

    private Node assignCompositeKey(Node node) {
        node = node.apply(
                String.format("%s + \"_\" + %s", InterfaceName.AccountId.name(), InterfaceName.ProductId.name()),
                new FieldList(config.getGroupByFields()),
                new FieldMetadata(InterfaceName.__Composite_Key__.name(), String.class));
        return node;
    }

    private Node imputeNull(Node node) {
        node = node.apply(
                new ActivityMetricsNullImputeFunc(new Fields(node.getFieldNamesArray()), config.getMetrics(), null),
                new FieldList(node.getFieldNames()), node.getSchema(), new FieldList(node.getFieldNames()),
                Fields.REPLACE);
        return node;
    }

    private List<FieldMetadata> prepareFms(Node source, List<ActivityMetrics> metrics) {
        List<FieldMetadata> fms = new ArrayList<>();
        config.getGroupByFields().forEach(field -> {
            fms.add(source.getSchema(field));
        });
        metrics.forEach(m -> {
            fms.add(metricsMetadata.get(m));
        });
        return fms;
    }

    /**
     * @param metrics
     * @return metrics name with period -> period boundaries
     */
    private Map<String, List<Integer>> preparePeriodRanges(List<ActivityMetrics> metrics) {
        Map<String, List<Integer>> periodRanges = new HashMap<>();
        metrics.forEach(m -> {
            switch (m.getMetrics()) {
            case SpendChange:
                List<Integer> list = new ArrayList<>();
                TimeFilter withinFilter = m.getPeriodsConfig().get(0).getRelation() == ComparisonType.WITHIN
                        ? m.getPeriodsConfig().get(0)
                        : m.getPeriodsConfig().get(1);
                TimeFilter betweenFilter = m.getPeriodsConfig().get(0).getRelation() == ComparisonType.BETWEEN
                        ? m.getPeriodsConfig().get(0)
                        : m.getPeriodsConfig().get(1);
                list.addAll(timeFilterToPeriodRange(withinFilter));
                list.addAll(timeFilterToPeriodRange(betweenFilter));
                periodRanges.put(ActivityMetricsUtils.getNameWithPeriod(m), list);
                break;
            case HasPurchased:
                break;
            default:
                periodRanges.put(ActivityMetricsUtils.getNameWithPeriod(m),
                        timeFilterToPeriodRange(m.getPeriodsConfig().get(0)));
                break;
            }
        });

        return periodRanges;
    }

    private List<Integer> timeFilterToPeriodRange(TimeFilter timeFilter) {
        PeriodBuilder periodBuilder = periodBuilders.get(timeFilter.getPeriod());
        List<Object> translatedTxnDateRange = timeFilterTranslator.translate(timeFilter).getValues();
        String minTxnDate = translatedTxnDateRange.get(0).toString();
        String maxTxnDate = translatedTxnDateRange.get(1).toString();
        return Arrays.asList(periodBuilder.toPeriodId(minTxnDate), periodBuilder.toPeriodId(maxTxnDate));
    }

    private boolean shouldLoadAccount() {
        if (config.getType() != ActivityType.PurchaseHistory) {
            return false;
        }
        for (ActivityMetrics m : config.getMetrics()) {
            if (m.getMetrics() == InterfaceName.ShareOfWallet && config.isAccountHasSegment()) {
                return true;
            }
        }
        if (config.isReduced()) {
            return false;
        } else {
            return true;
        }
    }

    private boolean shouldLoadProduct() {
        if (config.getType() == ActivityType.PurchaseHistory) {
            return true;
        } else {
            return false;
        }
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
