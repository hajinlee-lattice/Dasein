package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ActivityMetricsNullImputeFunc;
import com.latticeengines.dataflow.runtime.cascading.propdata.ActivityMetricsPivotAgg;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ActivityMetricsPivotConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

@Component(ActivityMetricsPivotFlow.BEAN_NAME)
public class ActivityMetricsPivotFlow extends ActivityMetricsBaseFlow<ActivityMetricsPivotConfig> {

    public static final String BEAN_NAME = "activityMetricsPivotFlow";

    private ActivityMetricsPivotConfig config;

    private List<String> pivotValues;

    private Node account;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node node = addSource(parameters.getBaseTables().get(0));

        if (shouldLoadAccount()) {
            account = addSource(parameters.getBaseTables().get(1));
        }

        init();
        if (node.getSchema(InterfaceName.__Composite_Key__.name()) != null) {
            node = node.discard(InterfaceName.__Composite_Key__.name());
        }
        node = pivot(node);

        if (config.isExpanded()) {
            node = expandAccount(node);
        }

        return node;
    }

    private void init() {
        pivotValues = new ArrayList<>();
        switch (config.getActivityType()) {
        case PurchaseHistory:
            config.getProductMap().forEach((productId, product) -> {
                pivotValues.add(productId);
            });
            break;
        default:
            throw new UnsupportedOperationException(config.getActivityType() + " is not supported");
        }
    }

    @SuppressWarnings("rawtypes")
    private Node pivot(Node node) {
        List<String> metricsFields = new ArrayList<>();
        metricsFields.addAll(node.getFieldNames());
        metricsFields.remove(config.getGroupByField());
        metricsFields.remove(config.getPivotField());

        List<String> fields = new ArrayList<>();
        List<FieldMetadata> fms = new ArrayList<>();
        fields.add(config.getGroupByField());
        fms.add(node.getSchema(config.getGroupByField()));

        for (String pivotVal : pivotValues)
            for (String metrics : metricsFields) {
                String field = ActivityMetricsUtils.getFullName(metrics, pivotVal);
                fields.add(field);
                fms.add(new FieldMetadata(field, node.getSchema(metrics).getJavaType()));
            }

        Aggregator agg = new ActivityMetricsPivotAgg(new Fields(fields.toArray(new String[fields.size()])),
                config.getGroupByField(), config.getPivotField(), metricsFields, pivotValues);
        node = node.groupByAndAggregate(new FieldList(config.getGroupByField()), agg, fms);
        return node;
    }

    private Node expandAccount(Node node) {
        List<String> toRetain = new ArrayList<>(node.getFieldNames());
        node = account.join(new FieldList(InterfaceName.AccountId.name()), node,
                new FieldList(InterfaceName.AccountId.name()), JoinType.LEFT).retain(new FieldList(toRetain));
        return imputeNull(node);
    }

    private Node imputeNull(Node node) {
        node = node.apply(
                new ActivityMetricsNullImputeFunc(new Fields(node.getFieldNamesArray()), config.getMetrics(),
                        pivotValues),
                new FieldList(node.getFieldNames()), node.getSchema(), new FieldList(node.getFieldNames()),
                Fields.REPLACE);
        return node;
    }

    private boolean shouldLoadAccount() {
        if (config.isExpanded()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ActivityMetricsPivotConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return ActivityMetricsPivotFlow.BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.ACTIVITY_METRICS_PIVOT;
    }
}
