package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ActivityMetricsPivotAgg;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ActivityMetricsPivotConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

@Component(ActivityMetricsPivotFlow.BEAN_NAME)
public class ActivityMetricsPivotFlow extends ConfigurableFlowBase<ActivityMetricsPivotConfig> {
    public static final String BEAN_NAME = "activityMetricsPivotFlow";

    private ActivityMetricsPivotConfig config;

    private List<Object> pivotValues;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node node = addSource(parameters.getBaseTables().get(0));

        init();
        node = pivot(node);

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

        for (Object pivotVal : pivotValues)
            for (String metrics : metricsFields) {
                String field = ActivityMetrics.getFullActivityMetricsName(metrics, String.valueOf(pivotVal));
                fields.add(field);
                fms.add(new FieldMetadata(field, node.getSchema(metrics).getJavaType()));
            }

        Aggregator agg = new ActivityMetricsPivotAgg(new Fields(fields.toArray(new String[fields.size()])),
                config.getGroupByField(), config.getPivotField(), metricsFields, pivotValues);
        node = node.groupByAndAggregate(new FieldList(config.getGroupByField()), agg, fms);
        return node;
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
