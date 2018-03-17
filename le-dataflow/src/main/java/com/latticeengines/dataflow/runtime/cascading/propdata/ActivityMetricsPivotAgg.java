package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ActivityMetricsPivotAgg extends BaseAggregator<ActivityMetricsPivotAgg.Context>
        implements Aggregator<ActivityMetricsPivotAgg.Context> {

    private static final long serialVersionUID = 5854576978926098341L;

    private List<Object> pivotValues;
    private String groupByField;
    private String pivotField;
    private List<String> metricsFields;
    private ActivityType activityType;

    public ActivityMetricsPivotAgg(Fields fieldDeclaration, String groupByField, String pivotField,
            List<String> metricsFields, List<Object> pivotValues, ActivityType activityType) {
        super(fieldDeclaration);
        this.pivotValues = pivotValues;
        this.groupByField = groupByField;
        this.pivotField = pivotField;
        this.metricsFields = metricsFields;
        this.activityType = activityType;
    }

    public static class Context extends BaseAggregator.Context {
        Map<String, Object> pivotData;
        Object groupByVal;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return group.getObject(groupByField) == null;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        context.groupByVal = group.getObject(groupByField);
        context.pivotData = new HashMap<>();
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        Object pivotValue = arguments.getObject(pivotField);
        for (String metrics : metricsFields) {
            context.pivotData.put(ActivityMetricsUtils.getFullName(metrics, String.valueOf(pivotValue)),
                    arguments.getObject(metrics));
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(namePositionMap.get(groupByField), context.groupByVal);
        pivotValues.forEach(pivotVal -> {
            metricsFields.forEach(metrics -> {
                String field = ActivityMetricsUtils.getFullName(metrics, String.valueOf(pivotVal));
                if (activityType == ActivityType.PurchaseHistory && context.pivotData.get(field) == null
                        && field.endsWith(InterfaceName.HasPurchased.name())) {
                    result.set(namePositionMap.get(field), false);
                } else {
                    result.set(namePositionMap.get(field), context.pivotData.get(field));
                }

            });
        });
        return result;
    }
}
