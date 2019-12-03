package com.latticeengines.dataflow.runtime.cascading.atlas;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class ActivityMetricsNullImputeFunc extends BaseOperation implements Function {

    private static final long serialVersionUID = 2492674081225939002L;

    private Map<String, Integer> namePositionMap;
    private List<ActivityMetrics> metrics;
    private List<String> pivotedVals;

    public ActivityMetricsNullImputeFunc(Fields fieldDeclaration, List<ActivityMetrics> metrics,
            List<String> pivotedVals) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.metrics = metrics;
        this.pivotedVals = pivotedVals;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = arguments.getTupleCopy();
        if (CollectionUtils.isNotEmpty(pivotedVals)) {
            for (String p : pivotedVals)
                for (ActivityMetrics m : metrics) {
                    result = update(m, ActivityMetricsUtils
                            .getFullName(ActivityMetricsUtils.getNameWithPeriod(m), p), result);
                }
        } else {
            for (ActivityMetrics m : metrics) {
                result = update(m, ActivityMetricsUtils.getNameWithPeriod(m), result);
            }
        }
        functionCall.getOutputCollector().add(result);
    }

    private Tuple update(ActivityMetrics m, String field, Tuple result) {
        int index = namePositionMap.get(field);
        switch (m.getMetrics()) {
            case SpendChange:
                if (result.getObject(index) == null) {
                    result.set(index, 0);
                }
                break;
            case TotalSpendOvertime:
            case AvgSpendOvertime:
                if (result.getObject(index) == null) {
                    result.set(index, 0.0);
                }
                break;
            case HasPurchased:
                if (result.getObject(index) == null) {
                    result.set(index, false);
                }
                break;
            default:
                break;
        }
        return result;
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }
}
