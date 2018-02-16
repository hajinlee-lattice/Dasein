package com.latticeengines.dataflow.runtime.cascading.cdl;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class AverageScoreAggregator extends BaseAggregator<AverageScoreAggregator.Context>
        implements Aggregator<AverageScoreAggregator.Context> {

    private static final long serialVersionUID = -3342650400167761862L;

    private String modelGuidField;
    private Map<String, String> scoreFieldMap;

    public static class Context extends BaseAggregator.Context {
        String modelGuid;
        double sum = 0.0;
        long count = 0;
    }

    public AverageScoreAggregator(String modelGuidField, Map<String, String> scoreFieldMap, String avgField) {
        super(new Fields(modelGuidField, avgField));
        this.modelGuidField = modelGuidField;
        this.scoreFieldMap = scoreFieldMap;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return StringUtils.isNotBlank(group.getString(modelGuidField));
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        context.modelGuid = group.getString(modelGuidField);
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        long count = context.count + 1;
        double sum = context.sum;

        String modelGuid = arguments.getString(modelGuidField);
        String scoreField = scoreFieldMap.get(modelGuid);
        Object scoreObj = arguments.getObject(scoreField);
        if (scoreObj != null) {
            double score;
            if (scoreObj instanceof Double) {
                score = (Double) scoreObj;
            } else {
                score = Double.valueOf(String.valueOf(scoreObj));
            }
            sum += score;
        }

        context.count = count;
        context.sum = sum;
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple result = Tuple.size(2);
        result.set(0, context.modelGuid);
        Double avg = null;
        if (context.count > 0) {
            avg = context.sum / context.count;
        }
        result.set(1, avg);
        return result;
    }
}
