package com.latticeengines.dataflow.exposed.builder.common;

public class Aggregation {

    private final String aggregatedFieldName;
    private final String targetFieldName;
    private final AggregationType aggregationType;
    private final FieldList outputFieldStrategy;

    public Aggregation(String aggregatedFieldName, String targetFieldName, AggregationType aggregationType) {
        this(aggregatedFieldName, targetFieldName, aggregationType, null);
    }

    public Aggregation(AggregationType aggregationType, FieldList outputFieldStrategy) {
        this(null, null, aggregationType, outputFieldStrategy);
    }

    public Aggregation(String aggregatedFieldName, String targetFieldName, AggregationType aggregationType,
            FieldList outputFieldStrategy) {
        this.aggregatedFieldName = aggregatedFieldName;
        this.targetFieldName = targetFieldName;
        this.aggregationType = aggregationType;
        this.outputFieldStrategy = outputFieldStrategy;
    }

    public String getAggregatedFieldName() {
        return aggregatedFieldName;
    }

    public String getTargetFieldName() {
        return targetFieldName;
    }

    public AggregationType getAggregationType() {
        return aggregationType;
    }

    public FieldList getOutputFieldStrategy() {
        return outputFieldStrategy;
    }
}
