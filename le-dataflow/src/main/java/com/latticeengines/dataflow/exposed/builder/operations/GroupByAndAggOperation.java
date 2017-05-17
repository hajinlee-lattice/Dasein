package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.Arrays;
import java.util.List;

import com.google.api.client.util.Lists;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Aggregator;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class GroupByAndAggOperation extends Operation {

    @SuppressWarnings("rawtypes")
    public GroupByAndAggOperation(Operation.Input prior, FieldList groupByFields, Aggregator aggregator) {
        Pipe groupby = null;
        groupby = new GroupBy(prior.pipe, new Fields(groupByFields.getFields()));
        groupby = new Every(groupby, aggregator, Fields.RESULTS);

        this.pipe = groupby;
        this.metadata = Lists.newArrayList(prior.metadata);
    }

    @SuppressWarnings("rawtypes")
    public GroupByAndAggOperation(Operation.Input prior, FieldList groupByFields, Aggregator aggregator, List<FieldMetadata> fms) {
        this(prior, groupByFields, aggregator, fms, Fields.RESULTS);
    }

    @SuppressWarnings("rawtypes")
    public GroupByAndAggOperation(Operation.Input prior, FieldList groupByFields, Aggregator aggregator, List<FieldMetadata> fms, Fields fieldSelectStrategy) {
        Pipe groupby = new GroupBy(prior.pipe, new Fields(groupByFields.getFields()));
        groupby = new Every(groupby, aggregator, fieldSelectStrategy);

        this.pipe = groupby;
        this.metadata = Lists.newArrayList(fms);
    }

    @SuppressWarnings("rawtypes")
    public GroupByAndAggOperation(Operation.Input prior, FieldList groupByFields, FieldList sortFields, Aggregator aggregator,
                                     boolean descending, boolean caseInsensitive) {
        init(prior, groupByFields, sortFields, aggregator, descending, caseInsensitive);
    }

    @SuppressWarnings("rawtypes")
    private void init(Operation.Input prior, FieldList groupByFields, FieldList sortFields, Aggregator aggregator, boolean descending,
                      boolean caseInsensitive) {

        Fields fields = new Fields(groupByFields.getFields());
        if (caseInsensitive) {
            List<String> groupByKeys = Arrays.asList(groupByFields.getFields());
            for (FieldMetadata metadata : prior.metadata) {
                if (groupByKeys.contains(metadata.getFieldName()) && String.class.equals(metadata.getJavaType())) {
                    fields.setComparator(metadata.getFieldName(), String.CASE_INSENSITIVE_ORDER);
                }
            }
        }

        Pipe groupby = new GroupBy(prior.pipe, fields, new Fields(sortFields.getFields()), descending);
        groupby = new Every(groupby, aggregator, Fields.REPLACE);

        this.pipe = groupby;
        this.metadata = Lists.newArrayList(prior.metadata);
    }

}
