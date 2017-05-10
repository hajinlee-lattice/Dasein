package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.api.client.util.Lists;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Buffer;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class GroupByAndBufferOperation extends Operation {
    @SuppressWarnings("rawtypes")
    public GroupByAndBufferOperation(Input prior, FieldList groupByFields, Buffer buffer) {
        Pipe groupby = null;
        groupby = new GroupBy(prior.pipe, new Fields(groupByFields.getFields()));
        groupby = new Every(groupby, buffer, Fields.RESULTS);

        this.pipe = groupby;
        this.metadata = Lists.newArrayList(prior.metadata);
    }

    @SuppressWarnings("rawtypes")
    public GroupByAndBufferOperation(Input prior, FieldList groupByFields, Buffer buffer, List<FieldMetadata> fms) {
        Pipe groupby = new GroupBy(prior.pipe, new Fields(groupByFields.getFields()));
        groupby = new Every(groupby, buffer, Fields.RESULTS);

        this.pipe = groupby;
        this.metadata = Lists.newArrayList(fms);
    }

    @SuppressWarnings("rawtypes")
    public GroupByAndBufferOperation(Input prior, FieldList groupByFields, FieldList sortFields, Buffer buffer, List<FieldMetadata> fms) {
        Pipe groupby = new GroupBy(prior.pipe, new Fields(groupByFields.getFields()));
        groupby = new Every(groupby, buffer, Fields.RESULTS);

        this.pipe = groupby;
        this.metadata = Lists.newArrayList(fms);
    }

    @SuppressWarnings("rawtypes")
    public GroupByAndBufferOperation(Input prior, FieldList groupByFields, FieldList sortFields, Buffer buffer,
            boolean descending, boolean caseInsensitive) {
        init(prior, groupByFields, sortFields, buffer, descending, caseInsensitive);
    }

    public GroupByAndBufferOperation(Input prior, FieldList groupByFields, FieldList sortFields,
            Map<String, Comparator<?>> comparators, Buffer buffer) {
        init(prior, groupByFields, sortFields, comparators, buffer);
    }

    @SuppressWarnings("rawtypes")
    private void init(Input prior, FieldList groupByFields, FieldList sortFields, Buffer buffer, boolean descending,
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
        groupby = new Every(groupby, buffer, Fields.REPLACE);
        this.pipe = groupby;
        this.metadata = Lists.newArrayList(prior.metadata);
    }

    private void init(Input prior, FieldList groupByFields, FieldList sortFields,
            Map<String, Comparator<?>> comparators, Buffer buffer) {
        Fields fields = new Fields(groupByFields.getFields());
        Fields srtFields = new Fields(sortFields.getFields());
        for (Map.Entry<String, Comparator<?>> entry : comparators.entrySet()) {
            srtFields.setComparator(entry.getKey(), entry.getValue());
        }
        Pipe groupby = new GroupBy(prior.pipe, fields, srtFields);
        groupby = new Every(groupby, buffer, Fields.REPLACE);
        this.pipe = groupby;
        this.metadata = Lists.newArrayList(prior.metadata);
    }

}
