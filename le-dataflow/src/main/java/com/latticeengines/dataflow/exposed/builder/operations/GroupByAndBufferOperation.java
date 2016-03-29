package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.Arrays;
import java.util.List;

import cascading.operation.Buffer;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.google.api.client.util.Lists;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;

public class GroupByAndBufferOperation extends Operation {
    public GroupByAndBufferOperation(Input prior, FieldList groupByFields, Buffer buffer) {
        Pipe groupby = null;
        groupby = new GroupBy(prior.pipe, new Fields(groupByFields.getFields()));
        groupby = new Every(groupby, buffer, Fields.RESULTS);

        this.pipe = groupby;
        this.metadata = Lists.newArrayList(prior.metadata);
    }

    public GroupByAndBufferOperation(Input prior, FieldList groupByFields, Buffer buffer, List<FieldMetadata> fms) {
        Pipe groupby = new GroupBy(prior.pipe, new Fields(groupByFields.getFields()));
        groupby = new Every(groupby, buffer, Fields.RESULTS);

        this.pipe = groupby;
        this.metadata = Lists.newArrayList(fms);
    }

    public GroupByAndBufferOperation(Input prior, FieldList groupByFields, FieldList sortFields, Buffer buffer,
            boolean descending, boolean caseInsensitive) {
        init(prior, groupByFields, sortFields, buffer, descending, caseInsensitive);
    }

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
}
