package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.pivot.PivotMapper;
import com.latticeengines.dataflow.runtime.cascading.PivotBuffer;

import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.tuple.Fields;

public class PivotOperation extends Operation {

    private final String prior;
    private PivotMapper pivotMapper;

    public PivotOperation(String prior, DataFlowBuilder.FieldList groupyByFields, PivotMapper pivotMapper,
                          CascadingDataFlowBuilder builder) {
        this(prior, groupyByFields, pivotMapper, true, builder);
    }

    public PivotOperation(String prior, DataFlowBuilder.FieldList groupyByFields, PivotMapper pivotMapper,
                          boolean caseInsensitiveGroupBy, CascadingDataFlowBuilder builder) {
        super(builder);
        this.prior = prior;
        this.pivotMapper = pivotMapper;
        this.metadata = constructMetadata(groupyByFields);

        String[] fieldNames = new String[this.metadata.size()];
        for (int i = 0; i< this.metadata.size(); i++) {
            DataFlowBuilder.FieldMetadata field = this.metadata.get(i);
            fieldNames[i] = field.getFieldName();
        }
        PivotBuffer buffer = new PivotBuffer(pivotMapper, new Fields(fieldNames));

        List<DataFlowBuilder.FieldMetadata> fieldMetadataList = getMetadata(prior);
        Fields fieldsWithComparator = new Fields(groupyByFields.getFields());
        if (caseInsensitiveGroupBy) {
            List<String> groupByKeys = Arrays.asList(groupyByFields.getFields());
            for (DataFlowBuilder.FieldMetadata metadata : fieldMetadataList) {
                if (groupByKeys.contains(metadata.getFieldName()) && String.class.equals(metadata.getJavaType())) {
                    fieldsWithComparator.setComparator(metadata.getFieldName(), String.CASE_INSENSITIVE_ORDER);
                }
            }
        }
        GroupBy groupby = new GroupBy(getPipe(prior), fieldsWithComparator);

        this.pipe = new Every(groupby, buffer, Fields.RESULTS);
    }

    private List<DataFlowBuilder.FieldMetadata> constructMetadata(DataFlowBuilder.FieldList groupyByFields) {
        List<DataFlowBuilder.FieldMetadata> originalMetadataList = getMetadata(prior);
        List<DataFlowBuilder.FieldMetadata> finalMetadataList =  new ArrayList<>();
        Set<String> resultColumns = pivotMapper.getResultColumnsLowerCase();
        List<String> uniqueColumns = Arrays.asList(groupyByFields.getFields());
        for (DataFlowBuilder.FieldMetadata field: originalMetadataList) {
            if (uniqueColumns.contains(field.getFieldName())) {
                if (resultColumns.contains(field.getFieldName().toLowerCase())) {
                    throw new IllegalArgumentException("Column " + field.getFieldName()
                            + " collides with one of the pivoted columns.");
                }
                finalMetadataList.add(field);
            }
        }
        finalMetadataList.addAll(pivotMapper.getFieldMetadataList());
        return finalMetadataList;
    }

}
