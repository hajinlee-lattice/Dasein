package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.tuple.Fields;

import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.dataflow.runtime.cascading.PivotBuffer;

public class PivotOperation extends Operation {

    private final Input prior;
    private PivotStrategy pivotStrategy;

    public PivotOperation(Input prior, String[] groupyByFields, PivotStrategy pivotStrategy) {
        this(prior, groupyByFields, pivotStrategy, true);
    }

    public PivotOperation(Input prior, String[] groupyByFields, PivotStrategy pivotStrategy,
            boolean caseInsensitiveGroupBy) {
        this.prior = prior;
        this.pivotStrategy = pivotStrategy;
        this.metadata = constructMetadata(groupyByFields);

        String[] fieldNames = new String[this.metadata.size()];
        for (int i = 0; i < this.metadata.size(); i++) {
            FieldMetadata field = this.metadata.get(i);
            fieldNames[i] = field.getFieldName();
        }
        PivotBuffer buffer = new PivotBuffer(pivotStrategy, new Fields(fieldNames));

        List<FieldMetadata> fieldMetadataList = prior.metadata;
        Fields fieldsWithComparator = new Fields(groupyByFields);
        if (caseInsensitiveGroupBy) {
            List<String> groupByKeys = Arrays.asList(groupyByFields);
            for (FieldMetadata metadata : fieldMetadataList) {
                if (groupByKeys.contains(metadata.getFieldName()) && String.class.equals(metadata.getJavaType())) {
                    fieldsWithComparator.setComparator(metadata.getFieldName(), String.CASE_INSENSITIVE_ORDER);
                }
            }
        }
        GroupBy groupby = new GroupBy(prior.pipe, fieldsWithComparator);

        this.pipe = new Every(groupby, buffer, Fields.RESULTS);
    }

    private List<FieldMetadata> constructMetadata(String[] groupyByFields) {
        List<FieldMetadata> originalMetadataList = prior.metadata;
        List<FieldMetadata> finalMetadataList = new ArrayList<>();
        Set<String> resultColumns = new HashSet<>();
        for (String column : pivotStrategy.getResultColumns()) {
            resultColumns.add(column.toLowerCase());
        }
        List<String> uniqueColumns = Arrays.asList(groupyByFields);
        for (FieldMetadata field : originalMetadataList) {
            if (uniqueColumns.contains(field.getFieldName())) {
                if (resultColumns.contains(field.getFieldName().toLowerCase())) {
                    throw new IllegalArgumentException("Column " + field.getFieldName()
                            + " collides with one of the pivoted columns.");
                }
                finalMetadataList.add(field);
            }
        }
        finalMetadataList.addAll(pivotStrategy.getFieldMetadataList());
        return finalMetadataList;
    }

}
