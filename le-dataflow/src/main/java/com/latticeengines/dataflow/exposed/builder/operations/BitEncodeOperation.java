package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.latticeengines.dataflow.runtime.cascading.propdata.BitEncodeBuffer;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import cascading.operation.Buffer;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.tuple.Fields;

public class BitEncodeOperation extends Operation {

    public BitEncodeOperation(Input prior, String[] groupyByFields, String keyField, String valueField,
            String encodedField, BitCodeBook codeBook) {
        this.metadata = constructMetadata(prior.metadata, groupyByFields, encodedField);

        String[] fieldNames = new String[this.metadata.size()];
        for (int i = 0; i < this.metadata.size(); i++) {
            FieldMetadata field = this.metadata.get(i);
            fieldNames[i] = field.getFieldName();
        }

        Buffer<?> buffer = new BitEncodeBuffer(new Fields(fieldNames), keyField, valueField, encodedField, codeBook);

        Fields fieldsWithComparator = new Fields(groupyByFields);
        List<String> groupByKeys = Arrays.asList(groupyByFields);
        for (FieldMetadata metadata : prior.metadata) {
            if (groupByKeys.contains(metadata.getFieldName()) && String.class.equals(metadata.getJavaType())) {
                fieldsWithComparator.setComparator(metadata.getFieldName(), String.CASE_INSENSITIVE_ORDER);
            }
        }
        GroupBy groupby = new GroupBy(prior.pipe, fieldsWithComparator);

        this.pipe = new Every(groupby, buffer, Fields.RESULTS);
    }

    private List<FieldMetadata> constructMetadata(List<FieldMetadata> originalMetadataList, String[] groupyByFields,
            String encodedField) {
        // retain group by fields and the encoded field
        List<FieldMetadata> finalMetadataList = new ArrayList<>();
        List<String> groupbyColumns = Arrays.asList(groupyByFields);
        for (FieldMetadata field : originalMetadataList) {
            if (groupbyColumns.contains(field.getFieldName())) {
                finalMetadataList.add(field);
            }
        }
        finalMetadataList.add(new FieldMetadata(encodedField, String.class));
        return finalMetadataList;
    }

}
