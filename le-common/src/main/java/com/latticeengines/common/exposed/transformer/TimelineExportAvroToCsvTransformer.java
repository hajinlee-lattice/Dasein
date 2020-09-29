package com.latticeengines.common.exposed.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;

public class TimelineExportAvroToCsvTransformer implements AvroToCsvTransformer {

    private List<String> fieldNames;

    public TimelineExportAvroToCsvTransformer(List<String> fieldNames) {
        this.fieldNames = CollectionUtils.isNotEmpty(fieldNames) ? fieldNames : new ArrayList<>();
    }

    @Override
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public Function<GenericRecord, List<String[]>> getCsvConverterFunction() {
        return genRecord -> {
            List<String[]> csvRowsForRecord = new ArrayList<>();

            // Construct timeline export Value List
            List<String> fieldValues = new ArrayList<>();
            for (String field : fieldNames) {
                fieldValues.add(genRecord.get(field) != null ? genRecord.get(field).toString() : "");
            }
            csvRowsForRecord.add(fieldValues.toArray(new String[0]));
            return csvRowsForRecord;
        };
    }
}
