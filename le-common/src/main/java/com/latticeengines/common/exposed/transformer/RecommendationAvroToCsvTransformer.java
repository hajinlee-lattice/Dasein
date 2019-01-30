package com.latticeengines.common.exposed.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;

public class RecommendationAvroToCsvTransformer implements AvroToCsvTransformer {
    final String[] CONTACT_FIELDS = new String[] {"Email","Address","Phone","State","ZipCode","Country","SfdcContactID","City","ContactID","Name"};

    private List<String> accountFields = null;

    @Override
    public List<String> getFieldNames(Schema schema) {
        List<String> fieldNames = schema.getFields().stream().map(field -> field.name()).collect(Collectors.toList());
        fieldNames.remove("CONTACTS");
        this.accountFields = new ArrayList<>(fieldNames);
        for (String cField: CONTACT_FIELDS) {
            fieldNames.add("CONTACT:"+cField);
        }
        return fieldNames;
    }

    @Override
    public Function<GenericRecord, List<String[]>> getCsvConverterFunction() {
        return new Function<GenericRecord, List<String[]>>() {
            @Override
            public List<String[]> apply(GenericRecord genRecord) {
                List<String[]> csvRowsForRecord = new ArrayList<>();

                // Construct Account Value List
                List<String> accountVals = new ArrayList<>();
                for (String field : accountFields) {
                    accountVals.add(genRecord.get(field) != null ? genRecord.get(field).toString() : "");
                }

                Object obj = genRecord.get("CONTACTS");
                if (obj != null && StringUtils.isNotBlank(obj.toString())) {
                    List<Map<String, String>> contactListMap = JsonUtils.deserialize(obj.toString(),
                            new TypeReference<List<Map<String, String>>>() {});
                    
                    for (Map<String, String> contactMap: contactListMap) {
                        List<String> csvRow = new ArrayList<String>(accountVals);
                        for (String cField: CONTACT_FIELDS) {
                            csvRow.add(contactMap.get(cField));
                        }
                        // Add to Global CSV Rows List
                        csvRowsForRecord.add(csvRow.toArray(new String[0]));
                    }
                }
                return csvRowsForRecord;
            }
        };
    }
}
