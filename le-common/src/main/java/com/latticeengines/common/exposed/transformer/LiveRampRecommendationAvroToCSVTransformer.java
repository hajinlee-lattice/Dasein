package com.latticeengines.common.exposed.transformer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveRampRecommendationAvroToCSVTransformer implements AvroToCsvTransformer {

    private static final Logger log = LoggerFactory.getLogger(LiveRampRecommendationAvroToCSVTransformer.class);

    private List<String> contactFields;
    private Map<String, String> contactDisplayNames;

    public LiveRampRecommendationAvroToCSVTransformer(Map<String, String> contactDisplayNames) {
        this.contactDisplayNames = MapUtils.isNotEmpty(contactDisplayNames) ? contactDisplayNames : new HashMap<>();
    }

    @Override
    public List<String> getFieldNames() {
        contactFields = new ArrayList<>(contactDisplayNames.keySet());
        Collections.sort(contactFields);
        List<String> fieldNames = contactFields.stream().map(contactField -> contactDisplayNames.get(contactField))
                .collect(Collectors.toList());
        log.info("Fields: " + String.join(", ", fieldNames));
        return fieldNames;
    }

    @Override
    public Function<GenericRecord, List<String[]>> getCsvConverterFunction() {
        return genRecord -> {
            List<String[]> csvRowsForRecord = new ArrayList<>();

            List<String> csvRow = new ArrayList<>();
            for (String field : contactFields) {
                csvRow.add(genRecord.get(field) != null ? genRecord.get(field).toString() : "");
            }
            csvRowsForRecord.add(csvRow.toArray(new String[0]));


            return csvRowsForRecord;
        };
    }
}
