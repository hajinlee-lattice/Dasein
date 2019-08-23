package com.latticeengines.common.exposed.transformer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;

public class RecommendationAvroToCsvTransformer implements AvroToCsvTransformer {

    private static final Logger log = LoggerFactory.getLogger(RecommendationAvroToCsvTransformer.class);

    private List<String> accountFields;
    private List<String> contactFields;
    private Map<String, String> accountDisplayNames;
    private Map<String, String> contactDisplayNames;
    private boolean ignoreAccountsWithoutContacts;

    public RecommendationAvroToCsvTransformer(Map<String, String> accountDisplayNames,
            Map<String, String> contactDisplayNames, boolean ignoreAccountsWithoutContacts) {
        this.accountDisplayNames = MapUtils.isNotEmpty(accountDisplayNames) ? accountDisplayNames : new HashMap<>();
        this.contactDisplayNames = MapUtils.isNotEmpty(contactDisplayNames) ? contactDisplayNames : new HashMap<>();
        this.ignoreAccountsWithoutContacts = ignoreAccountsWithoutContacts;
    }

    @Override
    public List<String> getFieldNames(Schema schema) {
        accountFields = accountDisplayNames.keySet().stream().collect(Collectors.toList());
        List<String> fieldNames = accountDisplayNames.values().stream().collect(Collectors.toList());
        contactFields = contactDisplayNames.keySet().stream().collect(Collectors.toList());
        fieldNames.addAll(contactDisplayNames.values());
        log.info("Fields: " + String.join(", ", fieldNames));
        return fieldNames;
    }

    @Override
    public Function<GenericRecord, List<String[]>> getCsvConverterFunction() {
        return genRecord -> {
            List<String[]> csvRowsForRecord = new ArrayList<>();

            // Construct Account Value List
            List<String> accountValues = new ArrayList<>();
            for (String field : accountFields) {
                accountValues.add(genRecord.get(field) != null ? genRecord.get(field).toString() : "");
            }

            Object obj = genRecord.get("CONTACTS");
            if (obj != null && StringUtils.isNotBlank(obj.toString())) {
                List<Map<String, String>> contactListMap = JsonUtils.deserialize(obj.toString(),
                        new TypeReference<List<Map<String, String>>>() {
                        });

                for (Map<String, String> contactMap : contactListMap) {
                    List<String> csvRow = new ArrayList<>(accountValues);
                    for (String cField : contactFields) {
                        csvRow.add(contactMap.get(cField));
                    }
                    // Add to Global CSV Rows List
                    csvRowsForRecord.add(csvRow.toArray(new String[0]));
                }
            } else if (!ignoreAccountsWithoutContacts) {
                csvRowsForRecord.add(accountValues.toArray(new String[0]));
            }
            return csvRowsForRecord;
        };
    }
}
