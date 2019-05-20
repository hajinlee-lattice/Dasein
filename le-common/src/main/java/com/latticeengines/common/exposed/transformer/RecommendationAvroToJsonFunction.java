package com.latticeengines.common.exposed.transformer;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;

public class RecommendationAvroToJsonFunction implements Function<GenericRecord, GenericRecord> {

    private Map<String, String> accountDisplayNames;
    private Map<String, String> contactDisplayNames;

    public RecommendationAvroToJsonFunction(Map<String, String> accountDisplayNames,
            Map<String, String> contactDisplayNames) {
        this.accountDisplayNames = accountDisplayNames;
        this.contactDisplayNames = contactDisplayNames;
    }

    @Override
    public GenericRecord apply(GenericRecord rec) {
        Object obj = rec.get("CONTACTS");
        if (obj != null && StringUtils.isNotBlank(obj.toString())) {
            obj = JsonUtils.deserialize(obj.toString(), new TypeReference<List<Map<String, String>>>() {
            });
            rec.put("CONTACTS", obj);
        }
        return rec;
    }
}
