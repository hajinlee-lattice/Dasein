package com.latticeengines.common.exposed.transformer;

import java.util.Map;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;

public class LiveRampRecommendationAvroToJsonFunction implements Function<GenericRecord, GenericRecord> {
    private Map<String, String> contactDisplayNames;

    public LiveRampRecommendationAvroToJsonFunction(Map<String, String> contactDisplayNames) {
        this.contactDisplayNames = contactDisplayNames;
    }

    @Override
    public GenericRecord apply(GenericRecord rec) {
        return rec;
    }
}
