package com.latticeengines.domain.exposed.datacloud.match;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.persistence.Id;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

public abstract class MatchCache<T> implements FabricEntity<T> {

    public abstract T getInstance();

    private static final String KEY = "Key";
    private static final String CACHE_CONTEXT = "CacheContext";
    private static final String TIMESTAMP = "Timestamp";
    private static final String UNKNOWN = "NULL";

    private Map<String, String> keyTokenValues = new TreeMap<String, String>();

    private static final String RECORD_TYPE_TOKEN = "{{RECORD_TYPE}}";

    private static final String SCHEMA_TEMPLATE = String.format(
            "{\"type\":\"record\",\"name\":\"%s\",\"doc\":\"%s\"," + "\"fields\":["
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]}" + "]}",
            RECORD_TYPE_TOKEN, MatchCache.class.getSimpleName(), KEY, CACHE_CONTEXT);

    @Id
    @JsonProperty(KEY)
    private String id;

    @JsonProperty(CACHE_CONTEXT)
    private Map<String, Object> cacheContext;

    @DynamoAttribute(TIMESTAMP)
    @JsonProperty(TIMESTAMP)
    private Long timestamp;

    public String buildId() {
        StringBuilder sb = new StringBuilder();
        for (String token : keyTokenValues.keySet()) {
            sb.append(token);
            sb.append(keyTokenValues.get(token) == null ? UNKNOWN : keyTokenValues.get(token));
        }
        id = sb.toString();
        return id;
    }

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        // we need to replace special char '.' from recordType otherwise avro
        // schema parser will run into exception
        String recordTypeStrForAvroSchema = recordType.replace('.', '_');

        Schema schema = new Schema.Parser()
                .parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordTypeStrForAvroSchema));
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(KEY, getId());
        try {
            String serializedContext = JsonUtils.serialize(getCacheContext());
            builder.set(CACHE_CONTEXT, serializedContext);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize json attributes", e);
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T fromFabricAvroRecord(GenericRecord record) {
        setId(record.get(KEY).toString());
        if (record.get(CACHE_CONTEXT) != null) {
            String serializedContext = record.get(CACHE_CONTEXT).toString();
            Map<String, Object> mapAttributes = JsonUtils.deserialize(serializedContext, Map.class);
            setCacheContext(mapAttributes);
        }
        return getInstance();
    }

    @Override
    public T fromHdfsAvroRecord(GenericRecord record) {
        Object idObj = record.get(KEY);
        if (idObj instanceof Utf8 || idObj instanceof String) {
            setId(idObj.toString());
        } else {
            setId(String.valueOf(idObj));
        }
        Map<String, Object> mapAttributes = new HashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            String key = field.name();
            Object value = record.get(key);
            if (value == null) {
                continue;
            }
            if (value instanceof Utf8) {
                value = value.toString();
            }
            mapAttributes.put(key, value);
        }
        setCacheContext(mapAttributes);
        return getInstance();
    }

    @Override
    public Schema getSchema(String recordType) {
        // we need to replace special char '.' from recordType otherwise avro
        // schema parser will run into exception
        String recordTypeStrForAvroSchema = recordType.replace('.', '_');
        return new Schema.Parser().parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordTypeStrForAvroSchema));
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, Object> getCacheContext() {
        return cacheContext;
    }

    public void setCacheContext(Map<String, Object> cacheContext) {
        this.cacheContext = cacheContext;
    }

    public Map<String, String> getKeyTokenValues() {
        return keyTokenValues;
    }

    public void setKeyTokenValues(Map<String, String> keyTokenValues) {
        this.keyTokenValues = keyTokenValues;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }


}
