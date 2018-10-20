package com.latticeengines.domain.exposed.datafabric;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;

public abstract class BaseHdfsAvroFabricEntity<T> extends BaseFabricEntity<T>
        implements FabricEntity<T> {

    private static final String PARTITION_KEY = "PartitionKey";
    private static final String SORT_KEY = "SortKey";
    private static final String ATTRIBUTES = "Attributes";
    private static final String RECORD_TYPE_TOKEN = "{{RECORD_TYPE}}";

    private static final String SCHEMA_TEMPLATE = String.format(
            "{\"type\":\"record\",\"name\":\"%s\",\"doc\":\"Testing data\"," + "\"fields\":["
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]}" + "]}",
            RECORD_TYPE_TOKEN, PARTITION_KEY, SORT_KEY, ATTRIBUTES);

    @DynamoHashKey(name = PARTITION_KEY, field = PARTITION_KEY)
    private String partitionKey;

    @DynamoRangeKey(name = SORT_KEY, field = SORT_KEY)
    private String sortKey;

    private Map<String, Object> attributes;

    public String getId() {
        return getPartitionKey() + "#" + getSortKey();
    }

    public void setId(String id) {
    }

    private String getPartitionKey() {
        return partitionKey;
    }

    protected void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    private String getSortKey() {
        return sortKey;
    }

    protected void setSortKey(String sortKey) {
        this.sortKey = sortKey;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public abstract String getPartitionKeyInHdfsRecord();

    public abstract String getSortKeyInHdfsRecord();

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = new Schema.Parser()
                .parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordType));
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(PARTITION_KEY, getPartitionKey());
        builder.set(SORT_KEY, getSortKey());
        try {
            String serializedAttributes = JsonUtils.serialize(getAttributes());
            builder.set(ATTRIBUTES, serializedAttributes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize map attributes", e);
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T fromFabricAvroRecord(GenericRecord record) {
        setPartitionKey(record.get(PARTITION_KEY).toString());
        setSortKey(record.get(SORT_KEY).toString());
        if (record.get(ATTRIBUTES) != null) {
            String serializedAttributes = record.get(ATTRIBUTES).toString();
            Map<String, Object> mapAttributes = JsonUtils.deserialize(serializedAttributes,
                    Map.class);
            setAttributes(mapAttributes);
        }
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T fromHdfsAvroRecord(GenericRecord record) {
        setFabricyPartitionKey(record);
        setFabricySortKey(record);

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
        setAttributes(mapAttributes);
        return (T) this;
    }

    @Override
    public Schema getSchema(String recordType) {
        return new Schema.Parser().parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordType));
    }

    protected void setFabricyPartitionKey(GenericRecord record) {
        String pkField = getPartitionKeyInHdfsRecord();
        if (StringUtils.isBlank(pkField)) {
            throw new IllegalArgumentException(
                    "Must specify primary key field in hdfs avro record.");
        }
        Object pkObj = record.get(pkField);
        if (pkObj instanceof Utf8 || pkObj instanceof String) {
            setPartitionKey(pkObj.toString());
        } else {
            setPartitionKey(String.valueOf(pkObj));
        }
    }

    protected void setFabricySortKey(GenericRecord record) {
        String skField = getSortKeyInHdfsRecord();
        if (StringUtils.isNotBlank(skField)) {
            Object skObj = record.get(skField);
            if (skObj instanceof Utf8 || skObj instanceof String) {
                setSortKey(skObj.toString());
            } else {
                setSortKey(String.valueOf(skObj));
            }
        }
    }

}
