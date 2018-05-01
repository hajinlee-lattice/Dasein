package com.latticeengines.datafabric.entitymanager.impl;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.BaseFabricEntity;
import com.latticeengines.domain.exposed.datafabric.DynamoHashKey;
import com.latticeengines.domain.exposed.datafabric.DynamoRangeKey;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

public class TestDynamoEntity extends BaseFabricEntity<TestDynamoEntity> implements FabricEntity<TestDynamoEntity> {

    public static final String PRIMARY_KEY = "PrimaryKey";
    public static final String SORT_KEY = "SortKey";

    private static final String LATTICE_ACCOUNT_ID = "lattice_account_id";
    private static final String LATTICE_ACCOUNT_SORT_ID = "lattice_account_sort_id";
    private static final String JSON_ATTRIBUTES = "json_attributes";
    private static final String MAP_ATTRIBUTES = "map_attributes";
    private static final String RECORD_TYPE_TOKEN = "{{RECORD_TYPE}}";

    private static final String SCHEMA_TEMPLATE = String.format(
            "{\"type\":\"record\",\"name\":\"%s\",\"doc\":\"Testing data\"," + "\"fields\":["
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]}" + "]}",
            RECORD_TYPE_TOKEN, LATTICE_ACCOUNT_ID, LATTICE_ACCOUNT_SORT_ID, JSON_ATTRIBUTES, MAP_ATTRIBUTES);

    @DynamoHashKey(name = PRIMARY_KEY, field = LATTICE_ACCOUNT_ID)
    @JsonProperty(LATTICE_ACCOUNT_ID)
    private String primaryId;

    @DynamoRangeKey(name = SORT_KEY, field = LATTICE_ACCOUNT_SORT_ID)
    @JsonProperty(LATTICE_ACCOUNT_SORT_ID)
    private String sortId;

    @JsonProperty(JSON_ATTRIBUTES)
    private JsonNode jsonAttributes;

    @JsonProperty(MAP_ATTRIBUTES)
    private Map<String, Object> mapAttributes;

    public String getId() {
        return getPrimaryId() + "#" + getSortId();
    }

    public void setId(String id) {}

    public String getSortId() {
        return sortId;
    }

    public void setSortId(String sortId) {
        this.sortId = sortId;
    }


    public String getPrimaryId() {
        return primaryId;
    }

    public void setPrimaryId(String primaryId) {
        this.primaryId = primaryId;
    }

    public JsonNode getJsonAttributes() {
        return jsonAttributes;
    }

    public void setJsonAttributes(JsonNode jsonAttributes) {
        this.jsonAttributes = jsonAttributes;
    }

    public Map<String, Object> getMapAttributes() {
        return mapAttributes;
    }

    public void setMapAttributes(Map<String, Object> mapAttributes) {
        this.mapAttributes = mapAttributes;
    }

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = new Schema.Parser().parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordType));
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(LATTICE_ACCOUNT_ID, getPrimaryId());
        builder.set(LATTICE_ACCOUNT_SORT_ID, getSortId());
        try {
            String serializedAttributes = JsonUtils.serialize(getJsonAttributes());
            builder.set(JSON_ATTRIBUTES, serializedAttributes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize json attributes", e);
        }
        try {
            String serializedAttributes = JsonUtils.serialize(getMapAttributes());
            builder.set(MAP_ATTRIBUTES, serializedAttributes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize map attributes", e);
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public TestDynamoEntity fromFabricAvroRecord(GenericRecord record) {
        setPrimaryId(record.get(LATTICE_ACCOUNT_ID).toString());
        setSortId(record.get(LATTICE_ACCOUNT_SORT_ID).toString());

        if (record.get(JSON_ATTRIBUTES) != null) {
            String serializedAttributes = record.get(JSON_ATTRIBUTES).toString();
            JsonNode jsonNode = JsonUtils.deserialize(serializedAttributes, JsonNode.class);
            setJsonAttributes(jsonNode);
        }

        if (record.get(MAP_ATTRIBUTES) != null) {
            String serializedAttributes = record.get(MAP_ATTRIBUTES).toString();
            Map<String, Object> mapAttributes = JsonUtils.deserialize(serializedAttributes, Map.class);
            setMapAttributes(mapAttributes);
        }
        return this;
    }

    @Override
    public TestDynamoEntity fromHdfsAvroRecord(GenericRecord record) {
        return this;
    }

    @Override
    public Schema getSchema(String recordType) {
        return new Schema.Parser().parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordType));
    }
}
