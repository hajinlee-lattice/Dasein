package com.latticeengines.domain.exposed.propdata.match;

import java.util.Map;

import javax.persistence.Id;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class LatticeAccount implements HasId<String>, FabricEntity<LatticeAccount> {
    private static final String LATTICE_ACCOUNT_ID = "lattice_account_id";
    private static final String ATTRIBUTES = "attributes";
    private static final String RECORD_TYPE_TOKEN = "{{RECORD_TYPE}}";

    private static final String SCHEMA_TEMPLATE = String.format(
            "{\"type\":\"record\",\"name\":\"%s\",\"doc\":\"Testing data\"," + "\"fields\":["
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]}" + "]}",
            RECORD_TYPE_TOKEN, LATTICE_ACCOUNT_ID, ATTRIBUTES);

    @Id
    @JsonProperty(LATTICE_ACCOUNT_ID)
    private String id;

    @JsonProperty(ATTRIBUTES)
    private Map<String, Object> attributes;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    @Override
    public GenericRecord toAvroRecord(String recordType) {
        Schema schema = new Schema.Parser().parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordType));
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(LATTICE_ACCOUNT_ID, getId());
        try {
            String serializedAttributes = JsonUtils.serialize(getAttributes());
            builder.set(ATTRIBUTES, serializedAttributes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize json attributes", e);
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public LatticeAccount fromAvroRecord(GenericRecord record) {
        setId(record.get(LATTICE_ACCOUNT_ID).toString());
        if (record.get(ATTRIBUTES) != null) {
            String serializedAttributes = record.get(ATTRIBUTES).toString();
            Map<String, Object> mapAttributes = JsonUtils.deserialize(serializedAttributes, Map.class);
            setAttributes(mapAttributes);
        }
        return this;
    }

    @Override
    public Schema getSchema(String recordType) {
        return new Schema.Parser().parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordType));
    }

}
