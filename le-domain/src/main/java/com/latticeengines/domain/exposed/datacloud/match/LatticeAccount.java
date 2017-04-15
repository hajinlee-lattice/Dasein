package com.latticeengines.domain.exposed.datacloud.match;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.Id;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.BaseFabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

public class LatticeAccount extends BaseFabricEntity<LatticeAccount> implements FabricEntity<LatticeAccount> {

    private static final String LATTICE_ACCOUNT_ID = "lattice_account_id";
    private static final String ATTRIBUTES = "attributes";
    public static final String LATTICE_ACCOUNT_ID_HDFS = "LatticeID";
    private static final String RECORD_TYPE_TOKEN = "{{RECORD_TYPE}}";
    
    private static final Log log = LogFactory.getLog(LatticeAccount.class);

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
    public GenericRecord toFabricAvroRecord(String recordType) {
        // we need to replace special char '.' from recordType otherwise avro
        // schema parser will run into exception
        String recordTypeStrForAvroSchema = recordType.replace('.', '_');

        Schema schema = new Schema.Parser()
                .parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordTypeStrForAvroSchema));
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
    public LatticeAccount fromFabricAvroRecord(GenericRecord record) {
        setId(record.get(LATTICE_ACCOUNT_ID).toString());
        if (record.get(ATTRIBUTES) != null) {
            String serializedAttributes = record.get(ATTRIBUTES).toString();
            log.info("serialized attr: " + serializedAttributes);
            Map<String, Object> mapAttributes = JsonUtils.deserialize(serializedAttributes, Map.class);
            log.info(String.format("map object: %s", mapAttributes.get("AlexaCategories")));
            setAttributes(mapAttributes);
        }
        return this;
    }

    @Override
    public LatticeAccount fromHdfsAvroRecord(GenericRecord record) {
        Object idObj = record.get(LATTICE_ACCOUNT_ID_HDFS);
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
        setAttributes(mapAttributes);
        return this;
    }

    @Override
    public Schema getSchema(String recordType) {
        // we need to replace special char '.' from recordType otherwise avro
        // schema parser will run into exception
        String recordTypeStrForAvroSchema = recordType.replace('.', '_');

        return new Schema.Parser().parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordTypeStrForAvroSchema));
    }

}
