package com.latticeengines.domain.exposed.datacloud.match;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.Id;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datafabric.BaseFabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

public class ContactTpsEntry extends BaseFabricEntity<ContactTpsEntry> implements FabricEntity<ContactTpsEntry> {

    public static final String TPS_UUID_HDFS = ContactMasterConstants.TPS_RECORD_UUID;
    private static final String TPS_ID = "Tps_id";
    private static final String ATTRIBUTES = "attributes";
    private static final String RECORD_TYPE_TOKEN = "{{RECORD_TYPE}}";

    private static final String SCHEMA_TEMPLATE = String.format(
            "{\"type\":\"record\",\"name\":\"%s\",\"doc\":\"Contact TPS\"," + "\"fields\":["
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]}" + "]}",
            RECORD_TYPE_TOKEN, TPS_ID, ATTRIBUTES);

    @Id
    @JsonProperty(TPS_ID)
    private String id;

    @JsonProperty(ATTRIBUTES)
    private Map<String, Object> attributes;

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = getSchema(recordType);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(TPS_ID, id);
        try {
            String serializedAttributes = JsonUtils.serialize(getAttributes());
            builder.set(ATTRIBUTES, serializedAttributes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize json attributes", e);
        }
        return builder.build();
    }

    @Override
    public Schema getSchema(String recordType) {
        // we need to replace special char '.' from recordType otherwise avro
        // schema parser will run into exception
        String recordTypeStrForAvroSchema = recordType.replace('.', '_');

        return new Schema.Parser().parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordTypeStrForAvroSchema));
    }

    @Override
    public ContactTpsEntry fromFabricAvroRecord(GenericRecord record) {
        setId(record.get(TPS_ID).toString());
        if (record.get(ATTRIBUTES) != null) {
            String serializedAttributes = record.get(ATTRIBUTES).toString();
            Map<String, Object> mapAttributes = JsonUtils.deserialize(serializedAttributes, Map.class);
            setAttributes(mapAttributes);
        }
        return this;
    }

    @Override
    public ContactTpsEntry fromHdfsAvroRecord(GenericRecord record) {
        Object idObj = record.get(TPS_UUID_HDFS);
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
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
