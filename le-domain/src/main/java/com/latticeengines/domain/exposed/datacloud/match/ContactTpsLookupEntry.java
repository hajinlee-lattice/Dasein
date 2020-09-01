package com.latticeengines.domain.exposed.datacloud.match;

import javax.persistence.Id;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datafabric.BaseFabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

public class ContactTpsLookupEntry extends BaseFabricEntity<ContactTpsLookupEntry>
        implements FabricEntity<ContactTpsLookupEntry> {

    public static final String TPS_LOOKUP_ID_HDFS = ContactMasterConstants.TPS_SITE_DUNS_NUMBER;
    public static final String TPS_RECORD_UUIDS_HDFS = ContactMasterConstants.TPS_RECORD_UUIDS;
    private static final String TPS_LOOKUP_ID = "Tps_lookup_id";
    private static final String TPS_RECORD_UUIDS = "Tps_record_uuids";
    private static final String RECORD_TYPE_TOKEN = "{{RECORD_TYPE}}";

    private static final String SCHEMA_TEMPLATE = String.format(
            "{\"type\":\"record\",\"name\":\"%s\",\"doc\":\"Contact TPS\"," + "\"fields\":["
                    + "{\"name\":\"%s\",\"type\":\"string\"}," + "{\"name\":\"%s\",\"type\":\"string\"}" + "]}",
            RECORD_TYPE_TOKEN, TPS_LOOKUP_ID, TPS_RECORD_UUIDS);

    @Id
    @JsonProperty(TPS_LOOKUP_ID)
    private String id;

    @JsonProperty(TPS_RECORD_UUIDS)
    private String recordUuids;

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = getSchema(recordType);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(TPS_LOOKUP_ID, id);
        builder.set(TPS_RECORD_UUIDS, recordUuids);
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
    public ContactTpsLookupEntry fromFabricAvroRecord(GenericRecord record) {
        setId(record.get(TPS_LOOKUP_ID).toString());
        if (record.get(TPS_RECORD_UUIDS) != null) {
            String records = record.get(TPS_RECORD_UUIDS).toString();
            setRecordUuids(records);
        }
        return this;
    }

    @Override
    public ContactTpsLookupEntry fromHdfsAvroRecord(GenericRecord record) {
        Object idObj = record.get(TPS_LOOKUP_ID_HDFS);
        if (idObj instanceof Utf8 || idObj instanceof String) {
            setId(idObj.toString());
        } else {
            setId(String.valueOf(idObj));
        }
        String ids = record.get(TPS_RECORD_UUIDS_HDFS).toString();
        setRecordUuids(ids);
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

    public String getRecordUuids() {
        return recordUuids;
    }

    public void setRecordUuids(String recordUuids) {
        this.recordUuids = recordUuids;
    }
}
