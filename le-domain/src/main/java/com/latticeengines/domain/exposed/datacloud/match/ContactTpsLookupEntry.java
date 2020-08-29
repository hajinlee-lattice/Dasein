package com.latticeengines.domain.exposed.datacloud.match;

import javax.persistence.Id;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.BaseFabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

public class ContactTpsLookupEntry extends BaseFabricEntity<ContactTpsLookupEntry>
        implements FabricEntity<ContactTpsLookupEntry> {

    public static final String TPS_LOOKUP_ID_HDFS = "SITE_DUNS_NUMBER";
    public static final String TPS_RECORD_IDS_HDFS = "RECORD_IDS";
    private static final String TPS_LOOKUP_ID = "Tps_lookup_id";
    private static final String TPS_RECORD_IDS = "Tps_record_ids";
    private static final String RECORD_TYPE_TOKEN = "{{RECORD_TYPE}}";
    private static final String SITE_DUNS_PREFIX = "_SITE_DUNS_";

    private static final String SCHEMA_TEMPLATE = String.format(
            "{\"type\":\"record\",\"name\":\"%s\",\"doc\":\"Contact TPS\"," + "\"fields\":["
                    + "{\"name\":\"%s\",\"type\":\"string\"}," + "{\"name\":\"%s\",\"type\":\"string\"}" + "]}",
            RECORD_TYPE_TOKEN, TPS_LOOKUP_ID, TPS_RECORD_IDS);

    @Id
    @JsonProperty(TPS_LOOKUP_ID)
    private String id;

    @JsonProperty(TPS_RECORD_IDS)
    private String recordIds;

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = getSchema(recordType);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(TPS_LOOKUP_ID, id);
        builder.set(TPS_RECORD_IDS, recordIds);
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
        if (record.get(TPS_RECORD_IDS) != null) {
            String records = record.get(TPS_RECORD_IDS).toString();
            setRecordIds(records);
        }
        return this;
    }

    @Override
    public ContactTpsLookupEntry fromHdfsAvroRecord(GenericRecord record) {
        Object idObj = record.get(TPS_LOOKUP_ID_HDFS);
        if (idObj instanceof Utf8 || idObj instanceof String) {
            setId(SITE_DUNS_PREFIX + idObj.toString());
        } else {
            setId(SITE_DUNS_PREFIX + String.valueOf(idObj));
        }
        String ids = record.get(TPS_RECORD_IDS_HDFS).toString();
        setRecordIds(ids);
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

    public String getRecordIds() {
        return recordIds;
    }

    public void setRecordIds(String recordIds) {
        this.recordIds = recordIds;
    }
}
