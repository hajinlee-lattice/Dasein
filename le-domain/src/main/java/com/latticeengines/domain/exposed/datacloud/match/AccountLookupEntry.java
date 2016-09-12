package com.latticeengines.domain.exposed.datacloud.match;

import javax.persistence.Id;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.DynamoHashKey;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

public class AccountLookupEntry implements FabricEntity<AccountLookupEntry> {

    private static final String DOMAIN_TOKEN = "_DOMAIN_";
    private static final String DUNS_TOKEN = "_DUNS_";

    public static final String UNKNOWN = "NULL";
    private static final String DOMAIN = "domain";
    private static final String DUNS = "duns";

    private static final String LATTICE_ACCOUNT_ID = "latticeAccountId";

    public static final String LATTICE_ACCOUNT_ID_HDFS = "LatticeID";
    private static final String KEY_HDFS = "Key";

    private static final String RECORD_TYPE_TOKEN = "{{RECORD_TYPE}}";

    private static final String SCHEMA_TEMPLATE = String.format(
            "{\"type\":\"record\",\"name\":\"%s\",\"doc\":\"Testing data\"," + "\"fields\":["
                    + "{\"name\":\"%s\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"domain\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"duns\",\"type\":[\"string\",\"null\"]}" + "]}",
            RECORD_TYPE_TOKEN, LATTICE_ACCOUNT_ID);

    @Id
    String id = null;

    @JsonProperty(DOMAIN)
    private String domain = UNKNOWN;

    @JsonProperty(DUNS)
    private String duns = UNKNOWN;

    @JsonProperty(LATTICE_ACCOUNT_ID)
    @DynamoHashKey(name = "AccountId")
    private String latticeAccountId = UNKNOWN;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLatticeAccountId() {
        return latticeAccountId;
    }

    public void setLatticeAccountId(String latticeAccountId) {
        this.latticeAccountId = latticeAccountId;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
        this.id = buildId(domain, duns);
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
        this.id = buildId(domain, duns);
    }

    public static String buildId(String domain, String duns) {
        domain = domain != null ? domain : UNKNOWN;
        duns = duns != null ? duns : UNKNOWN;
        return DOMAIN_TOKEN + domain + DUNS_TOKEN + duns;
    }

    public static AccountLookupEntry fromKey(String latticeAccountId, String key) {
        key = key.replace(DOMAIN_TOKEN, "");
        String[] tokens = key.split(DUNS_TOKEN);
        String domain = UNKNOWN.equals(tokens[0]) ? null : tokens[0];
        String duns = UNKNOWN.equals(tokens[1]) ? null : tokens[1];
        AccountLookupEntry entry = new AccountLookupEntry();
        entry.setDomain(domain);
        entry.setDuns(duns);
        entry.setLatticeAccountId(latticeAccountId);
        return entry;
    }

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        // we need to replace special char '.' from recordType otherwise avro
        // schema parser will run into exception
        String recordTypeStrForAvroSchema = recordType.replace('.', '_');

        Schema schema = new Schema.Parser()
                .parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordTypeStrForAvroSchema));
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(LATTICE_ACCOUNT_ID, getLatticeAccountId());
        builder.set(DOMAIN, getDomain());
        builder.set(DUNS, getDuns());
        return builder.build();
    }

    @Override
    public AccountLookupEntry fromFabricAvroRecord(GenericRecord record) {
        setLatticeAccountId(record.get(LATTICE_ACCOUNT_ID) == null ? null : record.get(LATTICE_ACCOUNT_ID).toString());
        setDomain(record.get(DOMAIN) == null ? null : record.get(DOMAIN).toString());
        setDuns(record.get(DUNS) == null ? null : record.get(DUNS).toString());
        return this;
    }

    @Override
    public AccountLookupEntry fromHdfsAvroRecord(GenericRecord record) {
        String latticeAccountId = record.get(LATTICE_ACCOUNT_ID_HDFS).toString();
        String key = record.get(KEY_HDFS).toString();
        return fromKey(latticeAccountId, key);
    }

    @Override
    public Schema getSchema(String recordType) {
        // we need to replace special char '.' from recordType otherwise avro
        // schema parser will run into exception
        String recordTypeStrForAvroSchema = recordType.replace('.', '_');

        return new Schema.Parser().parse(SCHEMA_TEMPLATE.replace(RECORD_TYPE_TOKEN, recordTypeStrForAvroSchema));
    }

}
