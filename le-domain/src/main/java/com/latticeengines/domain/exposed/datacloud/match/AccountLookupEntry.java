package com.latticeengines.domain.exposed.datacloud.match;

import java.util.Arrays;

import javax.persistence.Id;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datafabric.BaseFabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

public class AccountLookupEntry extends BaseFabricEntity<AccountLookupEntry>
        implements FabricEntity<AccountLookupEntry> {

    public static final String UNKNOWN = "NULL";
    public static final String LATTICE_ACCOUNT_ID_HDFS = "LatticeID";
    private static final String DOMAIN_TOKEN = "_DOMAIN_";
    private static final String DUNS_TOKEN = "_DUNS_";
    private static final String COUNTRY_TOKEN = "_COUNTRY_";
    private static final String STATE_TOKEN = "_STATE_";
    private static final String ZIPCODE_TOKEN = "_ZIPCODE_";
    private static final String DOMAIN = "domain"; // parsed from key
    private static final String DUNS = "duns"; // parsed from key
    private static final String LATTICE_ACCOUNT_ID = "latticeAccountId";
    private static final String PATCHED = "Patched";
    private static final String KEY_HDFS = "Key";
    private static final String LDC_DUNS = DataCloudConstants.ATTR_LDC_DUNS;
    private static final String DU_DUNS = DataCloudConstants.ATTR_DU_DUNS;
    private static final String GU_DUNS = DataCloudConstants.ATTR_GU_DUNS;

    // Some background of avro schema evolution
    // https://confluence.lattice-engines.com/display/ENG/How+to+evolve+DataCloud+Dynamo+table+schema
    // For newly added attributes
    private static final Schema UNION_NULL_STRING_SCHEMA = Schema
            .createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
    // To ensure backward compatibility, existing attributes LatticeAccountId,
    // Domain, DUNS has union type as (string, null)
    // If switch order to (null, string), cannot get value from avro from
    // existing Dynamo table
    private static final Schema UNION_STRING_NULL_SCHEMA = Schema
            .createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));

    @Id
    String id = null;

    @JsonProperty(DOMAIN)
    private String domain = UNKNOWN;

    @JsonProperty(DUNS)
    private String duns = UNKNOWN;

    @JsonProperty(LATTICE_ACCOUNT_ID)
    private String latticeAccountId = UNKNOWN;

    @JsonProperty(LDC_DUNS)
    private String ldcDuns;

    @JsonProperty(DU_DUNS)
    private String duDuns;

    @JsonProperty(GU_DUNS)
    private String guDuns;

    public static String buildId(String domain, String duns) {
        domain = domain != null ? domain : UNKNOWN;
        duns = duns != null ? duns : UNKNOWN;
        return DOMAIN_TOKEN + domain + DUNS_TOKEN + duns;
    }

    public static String buildIdWithLocation(String domain, String duns, String country,
            String state, String zipCode) {
        domain = domain != null ? domain : UNKNOWN;
        duns = duns != null ? duns : UNKNOWN;
        country = country != null ? country : UNKNOWN;
        state = state != null ? state : UNKNOWN;
        zipCode = zipCode != null ? zipCode : UNKNOWN;
        return DOMAIN_TOKEN + domain + DUNS_TOKEN + duns + COUNTRY_TOKEN + country + STATE_TOKEN
                + state + ZIPCODE_TOKEN + zipCode;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
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

    public boolean isPatched() {
        return Boolean.TRUE.equals(getTag(PATCHED, Boolean.class));
    }

    public void setPatched(Boolean patched) {
        setTag(PATCHED, patched);
    }

    public String getLdcDuns() {
        return ldcDuns;
    }

    public void setLdcDuns(String ldcDuns) {
        this.ldcDuns = ldcDuns;
    }

    public String getDuDuns() {
        return duDuns;
    }

    public void setDuDuns(String duDuns) {
        this.duDuns = duDuns;
    }

    public String getGuDuns() {
        return guDuns;
    }

    public void setGuDuns(String guDuns) {
        this.guDuns = guDuns;
    }

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = getSchema(recordType);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(LATTICE_ACCOUNT_ID, latticeAccountId);
        builder.set(DUNS, duns);
        builder.set(DOMAIN, domain);
        builder.set(LDC_DUNS, ldcDuns);
        builder.set(DU_DUNS, duDuns);
        builder.set(GU_DUNS, guDuns);
        return builder.build();
    }

    @Override
    public AccountLookupEntry fromFabricAvroRecord(GenericRecord record) {
        setLatticeAccountId(record.get(LATTICE_ACCOUNT_ID) == null ? null
                : record.get(LATTICE_ACCOUNT_ID).toString());
        setDomain(record.get(DOMAIN) == null ? null : record.get(DOMAIN).toString());
        setDuns(record.get(DUNS) == null ? null : record.get(DUNS).toString());
        setLdcDuns(record.get(LDC_DUNS) == null ? null : record.get(LDC_DUNS).toString());
        setDuDuns(record.get(DU_DUNS) == null ? null : record.get(DU_DUNS).toString());
        setGuDuns(record.get(GU_DUNS) == null ? null : record.get(GU_DUNS).toString());
        return this;
    }

    @Override
    public AccountLookupEntry fromHdfsAvroRecord(GenericRecord record) {
        Object idObj = record.get(LATTICE_ACCOUNT_ID_HDFS);
        String latticeAccountId;
        if (idObj instanceof Utf8 || idObj instanceof String) {
            latticeAccountId = idObj.toString();
        } else {
            latticeAccountId = String.valueOf(idObj);
        }
        String key = record.get(KEY_HDFS).toString();
        AccountLookupEntry entry = fromKey(latticeAccountId, key);
        entry.setLdcDuns(record.get(LDC_DUNS) == null ? null : record.get(LDC_DUNS).toString());
        entry.setDuDuns(record.get(DU_DUNS) == null ? null : record.get(DU_DUNS).toString());
        entry.setGuDuns(record.get(GU_DUNS) == null ? null : record.get(GU_DUNS).toString());
        return entry;
    }

    private AccountLookupEntry fromKey(String latticeAccountId, String key) {
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
    public Schema getSchema(String recordType) {
        Preconditions.checkNotNull(recordType);
        return SchemaBuilder.record(replaceSpacialChars(recordType)).fields()//
                .name(LATTICE_ACCOUNT_ID).type(UNION_STRING_NULL_SCHEMA).noDefault() //
                .name(DOMAIN).type(UNION_STRING_NULL_SCHEMA).noDefault() //
                .name(DUNS).type(UNION_STRING_NULL_SCHEMA).noDefault() //
                .name(LDC_DUNS).type(UNION_NULL_STRING_SCHEMA).withDefault(null) //
                .name(DU_DUNS).type(UNION_NULL_STRING_SCHEMA).withDefault(null) //
                .name(GU_DUNS).type(UNION_NULL_STRING_SCHEMA).withDefault(null) //
                .endRecord();
    }

}
