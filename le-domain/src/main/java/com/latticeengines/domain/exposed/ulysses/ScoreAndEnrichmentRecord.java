package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Id;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

public class ScoreAndEnrichmentRecord implements FabricEntity<ScoreAndEnrichmentRecord> {
    
    public static final String LATTICE_REQUEST_ID = "latticeRequestId";
    public static final String SCORE = "score";
    public static final String ATTRIBUTES = "attributes";
    public static final String CAMPAIGN_IDS = "campaignIds";
    public static final String CAMPAIGN_ID_LIST = "campaignIdList";
    public static final String CAMPAIGN_ID = "campaignId";
    public static final String EXTERNAL_ID = "externalId";
    public static final String TENANT_ID = "tenantId";
    public static final String REQUEST_TIMESTAMP = "requestTimestamp";
    
    private static String[] schemas = buildSchemas();
    private static Schema avroSchema = null;

    @Id
    @JsonProperty(LATTICE_REQUEST_ID)
    private String id;
    
    @JsonProperty(SCORE)
    @DynamoAttribute(SCORE)
    private double score;
    
    @JsonProperty(ATTRIBUTES)
    private Map<String, Object> attributes = new HashMap<>();
    
    @JsonProperty(CAMPAIGN_IDS)
    @DynamoAttribute(CAMPAIGN_IDS)
    private List<String> campaignIds = new ArrayList<>();
    
    @JsonProperty(TENANT_ID)
    @DynamoAttribute(TENANT_ID)
    private String tenantId;
    
    @JsonProperty(EXTERNAL_ID)
    @DynamoAttribute(EXTERNAL_ID)
    private String externalId;
    
    @JsonProperty(REQUEST_TIMESTAMP)
    @DynamoAttribute(REQUEST_TIMESTAMP)
    private long requestTimestamp;
    
    public void setCampaignIds(List<String> campaignIds) {
        this.campaignIds = campaignIds;
    }
    
    public List<String> getCampaignIds() {
        return campaignIds;
    }
    
    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }
    
    public void setValue(String key, Object value) {
        attributes.put(key, value);
    }

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = getSchema(recordType);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(LATTICE_REQUEST_ID, getId());
        builder.set(TENANT_ID, getTenantId());
        builder.set(EXTERNAL_ID, getExternalId());
        builder.set(REQUEST_TIMESTAMP, getRequestTimestamp());
        builder.set(SCORE, getScore());
        try {
            String serializedAttributes = JsonUtils.serialize(getAttributes());
            builder.set(ATTRIBUTES, serializedAttributes);
            
            Schema listSchema = new Schema.Parser().parse(schemas[0]);
            
            List<GenericRecord> data = new ArrayList<>();
            for (String campaignId : campaignIds) {
                GenericRecord datum = new GenericData.Record(listSchema);
                datum.put(CAMPAIGN_ID, campaignId);
                data.add(datum);
            }
            builder.set(CAMPAIGN_IDS, data);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize attributes", e);
        }
        return builder.build();
    }

    @Override
    public Schema getSchema(String recordType) {
        if (avroSchema == null) {
            avroSchema = new Schema.Parser().parse(schemas[1]);
        }
        return avroSchema;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ScoreAndEnrichmentRecord fromFabricAvroRecord(GenericRecord record) {
        setId(record.get(LATTICE_REQUEST_ID).toString());
        setTenantId(record.get(TENANT_ID).toString());
        setExternalId(record.get(EXTERNAL_ID).toString());
        setRequestTimestamp((long) record.get(REQUEST_TIMESTAMP));
        setScore((double) record.get(SCORE));

        if (record.get(ATTRIBUTES) != null) {
            String serializedAttributes = record.get(ATTRIBUTES).toString();
            Map<String, Object> attributes = JsonUtils.deserialize(serializedAttributes, Map.class);
            setAttributes(attributes);
        }
        
        GenericData.Array<GenericRecord> campaignIdRecords = (Array<GenericRecord>) record.get(CAMPAIGN_IDS);
        List<String> cIds = new ArrayList<>();
        for (GenericRecord r : campaignIdRecords) {
            cIds.add(r.get(CAMPAIGN_ID).toString());
        }
        setCampaignIds(cIds);
        return this;
    }

    @Override
    public ScoreAndEnrichmentRecord fromHdfsAvroRecord(GenericRecord record) {
        return fromFabricAvroRecord(record);
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public long getRequestTimestamp() {
        return requestTimestamp;
    }

    public void setRequestTimestamp(long requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }

    protected static String[] buildSchemas() {
        String[] schemas = new String[2];
        
        schemas[0] = AvroUtils.buildSchema("com/latticeengines/domain/exposed/ulysses/ScoreAndEnrichmentListType.avsc", //
                CAMPAIGN_ID_LIST, CAMPAIGN_ID);
        schemas[1] = AvroUtils.buildSchema("com/latticeengines/domain/exposed/ulysses/ScoreAndEnrichmentType.avsc", //
                LATTICE_REQUEST_ID, TENANT_ID, EXTERNAL_ID, REQUEST_TIMESTAMP, SCORE, ATTRIBUTES, CAMPAIGN_IDS, schemas[0]);
        return schemas;
    }
    
}
