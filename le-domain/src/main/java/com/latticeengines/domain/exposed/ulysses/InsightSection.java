package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class InsightSection implements FabricEntity<InsightSection> {
    
    public static final String DESCRIPTION = "description";
    public static final String HEADLINE = "headline";
    public static final String TIP = "tip";
    public static final String ATTRIBUTES = "attributes";
    public static final String INSIGHT_SOURCE_TYPE = "insight_source_type";
    
    private static Schema avroSchema = null;

    @JsonProperty(DESCRIPTION)
    private String description;
    
    @JsonProperty(HEADLINE)
    private String headline;
    
    @JsonProperty(TIP)
    private String tip;
    
    @JsonProperty(ATTRIBUTES)
    private List<String> attributes = new ArrayList<>();
    
    @JsonProperty(INSIGHT_SOURCE_TYPE)
    private InsightSourceType insightSourceType;


    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getHeadline() {
        return headline;
    }

    public void setHeadline(String headline) {
        this.headline = headline;
    }

    public String getTip() {
        return tip;
    }

    public void setTip(String tip) {
        this.tip = tip;
    }

    public InsightSourceType getInsightSourceType() {
        return insightSourceType;
    }

    public void setInsightSourceType(InsightSourceType insightSourceType) {
        this.insightSourceType = insightSourceType;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public void setId(String id) {
    }

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = getSchema(recordType);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(DESCRIPTION, getDescription());
        builder.set(HEADLINE, getHeadline());
        builder.set(TIP, getTip());
        builder.set(INSIGHT_SOURCE_TYPE, getInsightSourceType().name());
        try {
            String serializedAttributes = JsonUtils.serialize(getAttributes());
            builder.set(ATTRIBUTES, serializedAttributes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize attributes", e);
        }
        return builder.build();
    }

    @Override
    public Schema getSchema(String recordType) {
        return avroSchema;
    }

    @SuppressWarnings("unchecked")
    @Override
    public InsightSection fromFabricAvroRecord(GenericRecord record) {
        setDescription(record.get(DESCRIPTION).toString());
        setHeadline(record.get(HEADLINE).toString());
        setTip(record.get(TIP).toString());
        setInsightSourceType(InsightSourceType.valueOf(record.get(INSIGHT_SOURCE_TYPE).toString()));

        if (record.get(ATTRIBUTES) != null) {
            String serializedAttributes = record.get(ATTRIBUTES).toString();
            List<String> attributes = JsonUtils.deserialize(serializedAttributes, List.class);
            setAttributes(attributes);
        }
        
        return this;
    }

    @Override
    public InsightSection fromHdfsAvroRecord(GenericRecord record) {
        return fromFabricAvroRecord(record);
    }

    public static void setAvroSchema(Schema avroSchema) {
        InsightSection.avroSchema = avroSchema;
    }


}
