package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;
import com.latticeengines.domain.exposed.dataplatform.HasName;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Insight implements HasName, FabricEntity<Insight> {
    
    public static final String NAME = "name";
    public static final String INSIGHT_SECTIONS = "insight_sections";
    
    private static Schema avroSchema;

    @JsonProperty(NAME)
    private String name;
    
    @JsonProperty(INSIGHT_SECTIONS)
    private List<InsightSection> insightSections = new ArrayList<>();

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
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
        builder.set(NAME, getName());
        try {
            List<GenericRecord> data = new ArrayList<>();
            for (InsightSection insightSection : getInsightSections()) {
                GenericRecord datum = insightSection.toFabricAvroRecord(recordType);
                data.add(datum);
            }
            builder.set(INSIGHT_SECTIONS, data);
            
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
    public Insight fromFabricAvroRecord(GenericRecord record) {
        setName((record.get(NAME).toString()));
        GenericData.Array<GenericRecord> insightSectionRecords = (Array<GenericRecord>) record.get(INSIGHT_SECTIONS);
        List<InsightSection> insightSections = new ArrayList<>();
        InsightSection dummy = new InsightSection();
        for (GenericRecord r : insightSectionRecords) {
            insightSections.add(dummy.fromFabricAvroRecord(r));
        }
        setInsightSections(insightSections);
        return this;
    }

    @Override
    public Insight fromHdfsAvroRecord(GenericRecord record) {
        // TODO Auto-generated method stub
        return null;
    }

    public static void setAvroSchema(Schema avroSchema) {
        Insight.avroSchema = avroSchema;
    }

    public List<InsightSection> getInsightSections() {
        return insightSections;
    }

    public void setInsightSections(List<InsightSection> insightSections) {
        this.insightSections = insightSections;
    }

}
