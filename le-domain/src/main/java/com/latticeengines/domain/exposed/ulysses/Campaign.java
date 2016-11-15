package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "ULYSSES_CAMPAIGN", //
    uniqueConstraints = { @UniqueConstraint(columnNames = { "TENANT_ID", "NAME" }) })
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Campaign implements HasPid, HasName, HasTenantId, FabricEntity<Campaign> {
    
    public static final String NAME = "name";
    public static final String INSIGHTS = "insights";
    public static final String CAMPAIGN_ID = "campaign_id";
    
    private static String[] schemas;
    private static Schema avroSchema;
    
    static {
        schemas = buildSchemas();
        
        if (schemas[2] != null) {
            avroSchema = new Schema.Parser().parse(schemas[2]);
        }
        
        if (schemas[1] != null) {
            Insight.setAvroSchema(new Schema.Parser().parse(schemas[1]));
        }
        
        if (schemas[0] != null) {
            InsightSection.setAvroSchema(new Schema.Parser().parse(schemas[0]));
        }
    }

    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty(NAME)
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("campaign_type")
    @Column(name = "CAMPAIGN_TYPE", nullable = false)
    private CampaignType campaignType;

    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;
    
    @Column(name = "LAUNCHED", nullable = false)
    private boolean launched = false;

    @JsonProperty("segments")
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "ULYSSES_CAMPAIGN_SEGMENTS", joinColumns = @JoinColumn(name = "PID"))
    @Column(name = "SEGMENT_NAME")
    private List<String> segments = new ArrayList<>();
    
    @JsonProperty(INSIGHTS)
    @DynamoAttribute(INSIGHTS)
    @Transient
    private List<Insight> insights = new ArrayList<>();

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;
    
    @JsonProperty(CAMPAIGN_ID)
    @Column(name = "CAMPAIGN_ID", nullable = false)
    private String campaignId;
    
    public Campaign() {
        if (campaignId == null) {
            campaignId = UUID.randomUUID().toString();
        }
    }
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public CampaignType getCampaignType() {
        return campaignType;
    }

    public void setCampaignType(CampaignType campaignType) {
        this.campaignType = campaignType;
    }

    public List<String> getSegments() {
        return segments;
    }

    public void setSegments(List<String> segments) {
        this.segments = segments;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    public boolean isLaunched() {
        return launched;
    }

    public void setLaunched(boolean launched) {
        this.launched = launched;
    }
    
    public void setTenant(Tenant tenant) {
        if (tenant != null) {
            setTenantId(tenant.getPid());
            this.tenant = tenant;
        }
    }
    
    public Tenant getTenant() {
        return tenant;
    }

    public List<Insight> getInsights() {
        return insights;
    }

    public void setInsights(List<Insight> insights) {
        this.insights = insights;
    }

    @Override
    public String getId() {
        return campaignId;
    }

    @Override
    public void setId(String id) {
        this.campaignId = id;
    }

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = getSchema(recordType);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(NAME, getName());
        try {
            List<GenericRecord> data = new ArrayList<>();
            for (Insight insight : getInsights()) {
                GenericRecord datum = insight.toFabricAvroRecord(recordType);
                data.add(datum);
            }
            builder.set(INSIGHTS, data);
            
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
    public Campaign fromFabricAvroRecord(GenericRecord record) {
        setName((record.get(NAME).toString()));
        GenericData.Array<GenericRecord> insightRecords = (Array<GenericRecord>) record.get(INSIGHTS);
        List<Insight> insights = new ArrayList<>();
        Insight dummy = new Insight();
        for (GenericRecord r : insightRecords) {
            insights.add(dummy.fromFabricAvroRecord(r));
        }
        setInsights(insights);
        return this;
    }

    @Override
    public Campaign fromHdfsAvroRecord(GenericRecord record) {
        return fromFabricAvroRecord(record);
    }

    protected static String[] buildSchemas() {
        String[] schemas = new String[3];
        
        schemas[0] = AvroUtils.buildSchema("com/latticeengines/domain/exposed/ulysses/InsightSectionListType.avsc", //
                InsightSection.DESCRIPTION, InsightSection.HEADLINE, InsightSection.TIP, //
                InsightSection.ATTRIBUTES, InsightSection.INSIGHT_SOURCE_TYPE);
        schemas[1] = AvroUtils.buildSchema("com/latticeengines/domain/exposed/ulysses/InsightListType.avsc", //
                Insight.NAME, Insight.INSIGHT_SECTIONS, schemas[0]);
        schemas[2] = AvroUtils.buildSchema("com/latticeengines/domain/exposed/ulysses/CampaignType.avsc", //
                Campaign.NAME, Campaign.INSIGHTS, schemas[1]);
        return schemas;
    }
}
