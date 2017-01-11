package com.latticeengines.domain.exposed.ulysses;

import java.util.List;

import javax.persistence.Transient;

import org.apache.avro.reflect.AvroIgnore;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.datafabric.DynamoHashKey;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class LatticeInsights implements HasInsights, HasName, HasTenant, HasId<String> {
    @JsonProperty
    @DynamoHashKey(name = "Id")
    private String tenantId;

    @JsonProperty
    @DynamoAttribute
    private String name;

    @JsonProperty
    @AvroIgnore
    private Tenant tenant;

    @JsonProperty
    @Transient
    @AvroIgnore
    private List<Insight> insights;

    @JsonIgnore
    @DynamoAttribute
    private List<InsightAttribute> insightModifiers;

    @Override
    public List<Insight> getInsights() {
        return insights;
    }

    @Override
    public void setInsights(List<Insight> insights) {
        this.insights = insights;
    }

    @Override
    public String getId() {
        return tenantId;
    }

    @Override
    public void setId(String id) {
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            this.tenantId = tenant.getId();
        } else {
            this.tenantId = null;
        }
    }

    @Override
    public List<InsightAttribute> getInsightModifiers() {
        return insightModifiers;
    }

    @Override
    public void setInsightModifiers(List<InsightAttribute> insightModifiers) {
        this.insightModifiers = insightModifiers;
    }
}
