package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CleanupActionConfiguration extends ActionConfiguration {

    public CleanupActionConfiguration() {
    }

    @JsonProperty("impact_entities")
    private List<BusinessEntity> impactEntities = new ArrayList<>();

    public List<BusinessEntity> getImpactEntities() {
        return impactEntities;
    }

    public void setImpactEntities(List<BusinessEntity> impactEntities) {
        this.impactEntities = impactEntities;
    }

    public void addImpactEntity(BusinessEntity impactEntity) {
        this.impactEntities.add(impactEntity);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public String serialize() {
        return toString();
    }
}
