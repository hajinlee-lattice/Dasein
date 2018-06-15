package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @JsonProperty("deleted_records")
    private Map<BusinessEntity, Long> deletedRecords = new HashMap<>();

    public List<BusinessEntity> getImpactEntities() {
        return impactEntities;
    }

    public void setImpactEntities(List<BusinessEntity> impactEntities) {
        this.impactEntities = impactEntities;
    }

    public void addImpactEntity(BusinessEntity impactEntity) {
        this.impactEntities.add(impactEntity);
    }

    public Map<BusinessEntity, Long> getDeletedRecords() {
        return deletedRecords;
    }

    public void setDeletedRecords(Map<BusinessEntity, Long> deletedRecords) {
        this.deletedRecords = deletedRecords;
    }

    public void addDeletedRecords(BusinessEntity entity, Long deletedRecords) {
        this.deletedRecords.put(entity, deletedRecords);
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
