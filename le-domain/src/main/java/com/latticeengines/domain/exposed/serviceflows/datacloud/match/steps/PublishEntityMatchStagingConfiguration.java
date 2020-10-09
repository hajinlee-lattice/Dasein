package com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps;

import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class PublishEntityMatchStagingConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    private String customerSpace;

    @JsonProperty("versions")
    private Map<EntityMatchEnvironment, Integer> versions;

    @JsonProperty("entity_match_configuration")
    private EntityMatchConfiguration entityMatchConfiguration;

    @JsonProperty("entities")
    private Set<BusinessEntity> entities;

    public String getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }

    public Map<EntityMatchEnvironment, Integer> getVersions() {
        return versions;
    }

    public void setVersions(Map<EntityMatchEnvironment, Integer> versions) {
        this.versions = versions;
    }

    public EntityMatchConfiguration getEntityMatchConfiguration() {
        return entityMatchConfiguration;
    }

    public void setEntityMatchConfiguration(EntityMatchConfiguration entityMatchConfiguration) {
        this.entityMatchConfiguration = entityMatchConfiguration;
    }

    public Set<BusinessEntity> getEntities() {
        return entities;
    }

    public void setEntities(Set<BusinessEntity> entities) {
        this.entities = entities;
    }
}
