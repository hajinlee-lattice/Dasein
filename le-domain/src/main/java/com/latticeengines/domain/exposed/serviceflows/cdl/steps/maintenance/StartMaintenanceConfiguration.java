package com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class StartMaintenanceConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("entity")
    private BusinessEntity entity;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    @JsonIgnore
    public List<String> getEntityList() {
        if (entity != null) {
            return Arrays.asList(entity.name());
        } else {
            List<String> entities = new ArrayList<>();
            entities.add(BusinessEntity.Account.name());
            entities.add(BusinessEntity.Contact.name());
            entities.add(BusinessEntity.Product.name());
            entities.add(BusinessEntity.Transaction.name());
            return entities;
        }
    }
}
