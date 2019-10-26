package com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

public class DeleteByUploadStepConfiguration extends BaseWrapperStepConfiguration {

    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("hard_delete_table_set")
    private Set<String> hardDeleteTableSet;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public Set<String> getHardDeleteTableSet() {
        return hardDeleteTableSet;
    }

    public void setHardDeleteTableSet(Set<String> hardDeleteTableSet) {
        this.hardDeleteTableSet = hardDeleteTableSet;
    }
}
