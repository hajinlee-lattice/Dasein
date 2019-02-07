package com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class PublishEntityConfiguration extends BaseStepConfiguration {

    @NotNull
    @JsonProperty("EntityPublishRequest")
    private EntityPublishRequest entityPublishRequest;

    public EntityPublishRequest getEntityPublishRequest() {
        return entityPublishRequest;
    }

    public void setEntityPublishRequest(EntityPublishRequest entityPublishRequest) {
        this.entityPublishRequest = entityPublishRequest;
    }
}
