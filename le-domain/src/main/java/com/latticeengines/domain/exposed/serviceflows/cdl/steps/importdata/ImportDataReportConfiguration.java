package com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;

public class ImportDataReportConfiguration extends BaseReportStepConfiguration {
    @JsonProperty("entity")
    private BusinessEntity entity;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }
}
