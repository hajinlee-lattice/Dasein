package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessProductStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("data_qupta_limit")
    private Long dataQuotaLimit;

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Product;
    }

    public Long getDataQuotaLimit() {
        return dataQuotaLimit;
    }

    public void setDataQuotaLimit(Long dataQuotaLimit) {
        this.dataQuotaLimit = dataQuotaLimit;
    }
}
