package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessTransactionStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("actionIds")
    private List<Long> actionIds;
    @JsonProperty("data_qupta_limit")
    private Long dataQuotaLimit;

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Transaction;
    }

    public List<Long> getActionIds() {
        return actionIds;
    }

    public void setActionIds(List<Long> actionIds) {
        this.actionIds = actionIds;
    }

    public Long getDataQuotaLimit() {
        return dataQuotaLimit;
    }

    @Override
    public Long getDataQuotaLimit(ProductType type) {
        return null;
    }

    public void setDataQuotaLimit(Long dataQuotaLimit) {
        this.dataQuotaLimit = dataQuotaLimit;
    }
}
