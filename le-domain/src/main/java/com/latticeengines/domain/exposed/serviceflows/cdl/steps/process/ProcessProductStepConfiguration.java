package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessProductStepConfiguration extends BaseProcessEntityStepConfiguration {


    @JsonProperty("data_quota_limit")
    private Map<ProductType, Long> dataQuotaLimit = new HashMap<>();

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Product;
    }

    public Long getDataQuotaLimit() {
        return null;
    }

    public Long getDataQuotaLimit(ProductType type) {
        return dataQuotaLimit.get(type);
    }

    public void setDataQuotaLimit(ProductType type, Long quotaLimit) {
        this.dataQuotaLimit.put(type, quotaLimit);
    }
}
