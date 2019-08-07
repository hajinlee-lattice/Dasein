package com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

public class ConvertBatchStoreStepConfiguration extends BaseWrapperStepConfiguration {

    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("convert_service_config")
    private BaseConvertBatchStoreServiceConfiguration convertServiceConfig;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public BaseConvertBatchStoreServiceConfiguration getConvertServiceConfig() {
        return convertServiceConfig;
    }

    public void setConvertServiceConfig(BaseConvertBatchStoreServiceConfiguration convertServiceConfig) {
        this.convertServiceConfig = convertServiceConfig;
    }
}
