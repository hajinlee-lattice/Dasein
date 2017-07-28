package com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class AWSBatchConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("bean_name")
    private String beanName;

    @JsonProperty("target_table_name")
    private String targetTableName;

    @JsonProperty("is_run_in_aws")
    private boolean runInAws;

    @JsonProperty("containerMemMB")
    private Integer containerMemoryMB;

    public boolean isRunInAws() {
        return runInAws;
    }

    public void setRunInAws(boolean runInAws) {
        this.runInAws = runInAws;
    }

    public Integer getContainerMemoryMB() {
        return containerMemoryMB;
    }

    public void setContainerMemoryMB(Integer containerMemoryMB) {
        this.containerMemoryMB = containerMemoryMB;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getBeanName() {
        return beanName;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
