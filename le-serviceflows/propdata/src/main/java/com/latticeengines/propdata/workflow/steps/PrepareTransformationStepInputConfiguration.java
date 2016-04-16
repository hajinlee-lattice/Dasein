package com.latticeengines.propdata.workflow.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class PrepareTransformationStepInputConfiguration extends BaseStepConfiguration {

    @NotEmptyString
    @NotNull
    private String transformationConfiguration;

    @NotEmptyString
    @NotNull
    private String transformationConfigurationClasspath;

    @NotNull
    private CustomerSpace customerSpace;

    @NotEmptyString
    @NotNull
    private String rootOperationUid;

    @NotEmptyString
    @NotNull
    private String hdfsPodId;

    @JsonProperty("customer_space")
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customer_space")
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    @JsonProperty("root_operation_uid")
    public String getRootOperationUid() {
        return rootOperationUid;
    }

    @JsonProperty("root_operation_uid")
    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }

    @JsonProperty("hdfs_pod_id")
    public String getHdfsPodId() {
        return hdfsPodId;
    }

    @JsonProperty("hdfs_pod_id")
    public void setHdfsPodId(String hdfsPodId) {
        this.hdfsPodId = hdfsPodId;
    }

    @JsonProperty("transformation_configuration")
    public String getTransformationConfiguration() {
        return transformationConfiguration;
    }

    @JsonProperty("transformation_configuration")
    public void setTransformationConfiguration(String transformationConfiguration) {
        this.transformationConfiguration = transformationConfiguration;
    }

    @JsonProperty("transformation_configuration_classpath")
    public String getTransformationConfigurationClasspath() {
        return transformationConfigurationClasspath;
    }

    @JsonProperty("transformation_configuration_classpath")
    public void setTransformationConfigurationClasspath(String transformationConfigurationClasspath) {
        this.transformationConfigurationClasspath = transformationConfigurationClasspath;
    }
}
