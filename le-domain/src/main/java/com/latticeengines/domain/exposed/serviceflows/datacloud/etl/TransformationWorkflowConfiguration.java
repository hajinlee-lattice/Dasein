package com.latticeengines.domain.exposed.serviceflows.datacloud.etl;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.BaseDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PrepareTransformationStepInputConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.TransformationStepExecutionConfiguration;

public class TransformationWorkflowConfiguration extends BaseDataCloudWorkflowConfiguration {

    private static Map<String, Class<?>> stepConfigClasses = new HashMap<>();

    static {
        stepConfigClasses.put(PrepareTransformationStepInputConfiguration.class.getCanonicalName(),
                PrepareTransformationStepInputConfiguration.class);
        stepConfigClasses.put(TransformationStepExecutionConfiguration.class.getCanonicalName(),
                TransformationStepExecutionConfiguration.class);
    }

    @JsonIgnore
    @Override
    public Map<String, Class<?>> getStepConfigClasses() {
        return stepConfigClasses;
    }

    public static class Builder {
        private TransformationWorkflowConfiguration configuration = new TransformationWorkflowConfiguration();
        private PrepareTransformationStepInputConfiguration prepareConfig = new PrepareTransformationStepInputConfiguration();
        private TransformationStepExecutionConfiguration executeStep = new TransformationStepExecutionConfiguration();
        private CustomerSpace customerSpace;
        private String workflowName;
        private String payloadName;
        private String rootOperationUid;
        private String hdfsPodId;
        private TransformationConfiguration transformationConfiguration;
        private String internalResourceHostPort;
        private String serviceBeanName;
        private Integer mem;

        public TransformationWorkflowConfiguration build() {
            configuration.setContainerConfiguration(workflowName, customerSpace, payloadName);
            configuration.setContainerMemoryMB(mem);
            prepareConfig.setCustomerSpace(customerSpace);
            prepareConfig.setRootOperationUid(rootOperationUid);
            prepareConfig.setHdfsPodId(hdfsPodId);
            prepareConfig.setInternalResourceHostPort(internalResourceHostPort);
            prepareConfig.setTransformationConfiguration(JsonUtils.serialize(transformationConfiguration));

            prepareConfig.setServiceBeanName(serviceBeanName);
            prepareConfig
                    .setTransformationConfigurationClasspath(transformationConfiguration.getClass().getCanonicalName());
            configuration.add(prepareConfig);
            configuration.add(executeStep);

            return configuration;
        }

        public Builder rootOperationUid(String rootOperationUid) {
            this.rootOperationUid = rootOperationUid;
            return this;
        }

        public Builder hdfsPodId(String hdfsPodId) {
            this.hdfsPodId = hdfsPodId;
            return this;
        }

        public Builder transformationConfiguration(TransformationConfiguration transformationConfiguration) {
            this.transformationConfiguration = transformationConfiguration;
            return this;
        }

        public Builder customerSpace(CustomerSpace customerSpace) {
            this.customerSpace = customerSpace;
            return this;
        }

        public Builder workflowName(String workflowName) {
            this.workflowName = workflowName;
            return this;
        }

        public Builder payloadName(String payloadName) {
            this.payloadName = payloadName;
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            this.internalResourceHostPort = internalResourceHostPort;
            return this;
        }

        public Builder serviceBeanName(String serviceBeanName) {
            this.serviceBeanName = serviceBeanName;
            return this;
        }

        public Builder containerMemMB(Integer mem) {
            this.mem = mem;
            return this;
        }

    }

}
