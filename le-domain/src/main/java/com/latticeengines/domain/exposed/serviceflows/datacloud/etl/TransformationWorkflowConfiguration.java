package com.latticeengines.domain.exposed.serviceflows.datacloud.etl;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.BaseDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PrepareTransformationStepInputConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.TransformationStepExecutionConfiguration;

public class TransformationWorkflowConfiguration extends BaseDataCloudWorkflowConfiguration {

    public static class Builder {
        private TransformationWorkflowConfiguration configuration = new TransformationWorkflowConfiguration();
        private PrepareTransformationStepInputConfiguration prepareConfig = new PrepareTransformationStepInputConfiguration();
        private TransformationStepExecutionConfiguration executeStep = new TransformationStepExecutionConfiguration();
        private CustomerSpace customerSpace;
        private String rootOperationUid;
        private String hdfsPodId;
        private TransformationConfiguration transformationConfiguration;
        private String internalResourceHostPort;
        private String serviceBeanName;
        private Integer mem;

        public TransformationWorkflowConfiguration build() {
            configuration.setContainerConfiguration("transformationWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            configuration.setContainerMemoryMB(mem);
            prepareConfig.setCustomerSpace(customerSpace);
            prepareConfig.setRootOperationUid(rootOperationUid);
            prepareConfig.setHdfsPodId(hdfsPodId);
            prepareConfig.setInternalResourceHostPort(internalResourceHostPort);
            if (transformationConfiguration != null) {
                prepareConfig.setTransformationConfiguration(
                        JsonUtils.serialize(transformationConfiguration));
                prepareConfig.setTransformationConfigurationClasspath(
                        transformationConfiguration.getClass().getCanonicalName());
            }

            prepareConfig.setServiceBeanName(serviceBeanName);

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

        public Builder transformationConfiguration(
                TransformationConfiguration transformationConfiguration) {
            this.transformationConfiguration = transformationConfiguration;
            return this;
        }

        public Builder customerSpace(CustomerSpace customerSpace) {
            this.customerSpace = customerSpace;
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
