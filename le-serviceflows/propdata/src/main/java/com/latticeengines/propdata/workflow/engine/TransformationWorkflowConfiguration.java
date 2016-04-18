package com.latticeengines.propdata.workflow.engine;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.workflow.engine.steps.PrepareTransformationStepInputConfiguration;
import com.latticeengines.propdata.workflow.engine.steps.TransformationStepExecutionConfiguration;

public class TransformationWorkflowConfiguration extends WorkflowConfiguration {

    public static class Builder {
        private ObjectMapper om = new ObjectMapper();
        private TransformationWorkflowConfiguration configuration = new TransformationWorkflowConfiguration();
        private PrepareTransformationStepInputConfiguration prepareConfig = new PrepareTransformationStepInputConfiguration();
        private TransformationStepExecutionConfiguration parallelExecConfig = new TransformationStepExecutionConfiguration();
        private CustomerSpace customerSpace;
        private String workflowName;
        private String payloadName;
        private String rootOperationUid;
        private String hdfsPodId;
        private TransformationConfiguration transformationConfiguration;
        private String internalResourceHostPort;
        private String serviceBeanName;

        public TransformationWorkflowConfiguration build() {
            configuration.setContainerConfiguration(workflowName, customerSpace, payloadName);
            prepareConfig.setCustomerSpace(customerSpace);
            prepareConfig.setRootOperationUid(rootOperationUid);
            prepareConfig.setHdfsPodId(hdfsPodId);
            prepareConfig.setInternalResourceHostPort(internalResourceHostPort);
            try {
                prepareConfig.setTransformationConfiguration(om.writeValueAsString(transformationConfiguration));
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_25013, e.getMessage(), e);
            }
            prepareConfig.setServiceBeanName(serviceBeanName);
            prepareConfig
                    .setTransformationConfigurationClasspath(transformationConfiguration.getClass().getCanonicalName());
            configuration.add(prepareConfig);
            configuration.add(parallelExecConfig);

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

    }

}
