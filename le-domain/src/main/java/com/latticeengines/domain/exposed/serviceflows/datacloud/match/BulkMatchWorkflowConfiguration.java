package com.latticeengines.domain.exposed.serviceflows.datacloud.match;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.serviceflows.datacloud.BaseDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.ParallelBlockExecutionConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PrepareBulkMatchInputConfiguration;

public class BulkMatchWorkflowConfiguration extends BaseDataCloudWorkflowConfiguration {

    private static Map<String, Class<?>> stepConfigClasses = new HashMap<>();

    static {
        stepConfigClasses.put(PrepareBulkMatchInputConfiguration.class.getCanonicalName(),
                PrepareBulkMatchInputConfiguration.class);
        stepConfigClasses.put(ParallelBlockExecutionConfiguration.class.getCanonicalName(),
                ParallelBlockExecutionConfiguration.class);
    }

    @JsonIgnore
    @Override
    public Map<String, Class<?>> getStepConfigClasses() {
        return stepConfigClasses;
    }

    public static class Builder {

        private BulkMatchWorkflowConfiguration configuration = new BulkMatchWorkflowConfiguration();
        private PrepareBulkMatchInputConfiguration prepareConfig = new PrepareBulkMatchInputConfiguration();
        private ParallelBlockExecutionConfiguration parallelExecConfig = new ParallelBlockExecutionConfiguration();
        private CustomerSpace customerSpace;

        public Builder rootOperationUid(String rootUid) {
            prepareConfig.setRootOperationUid(rootUid);
            return this;
        }

        public Builder hdfsPodId(String hdfsPodId) {
            prepareConfig.setHdfsPodId(hdfsPodId);
            parallelExecConfig.setPodId(hdfsPodId);
            return this;
        }

        public Builder matchInput(MatchInput matchInput) {
            customerSpace = CustomerSpace.parse(matchInput.getTenant().getId());
            prepareConfig.setCustomerSpace(customerSpace);
            parallelExecConfig.setCustomerSpace(customerSpace);
            parallelExecConfig.setResultLocation(matchInput.getMatchResultPath());
            prepareConfig.setMatchInput(matchInput);
            InputBuffer inputBuffer = matchInput.getInputBuffer();
            if (inputBuffer instanceof AvroInputBuffer) {
                AvroInputBuffer avroInputBuffer = (AvroInputBuffer) inputBuffer;
                prepareConfig.setInputAvroDir(avroInputBuffer.getAvroDir());
                prepareConfig.setInputAvroSchema(avroInputBuffer.getSchema());
            }
            prepareConfig.setYarnQueue(matchInput.getYarnQueue());
            prepareConfig.setRealTimeProxyUrl(matchInput.getRealTimeProxyUrl());
            prepareConfig.setRealTimeThreadPoolSize(matchInput.getRealTimeThreadPoolSize());
            return this;
        }

        public Builder averageBlockSize(Integer blockSize) {
            prepareConfig.setAverageBlockSize(blockSize);
            return this;
        }

        public Builder microserviceHostPort(String hostPort) {
            parallelExecConfig.setMicroServiceHostPort(hostPort);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public BulkMatchWorkflowConfiguration build() {
            configuration.setContainerConfiguration("bulkMatchWorkflow", customerSpace, "BulkMatchWorkflow");
            configuration.add(prepareConfig);
            configuration.add(parallelExecConfig);
            return configuration;
        }

    }

}
