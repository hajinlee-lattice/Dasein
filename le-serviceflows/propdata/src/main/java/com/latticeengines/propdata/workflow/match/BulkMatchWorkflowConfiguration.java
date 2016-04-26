package com.latticeengines.propdata.workflow.match;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.propdata.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.propdata.match.InputBuffer;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.propdata.workflow.match.steps.ParallelBlockExecutionConfiguration;
import com.latticeengines.propdata.workflow.match.steps.PrepareBulkMatchInputConfiguration;

public class BulkMatchWorkflowConfiguration extends WorkflowConfiguration {

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
            prepareConfig.setKeyMap(matchInput.getKeyMap());
            InputBuffer inputBuffer = matchInput.getInputBuffer();
            if (inputBuffer instanceof AvroInputBuffer) {
                AvroInputBuffer avroInputBuffer = (AvroInputBuffer) inputBuffer;
                prepareConfig.setInputAvroDir(avroInputBuffer.getAvroDir());
            }
            prepareConfig.setPredefinedSelection(matchInput.getPredefinedSelection());
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

        public Builder returnUnmatched(Boolean returnUnmatched) {
            prepareConfig.setReturnUnmatched(returnUnmatched);
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
