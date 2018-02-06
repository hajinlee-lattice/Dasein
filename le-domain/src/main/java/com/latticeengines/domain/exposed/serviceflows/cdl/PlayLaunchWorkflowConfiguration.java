package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;

public class PlayLaunchWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private PlayLaunchWorkflowConfiguration configuration = new PlayLaunchWorkflowConfiguration();
        private PlayLaunchInitStepConfiguration initStepConf = new PlayLaunchInitStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("playLaunchWorkflow", customerSpace, "playLaunchWorkflow");
            initStepConf.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder playLaunch(PlayLaunch playLaunch) {
            initStepConf.setPlayName(playLaunch.getPlay().getName());
            initStepConf.setPlayLaunchId(playLaunch.getLaunchId());
            configuration.setUserId(playLaunch.getPlay().getCreatedBy());
            return this;
        }

        public Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(workflowName);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public PlayLaunchWorkflowConfiguration build() {
            configuration.add(initStepConf);
            return configuration;
        }

    }
}
