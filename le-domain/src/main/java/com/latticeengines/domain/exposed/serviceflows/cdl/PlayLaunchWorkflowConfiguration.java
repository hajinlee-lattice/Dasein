package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchExportFilesToS3Configuration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;

public class PlayLaunchWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String RECOMMENDATION_AVRO_HDFS_FILEPATH = "RECOMMENDATION_AVRO_HDFS_FILEPATH";
    public static final String RECOMMENDATION_EXPORT_FILES = "RECOMMENDATION_EXPORT_FILES";

    public static class Builder {
        private PlayLaunchWorkflowConfiguration configuration = new PlayLaunchWorkflowConfiguration();
        private PlayLaunchInitStepConfiguration initStepConf = new PlayLaunchInitStepConfiguration();
        private PlayLaunchExportFilesGeneratorConfiguration exportFileGeneratorConf = new PlayLaunchExportFilesGeneratorConfiguration();
        private PlayLaunchExportFilesToS3Configuration exportFilesToS3Conf = new PlayLaunchExportFilesToS3Configuration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("playLaunchWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            initStepConf.setCustomerSpace(customerSpace);
            exportFileGeneratorConf.setCustomerSpace(customerSpace);
            exportFilesToS3Conf.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder playLaunch(PlayLaunch playLaunch) {
            initStepConf.setPlayName(playLaunch.getPlay().getName());
            initStepConf.setPlayLaunchId(playLaunch.getLaunchId());
            configuration.setUserId(playLaunch.getPlay().getCreatedBy());
            exportFilesToS3Conf.setExternalAudienceId(playLaunch.getAudienceId());
            return this;
        }

        public Builder exportPlayLaunch(PlayLaunch playLaunch, boolean canBeLaunchedToExternal) {
            if (!canBeLaunchedToExternal) {
                exportFilesToS3Conf.setSkipStep(true);
                exportFileGeneratorConf.setSkipStep(true);
                return this;
            }
            exportFileGeneratorConf.setPlayName(playLaunch.getPlay().getName());
            exportFileGeneratorConf.setPlayLaunchId(playLaunch.getLaunchId());
            exportFilesToS3Conf.setPlayName(playLaunch.getPlay().getName());
            exportFilesToS3Conf.setPlayLaunchId(playLaunch.getLaunchId());
            return this;
        }
        
        public Builder lookupIdMap(LookupIdMap lookupIdMap) {
            if (lookupIdMap == null) {
                return this;
            }
            exportFileGeneratorConf.setDestinationSysType(lookupIdMap.getExternalSystemType());
            exportFileGeneratorConf.setDestinationOrgId(lookupIdMap.getOrgId());
            exportFileGeneratorConf.setDestinationSysName(lookupIdMap.getExternalSystemName());
            exportFilesToS3Conf.setLookupIdMap(lookupIdMap);
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
            configuration.add(exportFileGeneratorConf);
            configuration.add(exportFilesToS3Conf);
            return configuration;
        }

    }
}
