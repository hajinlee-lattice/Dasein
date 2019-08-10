package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Arrays;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchExportFilesToS3Configuration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchExportPublishToSNSConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CampaignLaunchInitStepConfiguration;

public class CampaignLaunchWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String RECOMMENDATION_AVRO_HDFS_FILEPATH = "RECOMMENDATION_AVRO_HDFS_FILEPATH";
    public static final String RECOMMENDATION_EXPORT_FILES = "RECOMMENDATION_EXPORT_FILES";
    public static final String RECOMMENDATION_WORKFLOW_REQUEST_ID = "RECOMMENDATION_WORKFLOW_REQUEST_ID";
    public static final String RECOMMENDATION_S3_EXPORT_FILE_PATHS = "RECOMMENDATION_S3_EXPORT_FILE_PATHS";

    public static class Builder {
        private CampaignLaunchWorkflowConfiguration configuration = new CampaignLaunchWorkflowConfiguration();
        private ImportExportS3StepConfiguration importS3 = new ImportExportS3StepConfiguration();
        private CampaignLaunchInitStepConfiguration initStepConf = new CampaignLaunchInitStepConfiguration();
        private PlayLaunchExportFilesGeneratorConfiguration exportFileGeneratorConf = new PlayLaunchExportFilesGeneratorConfiguration();
        private PlayLaunchExportFilesToS3Configuration exportFilesToS3Conf = new PlayLaunchExportFilesToS3Configuration();
        private PlayLaunchExportPublishToSNSConfiguration exportPublishToSNSConf = new PlayLaunchExportPublishToSNSConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("campaignLaunchWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            importS3.setCustomerSpace(customerSpace);
            initStepConf.setCustomerSpace(customerSpace);
            exportFileGeneratorConf.setCustomerSpace(customerSpace);
            exportFilesToS3Conf.setCustomerSpace(customerSpace);
            exportPublishToSNSConf.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder dataCollectionVersion(DataCollection.Version version) {
            importS3.setVersion(version);
            initStepConf.setDataCollectionVersion(version);
            return this;
        }

        public Builder playLaunch(PlayLaunch playLaunch) {
            initStepConf.setPlayName(playLaunch.getPlay().getName());
            initStepConf.setPlayLaunchId(playLaunch.getLaunchId());
            configuration.setUserId(playLaunch.getPlay().getCreatedBy());
            exportFileGeneratorConf.setChannelConfig(playLaunch.getChannelConfig());
            exportFilesToS3Conf.setPlayName(playLaunch.getPlay().getName());
            exportFilesToS3Conf.setPlayDisplayName(playLaunch.getPlay().getDisplayName());
            exportPublishToSNSConf.setExternalFolderName(playLaunch.getFolderName());
            exportPublishToSNSConf.setExternalAudienceId(playLaunch.getAudienceId());
            exportPublishToSNSConf.setExternalAudienceName(playLaunch.getAudienceName());
            exportPublishToSNSConf.setChannelConfig(playLaunch.getChannelConfig());
            return this;
        }

        public Builder exportPublishPlayLaunch(PlayLaunch playLaunch, boolean canBeLaunchedToExternal) {
            if (!Arrays.asList(CDLExternalSystemType.MAP, CDLExternalSystemType.ADS, CDLExternalSystemType.FILE_SYSTEM)
                    .contains(playLaunch.getDestinationSysType())) {
                exportFileGeneratorConf.setSkipStep(true);
                exportFilesToS3Conf.setSkipStep(true);
                exportPublishToSNSConf.setSkipStep(true);
                return this;
            }

            exportFileGeneratorConf.setPlayName(playLaunch.getPlay().getName());
            exportFileGeneratorConf.setPlayLaunchId(playLaunch.getLaunchId());
            exportFilesToS3Conf.setPlayLaunchId(playLaunch.getLaunchId());

            if (!canBeLaunchedToExternal) {
                exportPublishToSNSConf.setSkipStep(true);
                return this;
            }
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
            exportPublishToSNSConf.setLookupIdMap(lookupIdMap);
            return this;
        }

        public Builder playLaunchDestination(CDLExternalSystemType destination) {
            exportFilesToS3Conf.setPlayLaunchDestination(destination);
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

        public Builder accountAttributeExportDiplayNames(Map<String, String> accountExportDisplayNames) {
            exportFileGeneratorConf.setAccountDisplayNames(accountExportDisplayNames);
            return this;
        }

        public Builder contactAttributeExportDiplayNames(Map<String, String> contactExportDisplayNames) {
            exportFileGeneratorConf.setContactDisplayNames(contactExportDisplayNames);
            return this;
        }

        public CampaignLaunchWorkflowConfiguration build() {
            configuration.add(importS3);
            configuration.add(initStepConf);
            configuration.add(exportFileGeneratorConf);
            configuration.add(exportFilesToS3Conf);
            configuration.add(exportPublishToSNSConf);
            return configuration;
        }

    }
}
