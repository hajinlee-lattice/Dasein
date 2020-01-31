package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchExportFilesToS3Configuration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchExportPublishToSNSConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchInitStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.ImportDeltaCalculationResultsFromS3StepConfiguration;

public class DeltaCampaignLaunchWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String RECOMMENDATION_WORKFLOW_REQUEST_ID = "RECOMMENDATION_WORKFLOW_REQUEST_ID";
    public static final String RECOMMENDATION_HDFS_EXPORT_FILE_PATHS = "RECOMMENDATION_HDFS_EXPORT_FILE_PATHS";
    public static final String RECOMMENDATION_S3_EXPORT_FILE_PATHS = "RECOMMENDATION_S3_EXPORT_FILE_PATHS";

    public static final String ADD = "add";
    public static final String DELETE = "delete";

    // avro file path in hdfs
    public static final String RECOMMENDATION_AVRO_HDFS_FILEPATH = "RECOMMENDATION_AVRO_HDFS_FILEPATH";
    public static final String ADD_CSV_EXPORT_AVRO_HDFS_FILEPATH = "ADD_CSV_EXPORT_AVRO_HDFS_FILEPATH";
    public static final String DELETE_CSV_EXPORT_AVRO_HDFS_FILEPATH = "DELETE_CSV_EXPORT_AVRO_HDFS_FILEPATH";
    // csv/json file path in hdfs after file generation
    public static final String ADD_CSV_EXPORT_FILES = "ADD_CSV_EXPORT_FILES";
    public static final String DELETE_CSV_EXPORT_FILES = "DELETE_CSV_EXPORT_FILES";
    // add and delete csv/json file paths in S3 in the format of map
    public static final String ADD_AND_DELETE_S3_EXPORT_FILES = "ADD_AND_DELETE_S3_EXPORT_FILES";

    public static final String DATA_FRAME_NUM = "DATA_FRAME_NUM";
    public static final String CREATE_RECOMMENDATION_DATA_FRAME = "CREATE_RECOMMENDATION_DATA_FRAME";
    public static final String CREATE_ADD_CSV_DATA_FRAME = "CREATE_ADD_CSV_DATA_FRAME";
    public static final String CREATE_DELETE_CSV_DATA_FRAME = "CREATE_DELETE_CSV_DATA_FRAME";

    public static class Builder {
        private DeltaCampaignLaunchWorkflowConfiguration configuration = new DeltaCampaignLaunchWorkflowConfiguration();
        private ImportDeltaCalculationResultsFromS3StepConfiguration importDeltaCalculationResultsFromS3Conf = new ImportDeltaCalculationResultsFromS3StepConfiguration();
        private DeltaCampaignLaunchInitStepConfiguration initStepConf = new DeltaCampaignLaunchInitStepConfiguration();
        private DeltaCampaignLaunchExportFilesGeneratorConfiguration exportFileGeneratorConf = new DeltaCampaignLaunchExportFilesGeneratorConfiguration();
        private DeltaCampaignLaunchExportFilesToS3Configuration exportFilesToS3Conf = new DeltaCampaignLaunchExportFilesToS3Configuration();
        private DeltaCampaignLaunchExportPublishToSNSConfiguration exportPublishToSNSConf = new DeltaCampaignLaunchExportPublishToSNSConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("deltaCampaignLaunchWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            importDeltaCalculationResultsFromS3Conf.setCustomerSpace(customerSpace);
            initStepConf.setCustomerSpace(customerSpace);
            exportFileGeneratorConf.setCustomerSpace(customerSpace);
            exportFilesToS3Conf.setCustomerSpace(customerSpace);
            exportPublishToSNSConf.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder dataCollectionVersion(DataCollection.Version version) {
            initStepConf.setDataCollectionVersion(version);
            return this;
        }

        public Builder playLaunch(PlayLaunch playLaunch) {
            importDeltaCalculationResultsFromS3Conf.setPlayId(playLaunch.getPlay().getName());
            importDeltaCalculationResultsFromS3Conf.setLaunchId(playLaunch.getLaunchId());
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
            exportFileGeneratorConf.setPlayName(playLaunch.getPlay().getName());
            exportFileGeneratorConf.setPlayLaunchId(playLaunch.getLaunchId());
            exportFilesToS3Conf.setPlayLaunchId(playLaunch.getLaunchId());
            exportPublishToSNSConf.setSkipStep(!canBeLaunchedToExternal);
            return this;
        }

        public Builder lookupIdMap(LookupIdMap lookupIdMap) {
            if (lookupIdMap == null) {
                return this;
            }

            ExternalSystemAuthentication externalAuth = lookupIdMap.getExternalAuthentication();
            if (lookupIdMap.getExternalSystemType() != CDLExternalSystemType.FILE_SYSTEM
                    && (externalAuth == null || StringUtils.isBlank(externalAuth.getTrayAuthenticationId())
                            || !externalAuth.getTrayWorkflowEnabled())) {
                exportFileGeneratorConf.setSkipStep(true);
                exportFilesToS3Conf.setSkipStep(true);
                exportPublishToSNSConf.setSkipStep(true);
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

        public DeltaCampaignLaunchWorkflowConfiguration build() {
            configuration.add(importDeltaCalculationResultsFromS3Conf);
            configuration.add(initStepConf);
            configuration.add(exportFileGeneratorConf);
            configuration.add(exportFilesToS3Conf);
            configuration.add(exportPublishToSNSConf);
            return configuration;
        }

    }
}
