package com.latticeengines.domain.exposed.serviceflows.dcp;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLDataFeedImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.DCPExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;

public class DCPSourceImportWorkflowConfiguration extends BaseDCPWorkflowConfiguration {

    public static final String WORKFLOW_NAME = "dcpSourceImportWorkflow";
    public static final String UPLOAD_ID = "UPLOAD_ID";
    public static final String SOURCE_ID = "SOURCE_ID";
    public static final String PROJECT_ID = "PROJECT_ID";

    public DCPSourceImportWorkflowConfiguration() {
    }

    public static class Builder {
        private DCPSourceImportWorkflowConfiguration configuration = new DCPSourceImportWorkflowConfiguration();

        private ImportSourceStepConfiguration importSourceStepConfiguration = new ImportSourceStepConfiguration();
        private DCPExportStepConfiguration exportS3StepConfiguration = new DCPExportStepConfiguration();
        private MatchDataCloudWorkflowConfiguration.Builder matchConfig = new MatchDataCloudWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importSourceStepConfiguration.setCustomerSpace(customerSpace);
            exportS3StepConfiguration.setCustomerSpace(customerSpace);
            matchConfig.customer(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importSourceStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            exportS3StepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importSourceStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            exportS3StepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder projectId(String projectId) {
            importSourceStepConfiguration.setProjectId(projectId);
            exportS3StepConfiguration.setProjectId(projectId);
            return this;
        }

        public Builder sourceId(String sourceId) {
            importSourceStepConfiguration.setSourceId(sourceId);
            exportS3StepConfiguration.setSourceId(sourceId);
            return this;
        }

        public Builder uploadPid(Long uploadPid) {
            importSourceStepConfiguration.setUploadPid(uploadPid);
            exportS3StepConfiguration.setUploadPid(uploadPid);
            return this;
        }

        public Builder statsPid(long statsPid) {
            importSourceStepConfiguration.setStatsPid(statsPid);
            return this;
        }

        // BEGIN: Match
        public Builder dataCloudVersion(String version) {
            matchConfig.dataCloudVersion(version);
            return this;
        }

        public Builder queue(String queue) {
            matchConfig.matchQueue(queue);
            return this;
        }
        // END: Match

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public DCPSourceImportWorkflowConfiguration build() {
            configuration.setContainerConfiguration(WORKFLOW_NAME, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(importSourceStepConfiguration);
            configuration.add(exportS3StepConfiguration);

            matchConfig.joinWithInternalId(true);
            matchConfig.fetchOnly(false);
            matchConfig.matchType(MatchStepConfiguration.DCP);
            matchConfig.matchRequestSource(MatchRequestSource.ENRICHMENT);
            matchConfig.excludePublicDomains(false);
            matchConfig.matchColumnSelection(getDCPEnrichAttrs());
            matchConfig.sourceSchemaInterpretation(SchemaInterpretation.SalesforceAccount.name());
            configuration.add(matchConfig.build());

            return configuration;
        }

        //FIXME: in alpha release, use a hard coded enrich list
        private List<String> getDCPEnrichAttrs() {
            return Arrays.asList(
                    DataCloudConstants.ATTR_LDC_DUNS,
                    DataCloudConstants.ATTR_LDC_NAME,
                    "TRADESTYLE_NAME",
                    "LDC_Street",
                    "STREET_ADDRESS_2",
                    DataCloudConstants.ATTR_CITY,
                    DataCloudConstants.ATTR_STATE,
                    DataCloudConstants.ATTR_ZIPCODE,
                    DataCloudConstants.ATTR_COUNTRY,
                    "TELEPHONE_NUMBER",
                    "LE_SIC_CODE"
            );
        }
    }
}
