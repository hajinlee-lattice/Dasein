package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.modeling.ModelingType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.SegmentExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class PrepareScoringAfterModelingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @JsonProperty("modelingType")
    private ModelingType modelingType;

    public ModelingType getModelingType() {
        return modelingType;
    }

    public void setModelingType(ModelingType modelingType) {
        this.modelingType = modelingType;
    }

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private PrepareScoringAfterModelingWorkflowConfiguration configuration = new PrepareScoringAfterModelingWorkflowConfiguration();

        private SetConfigurationForScoringConfiguration setConfigForScoring = new SetConfigurationForScoringConfiguration();
        private SegmentExportStepConfiguration initStepConf = new SegmentExportStepConfiguration();
        private MicroserviceStepConfiguration prepareSegmentMatching = new MicroserviceStepConfiguration();

        private MatchCdlAccountWorkflowConfiguration.Builder matchCdlWorkflowConfBuilder = new MatchCdlAccountWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            setConfigForScoring.setCustomerSpace(customerSpace);
            initStepConf.setCustomerSpace(customerSpace);
            prepareSegmentMatching.setCustomerSpace(customerSpace);
            matchCdlWorkflowConfBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            setConfigForScoring.setMicroServiceHostPort(microServiceHostPort);
            prepareSegmentMatching.setMicroServiceHostPort(microServiceHostPort);
            matchCdlWorkflowConfBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            setConfigForScoring.setInternalResourceHostPort(internalResourceHostPort);
            prepareSegmentMatching.setInternalResourceHostPort(internalResourceHostPort);
            matchCdlWorkflowConfBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder matchInputTableName(String tableName) {
            matchCdlWorkflowConfBuilder.matchInputTableName(tableName);
            return this;
        }

        public Builder matchAccountIdColumn(String matchAccountIdColumn) {
            matchCdlWorkflowConfBuilder.matchAccountIdColumn(matchAccountIdColumn);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            setConfigForScoring.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            setConfigForScoring.setInputProperties(inputProperties);
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder modelingType(ModelingType modelingType) {
            configuration.setModelingType(modelingType);
            return this;
        }

        public Builder metadataSegmentExport(MetadataSegmentExport metadataSegmentExport) {
            initStepConf.setMetadataSegmentExport(metadataSegmentExport);
            return this;
        }

        public PrepareScoringAfterModelingWorkflowConfiguration build() {
            configuration.setContainerConfiguration("prepareScoringAfterModelingWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(setConfigForScoring);
            configuration.add(initStepConf);
            configuration.add(prepareSegmentMatching);
            configuration.add(matchCdlWorkflowConfBuilder.build("customEventSimpleMatchWorkflow"));
            return configuration;
        }
    }

}
