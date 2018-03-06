package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.modeling.ModelingType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class CustomEventMatchWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.DataCloud.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    @JsonProperty("modelingType")
    private ModelingType modelingType;

    public ModelingType getModelingType() {
        return modelingType;
    }

    public void setModelingType(ModelingType modelingType) {
        this.modelingType = modelingType;
    }

    public static class Builder {
        private CustomEventMatchWorkflowConfiguration configuration = new CustomEventMatchWorkflowConfiguration();

        private MatchCdlWithAccountIdWorkflowConfiguration.Builder matchWithAccountIdBuilder = new MatchCdlWithAccountIdWorkflowConfiguration.Builder();
        private MatchCdlWithoutAccountIdWorkflowConfiguration.Builder matchWithoutAccountIdBuilder = new MatchCdlWithoutAccountIdWorkflowConfiguration.Builder();
        private MatchCdlStepConfiguration matchCdlStepConfiguration = new MatchCdlStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("customEventMatchingWorkflowConfiguration", customerSpace,
                    configuration.getClass().getSimpleName());

            return this;
        }

        public Builder modelingType(ModelingType modelingType) {
            configuration.setModelingType(modelingType);
            return this;
        }

        public CustomEventMatchWorkflowConfiguration build() {
            configuration.add(matchWithAccountIdBuilder.build());
            configuration.add(matchWithoutAccountIdBuilder.build());
            configuration.add(matchCdlStepConfiguration);

            return configuration;
        }
    }
}
