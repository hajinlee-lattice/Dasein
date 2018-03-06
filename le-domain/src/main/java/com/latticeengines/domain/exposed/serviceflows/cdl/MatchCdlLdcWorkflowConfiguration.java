package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class MatchCdlLdcWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.DataCloud.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private MatchCdlLdcWorkflowConfiguration configuration = new MatchCdlLdcWorkflowConfiguration();

        private MatchStepConfiguration matchStepConfiguration = new MatchStepConfiguration();
        private BulkMatchWorkflowConfiguration.Builder bulkMatchWorkflowConfigurationBuilder = new BulkMatchWorkflowConfiguration.Builder();
        private ProcessMatchResultConfiguration processMatchResultConfiguration = new ProcessMatchResultConfiguration();

        public MatchCdlLdcWorkflowConfiguration build() {
            configuration.add(matchStepConfiguration);
            configuration.add(bulkMatchWorkflowConfigurationBuilder.build());
            configuration.add(processMatchResultConfiguration);

            return configuration;
        }
    }
}
