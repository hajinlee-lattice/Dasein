package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;

public class MatchCdlWithoutAccountIdWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private MatchCdlWithoutAccountIdWorkflowConfiguration configuration = new MatchCdlWithoutAccountIdWorkflowConfiguration();

        MatchCdlAccountConfiguration matchCdlAccountConfiguration = new MatchCdlAccountConfiguration();
        MatchCdlLdcWorkflowConfiguration.Builder ldcConfigurationBuilder = new MatchCdlLdcWorkflowConfiguration.Builder();

        public MatchCdlWithoutAccountIdWorkflowConfiguration build() {
            configuration.add(matchCdlAccountConfiguration);
            configuration.add(ldcConfigurationBuilder.build());
            return configuration;
        }

    }
}
