package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;

public class MatchCdlWithAccountIdWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private MatchCdlWithAccountIdWorkflowConfiguration configuration = new MatchCdlWithAccountIdWorkflowConfiguration();

        MatchCdlAccountConfiguration matchCdlAccountConfiguration = new MatchCdlAccountConfiguration();
        MatchCdlLdcWorkflowConfiguration.Builder ldcConfigurationBuilder = new MatchCdlLdcWorkflowConfiguration.Builder();

        public MatchCdlWithAccountIdWorkflowConfiguration build() {
            configuration.add(matchCdlAccountConfiguration);
            configuration.add(ldcConfigurationBuilder.build());
            return configuration;
        }
    }
}
