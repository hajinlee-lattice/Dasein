package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.MatchDataCloudWorkflowConfiguration;

public class MatchCdlWithAccountIdWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private MatchCdlWithAccountIdWorkflowConfiguration configuration = new MatchCdlWithAccountIdWorkflowConfiguration();

        MatchCdlAccountConfiguration matchCdlAccountConfiguration = new MatchCdlAccountConfiguration();
        MatchDataCloudWorkflowConfiguration.Builder ldcConfigurationBuilder = new MatchDataCloudWorkflowConfiguration.Builder();

        public MatchCdlWithAccountIdWorkflowConfiguration build() {
            configuration.add(matchCdlAccountConfiguration);
            configuration.add(ldcConfigurationBuilder.build());
            return configuration;
        }
    }
}
