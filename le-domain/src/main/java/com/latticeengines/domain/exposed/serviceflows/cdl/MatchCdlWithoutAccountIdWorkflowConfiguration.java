package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.MatchDataCloudWorkflowConfiguration;

public class MatchCdlWithoutAccountIdWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private MatchCdlWithoutAccountIdWorkflowConfiguration configuration = new MatchCdlWithoutAccountIdWorkflowConfiguration();

        MatchCdlAccountConfiguration matchCdlAccountConfiguration = new MatchCdlAccountConfiguration();
        MatchDataCloudWorkflowConfiguration.Builder ldcConfigurationBuilder = new MatchDataCloudWorkflowConfiguration.Builder();

        public MatchCdlWithoutAccountIdWorkflowConfiguration build() {
            configuration.add(matchCdlAccountConfiguration);
            configuration.add(ldcConfigurationBuilder.build());
            return configuration;
        }

    }
}
