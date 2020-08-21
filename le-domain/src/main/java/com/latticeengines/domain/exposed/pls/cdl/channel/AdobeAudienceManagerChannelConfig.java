package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AdobeAudienceManagerChannelConfig extends LiveRampChannelConfig {
    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Adobe_Audience_Mgr;

    @Override
    public CDLExternalSystemName getSystemName() {
        return systemName;
    }

    @Override
    public boolean shouldResetDeltaCalculations(ChannelConfig channelConfig) {
        if (!(channelConfig instanceof AdobeAudienceManagerChannelConfig)) {
            return false;
        }

        return super.shouldResetDeltaCalculations(channelConfig);
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        AdobeAudienceManagerChannelConfig channelConfig = this;
        AdobeAudienceManagerChannelConfig newChannelConfig = (AdobeAudienceManagerChannelConfig) config;

        channelConfig.setAudienceId(newChannelConfig.getAudienceId());
        channelConfig.setAudienceName(newChannelConfig.getAudienceName());
        channelConfig.setAccountLimit(newChannelConfig.getAccountLimit());
        channelConfig.setJobFunctions(newChannelConfig.getJobFunctions());
        channelConfig.setJobLevels(newChannelConfig.getJobLevels());
        return this;
    }
}
