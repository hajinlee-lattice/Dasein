package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GoogleDisplayNVideo360ChannelConfig extends LiveRampChannelConfig {
    private static final CDLExternalSystemName systemName = CDLExternalSystemName.Google_Display_N_Video_360;

    @Override
    public CDLExternalSystemName getSystemName() {
        return systemName;
    }

    @Override
    public boolean shouldResetDeltaCalculations(ChannelConfig channelConfig) {
        if (!(channelConfig instanceof GoogleDisplayNVideo360ChannelConfig)) {
            return false;
        }

        return super.shouldResetDeltaCalculations(channelConfig);
    }

    @Override
    public ChannelConfig copyConfig(ChannelConfig config) {
        GoogleDisplayNVideo360ChannelConfig channelConfig = this;
        GoogleDisplayNVideo360ChannelConfig newChannelConfig = (GoogleDisplayNVideo360ChannelConfig) config;

        channelConfig.setAudienceId(newChannelConfig.getAudienceId());
        channelConfig.setAudienceName(newChannelConfig.getAudienceName());
        channelConfig.setAccountLimit(newChannelConfig.getAccountLimit());
        channelConfig.setJobFunctions(newChannelConfig.getJobFunctions());
        channelConfig.setJobLevels(newChannelConfig.getJobLevels());
        return this;
    }
}
