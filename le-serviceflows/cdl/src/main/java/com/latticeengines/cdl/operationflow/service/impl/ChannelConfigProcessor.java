package com.latticeengines.cdl.operationflow.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageBody;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;

/**
 * This class process the configuration based on the Channel Type
 * Each method sets the properties needed in the sns configuration
 */
@Component
public class ChannelConfigProcessor {

    public void updateSnsMessageWithChannelConfig(ChannelConfig channelConfig, ExternalIntegrationMessageBody messageBody) {
        if(channelConfig instanceof LinkedInChannelConfig){
            updateSnsMessageWithChannelConfig((LinkedInChannelConfig) channelConfig, messageBody);
            return;
        }

        return;
    }

    private void updateSnsMessageWithChannelConfig(LinkedInChannelConfig channelConfig, ExternalIntegrationMessageBody messageBody) {
        messageBody.setAudienceType(channelConfig.getAudienceType() != null ? channelConfig.getAudienceType().toString() : null);
    }
}
