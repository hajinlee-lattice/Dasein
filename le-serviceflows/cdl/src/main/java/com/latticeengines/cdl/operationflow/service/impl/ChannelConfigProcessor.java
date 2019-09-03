package com.latticeengines.cdl.operationflow.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageBody;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.FacebookChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;

/**
 * This class process the configuration based on the Channel Type
 * Each method sets the properties needed in the sns configuration
 */
@Component
public class ChannelConfigProcessor {

    private static final Logger log = LoggerFactory.getLogger(ChannelConfigProcessor.class);

    public void updateSnsMessageWithChannelConfig(ChannelConfig channelConfig, ExternalIntegrationMessageBody messageBody) {
        if(channelConfig instanceof LinkedInChannelConfig){
            updateSnsMessageWithChannelConfig((LinkedInChannelConfig) channelConfig, messageBody);
            return;
        }
        if (channelConfig instanceof FacebookChannelConfig) {
            updateSnsMessageWithChannelConfig((FacebookChannelConfig) channelConfig, messageBody);
            return;
        }

        return;
    }

    private void updateSnsMessageWithChannelConfig(LinkedInChannelConfig channelConfig, ExternalIntegrationMessageBody messageBody) {
        messageBody.setAudienceType(
                channelConfig.getAudienceType() != null ? channelConfig.getAudienceType().getType() : null);
    }

    private void updateSnsMessageWithChannelConfig(FacebookChannelConfig channelConfig,
            ExternalIntegrationMessageBody messageBody) {
        messageBody.setAudienceType(
                channelConfig.getAudienceType() != null ? channelConfig.getAudienceType().getType() : null);
    }

}
