package com.latticeengines.cdl.operationflow.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageBody;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
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

    public Boolean isContactAudienceType(CDLExternalSystemName destinationSystemName, ChannelConfig channelConfig) {
        switch (destinationSystemName) {
        case Marketo:
        case GoogleAds:
            return true;
        case LinkedIn:
            if (channelConfig == null) {
                log.info("Channel config object is null. Returning false by default");
                return false;
            }
            LinkedInChannelConfig linkedinConfig = (LinkedInChannelConfig) channelConfig;
            return linkedinConfig.getAudienceType() == AudienceType.CONTACTS;
        case Facebook:
            if (channelConfig == null) {
                log.info("Channel config object is null. Returning false by default");
                return false;
            }
            FacebookChannelConfig facebookConfig = (FacebookChannelConfig) channelConfig;
            return facebookConfig.getAudienceType() == AudienceType.CONTACTS;
        default:
            return false;
        }
    }

    public Boolean shouldApplyEmailFilter(CDLExternalSystemName destinationSystemName, ChannelConfig channelConfig) {
        return isContactAudienceType(destinationSystemName, channelConfig);
    }

    public Boolean shouldApplyAccountNameOrWebsiteFilter(CDLExternalSystemName destinationSystemName,
            ChannelConfig channelConfig) {
        switch (destinationSystemName) {
        case LinkedIn:
            LinkedInChannelConfig linkedinConfig = (LinkedInChannelConfig) channelConfig;
            return linkedinConfig.getAudienceType() == AudienceType.ACCOUNTS;
        case Facebook:
            FacebookChannelConfig fbConfig = (FacebookChannelConfig) channelConfig;
            return fbConfig.getAudienceType() == AudienceType.ACCOUNTS;
        default:
            return false;
        }
    }
}
