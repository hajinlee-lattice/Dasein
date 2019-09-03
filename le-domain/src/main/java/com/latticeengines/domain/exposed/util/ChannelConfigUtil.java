package com.latticeengines.domain.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.FacebookChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;

public class ChannelConfigUtil {

    private static final Logger log = LoggerFactory.getLogger(ChannelConfigUtil.class);

    public static Boolean isContactAudienceType(CDLExternalSystemName destinationSystemName,
            ChannelConfig channelConfig) {
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

    public static Boolean shouldApplyEmailFilter(CDLExternalSystemName destinationSystemName,
            ChannelConfig channelConfig) {
        return isContactAudienceType(destinationSystemName, channelConfig);
    }

    public static Boolean shouldApplyAccountNameOrWebsiteFilter(CDLExternalSystemName destinationSystemName,
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
