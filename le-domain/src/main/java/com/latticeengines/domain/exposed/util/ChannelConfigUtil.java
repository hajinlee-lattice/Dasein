package com.latticeengines.domain.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.FacebookChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.GoogleChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;

public final class ChannelConfigUtil {

    protected ChannelConfigUtil() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(ChannelConfigUtil.class);

    public static Boolean isContactAudienceType(CDLExternalSystemName destinationSystemName,
            ChannelConfig channelConfig) {
        if (CDLExternalSystemName.LIVERAMP.contains(destinationSystemName)) {
            return false;
        }
        switch (destinationSystemName) {
        case Marketo:
        case Outreach:
            return true;
        case Eloqua:
            return true;
        case Salesforce:
            return false;
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
        case GoogleAds:
            if (channelConfig == null) {
                log.info("Channel config object is null. Returning false by default");
                return false;
            }
            GoogleChannelConfig googleConfig = (GoogleChannelConfig) channelConfig;
            return googleConfig.getAudienceType() == AudienceType.CONTACTS;
        default:
            return false;
        }
    }

    public static Boolean shouldApplyAccountNameOrWebsiteFilter(CDLExternalSystemName destinationSystemName,
            ChannelConfig channelConfig) {
        switch (destinationSystemName) {
        case LinkedIn:
            LinkedInChannelConfig linkedInConfig = (LinkedInChannelConfig) channelConfig;
            return linkedInConfig.getAudienceType() == AudienceType.ACCOUNTS;
        case Facebook:
            FacebookChannelConfig fbConfig = (FacebookChannelConfig) channelConfig;
            return fbConfig.getAudienceType() == AudienceType.ACCOUNTS;
        case GoogleAds:
            GoogleChannelConfig googleConfig = (GoogleChannelConfig) channelConfig;
            return googleConfig.getAudienceType() == AudienceType.ACCOUNTS;
        default:
            return false;
        }
    }
}
