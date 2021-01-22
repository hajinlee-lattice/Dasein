package com.latticeengines.domain.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.EloquaChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.FacebookChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.GoogleChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;

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

        if (channelConfig == null) {
            log.info("Channel config object is null. Returning false by default");
            return false;
        }

        switch (destinationSystemName) {
        case Marketo:
            MarketoChannelConfig marketoConfig = (MarketoChannelConfig) channelConfig;
            return marketoConfig.getAudienceType() == AudienceType.CONTACTS;
        case Eloqua:
            EloquaChannelConfig eloquaConfig = (EloquaChannelConfig) channelConfig;
            return eloquaConfig.getAudienceType() == AudienceType.CONTACTS;
        case Salesforce:
            SalesforceChannelConfig sfConfig = (SalesforceChannelConfig) channelConfig;
            return sfConfig.getAudienceType() == AudienceType.CONTACTS;
        case Outreach:
            OutreachChannelConfig outreachConfig = (OutreachChannelConfig) channelConfig;
            return outreachConfig.getAudienceType() == AudienceType.CONTACTS;
        case LinkedIn:
            LinkedInChannelConfig linkedinConfig = (LinkedInChannelConfig) channelConfig;
            return linkedinConfig.getAudienceType() == AudienceType.CONTACTS;
        case Facebook:
            FacebookChannelConfig facebookConfig = (FacebookChannelConfig) channelConfig;
            return facebookConfig.getAudienceType() == AudienceType.CONTACTS;
        case GoogleAds:
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

    public static String getTaskDescription(CDLExternalSystemName destinationSystemName,
            ChannelConfig channelConfig) {
        if (destinationSystemName != CDLExternalSystemName.Outreach) {
            return "";
        }

        OutreachChannelConfig config = (OutreachChannelConfig) channelConfig;
        return config.getTaskDescription();
    }
}
