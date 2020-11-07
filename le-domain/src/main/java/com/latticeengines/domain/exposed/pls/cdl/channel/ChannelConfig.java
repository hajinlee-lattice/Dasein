package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT)
@JsonSubTypes({ //
        @Type(value = SalesforceChannelConfig.class, name = "salesforce"), //
        @Type(value = MarketoChannelConfig.class, name = "marketo"), //
        @Type(value = EloquaChannelConfig.class, name = "eloqua"), //
        @Type(value = S3ChannelConfig.class, name = "aws_s3"), //
        @Type(value = LinkedInChannelConfig.class, name = "linkedin"), //
        @Type(value = FacebookChannelConfig.class, name = "facebook"), //
        @Type(value = OutreachChannelConfig.class, name = "outreach"), //
        @Type(value = GoogleChannelConfig.class, name = "googleads"), //
        @Type(value = AdobeAudienceManagerChannelConfig.class, name = "adobe_audience_mgr"), //
        @Type(value = AppNexusChannelConfig.class, name = "appnexus"), //
        @Type(value = GoogleDisplayNVideo360ChannelConfig.class, name = "google_display_n_video_360"), //
        @Type(value = MediaMathChannelConfig.class, name = "mediamath"), //
        @Type(value = TradeDeskChannelConfig.class, name = "tradedesk"), //
        @Type(value = VerizonMediaChannelConfig.class, name = "verizon_media") //
})
public interface ChannelConfig {

    ChannelConfig copyConfig(ChannelConfig config);

    CDLExternalSystemName getSystemName();

    AudienceType getAudienceType();

    String getAudienceId();

    void setAudienceId(String audienceId);

    String getAudienceName();

    void setAudienceName(String audienceName);

    boolean shouldResetDeltaCalculations(ChannelConfig channelConfig);

    boolean isSuppressAccountsWithoutContacts();

    boolean isSuppressContactsWithoutEmails();

    boolean isSuppressAccountsWithoutLookupId();

    void populateLaunchFromChannelConfig(PlayLaunch playLaunch);
}
