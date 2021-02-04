package com.latticeengines.domain.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.LaunchBaseType;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.EloquaChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.GoogleChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MediaMathChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;

public class ChannelConfigUtilUnitTestNG {

    @Test(groups = "unit")
    public void testOutreachAudienceType() throws Exception {
        OutreachChannelConfig outreachConfig = new OutreachChannelConfig();
        outreachConfig.setLaunchBaseType(LaunchBaseType.AUDIENCE);
        outreachConfig.setAudienceType(AudienceType.CONTACTS);

        boolean result = ChannelConfigUtil.isContactAudienceType(CDLExternalSystemName.Outreach, outreachConfig);
        Assert.assertTrue(result);
    }

    @Test(groups = "unit")
    public void testOutreachAccountAudienceType() throws Exception {
        OutreachChannelConfig outreachConfig = new OutreachChannelConfig();
        outreachConfig.setLaunchBaseType(LaunchBaseType.TASK);
        outreachConfig.setAudienceType(AudienceType.ACCOUNTS);

        boolean result = ChannelConfigUtil.isContactAudienceType(CDLExternalSystemName.Outreach, outreachConfig);
        Assert.assertFalse(result);
    }

    @Test(groups = "unit")
    public void testLiverampConnectorAudienceType() throws Exception {
        MediaMathChannelConfig mmConfig = new MediaMathChannelConfig();
        boolean result = ChannelConfigUtil.isContactAudienceType(CDLExternalSystemName.MediaMath, mmConfig);
        Assert.assertFalse(result);
    }

    @Test(groups = "unit")
    public void testMarketoConnectorAudienceType() throws Exception {
        MarketoChannelConfig marketoConfig = new MarketoChannelConfig();
        boolean result = ChannelConfigUtil.isContactAudienceType(CDLExternalSystemName.Marketo, marketoConfig);
        Assert.assertTrue(result);
    }

    @Test(groups = "unit")
    public void testSalesforceConnectorAudienceType() throws Exception {
        SalesforceChannelConfig salesforceConfig = new SalesforceChannelConfig();
        boolean result = ChannelConfigUtil.isContactAudienceType(CDLExternalSystemName.Salesforce, salesforceConfig);
        Assert.assertFalse(result);
    }

    @Test(groups = "unit")
    public void testEloquaConnectorAudienceType() throws Exception {
        EloquaChannelConfig eloquaConfig = new EloquaChannelConfig();
        boolean result = ChannelConfigUtil.isContactAudienceType(CDLExternalSystemName.Eloqua, eloquaConfig);
        Assert.assertTrue(result);
    }

    @Test(groups = "unit")
    public void testGoogleConnectorAudienceType() throws Exception {
        GoogleChannelConfig googleConfig = new GoogleChannelConfig();
        boolean result = ChannelConfigUtil.isContactAudienceType(CDLExternalSystemName.GoogleAds, googleConfig);
        Assert.assertTrue(result);
    }

    @Test(groups = "unit")
    public void testLinkedInAccountAudienceType() throws Exception {
        LinkedInChannelConfig linkedInConfig = new LinkedInChannelConfig();
        linkedInConfig.setAudienceType(AudienceType.ACCOUNTS);

        boolean result = ChannelConfigUtil.isContactAudienceType(CDLExternalSystemName.LinkedIn, linkedInConfig);
        Assert.assertFalse(result);
    }

    @Test(groups = "unit")
    public void testLinkedInContactAudienceType() throws Exception {
        LinkedInChannelConfig linkedInConfig = new LinkedInChannelConfig();
        linkedInConfig.setAudienceType(AudienceType.CONTACTS);

        boolean result = ChannelConfigUtil.isContactAudienceType(CDLExternalSystemName.LinkedIn, linkedInConfig);
        Assert.assertTrue(result);
    }
}
