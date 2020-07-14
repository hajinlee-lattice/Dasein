package com.latticeengines.apps.cdl.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.EloquaChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.FacebookChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.GoogleChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;

public class ChannelConfigUnitTestNG {

    private static final String testAudienceId1 = "AudienceId1";
    private static final String testAudienceId2 = "AudienceId2";
    private static final String testAudienceName1 = "Audience1";
    private static final String testAudienceName2 = "Audience2";
    private static final String testFolderName1 = "Folder1";
    private static final String testFolderName2 = "Folder2";

    @Test(groups = "functional")
    public void testSalesforceChannelConfig() {
        ChannelConfig config = new SalesforceChannelConfig();
        ((SalesforceChannelConfig) config).setAccountLimit(100l);
        ((SalesforceChannelConfig) config).setSuppressAccountsWithoutLookupId(true);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertTrue(launch.getExcludeItemsWithoutSalesforceId());
        Assert.assertFalse(config.shouldResetDeltaCalculations(null));
    }

    @Test(groups = "functional")
    public void testEloquaChannelConfig() {
        ChannelConfig config = new EloquaChannelConfig();
        Assert.assertTrue(config.isSuppressAccountsWithoutContacts());
        Assert.assertTrue(config.isSuppressContactsWithoutEmails());
        Assert.assertFalse(config.shouldResetDeltaCalculations(null));
    }

    @Test(groups = "functional")
    public void testMarketoChannelConfig() {
        ChannelConfig config = new MarketoChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);
        ((MarketoChannelConfig) config).setFolderName(testFolderName1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);
        Assert.assertEquals(launch.getFolderName(), testFolderName1);

        MarketoChannelConfig copy = new MarketoChannelConfig();
        copy = (MarketoChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceId(testAudienceId1);
        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceName(testAudienceName1);
        copy.setFolderName(testFolderName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        Assert.assertTrue(config.isSuppressAccountsWithoutContacts());
        Assert.assertTrue(config.isSuppressContactsWithoutEmails());
        Assert.assertFalse(config.shouldResetDeltaCalculations(null));
    }

    @Test(groups = "functional")
    public void testLinkedInChannelConfig() {
        ChannelConfig config = new LinkedInChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);
        ((LinkedInChannelConfig) config).setAudienceType(AudienceType.CONTACTS);
        ((LinkedInChannelConfig) config).setFolderName(testFolderName1);
        Assert.assertTrue(config.isSuppressAccountsWithoutContacts());
        Assert.assertTrue(config.isSuppressContactsWithoutEmails());

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);

        LinkedInChannelConfig copy = new LinkedInChannelConfig();
        copy = (LinkedInChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceId(testAudienceId1);
        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceName(testAudienceName1);
        copy.setAudienceType(AudienceType.ACCOUNTS);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        Assert.assertFalse(copy.isSuppressAccountsWithoutContacts());
        Assert.assertFalse(copy.isSuppressContactsWithoutEmails());
        Assert.assertFalse(config.shouldResetDeltaCalculations(null));
    }

    @Test(groups = "functional")
    public void testS3CannelConfig() {
        ChannelConfig config = new S3ChannelConfig();
        ((S3ChannelConfig) config).setAudienceType(AudienceType.CONTACTS);
        Assert.assertFalse(config.isSuppressAccountsWithoutContacts());
        Assert.assertFalse(config.isSuppressContactsWithoutEmails());
        Assert.assertFalse(config.isSuppressAccountsWithoutLookupId());

        S3ChannelConfig copy = new S3ChannelConfig();
        copy = (S3ChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceType(AudienceType.ACCOUNTS);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));
    }

    @Test(groups = "functional")
    public void testOutreachChannelConfig() {
        ChannelConfig config = new OutreachChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);
        ((OutreachChannelConfig) config).setFolderName(testFolderName1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);
        Assert.assertEquals(launch.getFolderName(), testFolderName1);

        OutreachChannelConfig copy = new OutreachChannelConfig();
        copy = (OutreachChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceId(testAudienceId1);
        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));
    }

    @Test(groups = "functional")
    public void testGoogleChannelConfig() {
        ChannelConfig config = new GoogleChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);
        ((GoogleChannelConfig) config).setFolderName(testFolderName1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);

        GoogleChannelConfig copy = new GoogleChannelConfig();
        copy = (GoogleChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceId(testAudienceId1);
        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));
    }

    @Test(groups = "functional")
    public void testFacebookChannelConfig() {
        ChannelConfig config = new FacebookChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);
        ((FacebookChannelConfig) config).setFolderName(testFolderName1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);

        FacebookChannelConfig copy = new FacebookChannelConfig();
        copy = (FacebookChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceId(testAudienceId1);
        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

    }

}
