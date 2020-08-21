package com.latticeengines.apps.cdl.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.cdl.channel.AdobeAudienceManagerChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.AppNexusChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.EloquaChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.FacebookChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.GoogleChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.GoogleDisplayNVideo360ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LiveRampChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MediaMathChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.TradeDeskChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.VerizonMediaChannelConfig;

public class ChannelConfigUnitTestNG {
    private static final String testAudienceId1 = "AudienceId1";
    private static final String testAudienceId2 = "AudienceId2";
    private static final String testAudienceName1 = "Audience1";
    private static final String testAudienceName2 = "Audience2";
    private static final String testFolderName1 = "Folder1";
    private static final String testFolderName2 = "Folder2";
    private static final String testFolderId1 = "FolderId1";
    private static final String[] testJobLevels = { "Software Engineer", "CEO", "President" };
    private static final String[] testJobLevelsSame = { "CEO", "President", "Software Engineer" };
    private static final String[] testJobLevelsDifferent = { "Vice President", "CEO", "President" };
    private static final String[] testJobFunctions = { "Marketing", "Research", "HR" };
    private static final String[] testJobFunctionsSame = { "HR", "Research", "Marketing" };
    private static final String[] testJobFunctionsDifferent = { "HR", "Marketing" };

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
        ((MarketoChannelConfig) config).setFolderId(testFolderId1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);
        Assert.assertEquals(launch.getFolderName(), testFolderName1);
        Assert.assertEquals(launch.getFolderId(), testFolderId1);

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

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);

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

    @Test(groups = "functional")
    public void testAdobeAudienceManagerChannelConfig() {
        ChannelConfig config = new AdobeAudienceManagerChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);

        AdobeAudienceManagerChannelConfig copy = new AdobeAudienceManagerChannelConfig();
        copy = (AdobeAudienceManagerChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        testLiveRampChannelConfig((LiveRampChannelConfig) config, copy);
    }

    @Test(groups = "functional")
    public void testAppNexusChannelConfig() {
        ChannelConfig config = new AppNexusChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);

        AppNexusChannelConfig copy = new AppNexusChannelConfig();
        copy = (AppNexusChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        testLiveRampChannelConfig((LiveRampChannelConfig) config, copy);
    }

    @Test(groups = "functional")
    public void testGoogleDisplayNVideo360ChannelConfig() {
        ChannelConfig config = new GoogleDisplayNVideo360ChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);

        GoogleDisplayNVideo360ChannelConfig copy = new GoogleDisplayNVideo360ChannelConfig();
        copy = (GoogleDisplayNVideo360ChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        testLiveRampChannelConfig((LiveRampChannelConfig) config, copy);
    }

    @Test(groups = "functional")
    public void testMediaMathChannelConfig() {
        ChannelConfig config = new MediaMathChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);

        MediaMathChannelConfig copy = new MediaMathChannelConfig();
        copy = (MediaMathChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        testLiveRampChannelConfig((LiveRampChannelConfig) config, copy);
    }

    @Test(groups = "functional")
    public void testTradeDeskChannelConfig() {
        ChannelConfig config = new TradeDeskChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);

        TradeDeskChannelConfig copy = new TradeDeskChannelConfig();
        copy = (TradeDeskChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        testLiveRampChannelConfig((LiveRampChannelConfig) config, copy);
    }

    @Test(groups = "functional")
    public void testVerizonMediaChannelConfig() {
        ChannelConfig config = new VerizonMediaChannelConfig();
        config.setAudienceName(testAudienceName1);
        config.setAudienceId(testAudienceId1);

        PlayLaunch launch = new PlayLaunch();
        config.populateLaunchFromChannelConfig(launch);
        Assert.assertEquals(launch.getAudienceId(), testAudienceId1);
        Assert.assertEquals(launch.getAudienceName(), testAudienceName1);

        VerizonMediaChannelConfig copy = new VerizonMediaChannelConfig();
        copy = (VerizonMediaChannelConfig) copy.copyConfig(config);

        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));
        copy.setAudienceId(testAudienceId2);
        Assert.assertFalse(config.shouldResetDeltaCalculations(copy));

        copy.setAudienceName(testAudienceName2);
        Assert.assertTrue(config.shouldResetDeltaCalculations(copy));

        testLiveRampChannelConfig((LiveRampChannelConfig) config, copy);
    }

    private void testLiveRampChannelConfig(LiveRampChannelConfig originalChannelConfig,
            LiveRampChannelConfig copyChannelConfig) {
        copyChannelConfig = (LiveRampChannelConfig) copyChannelConfig.copyConfig(originalChannelConfig);

        originalChannelConfig.setJobLevels(testJobLevels);
        copyChannelConfig.setJobLevels(testJobLevelsSame);
        Assert.assertFalse(originalChannelConfig.shouldResetDeltaCalculations(copyChannelConfig));

        originalChannelConfig.setJobFunctions(testJobFunctions);
        copyChannelConfig.setJobFunctions(testJobFunctionsSame);
        Assert.assertFalse(originalChannelConfig.shouldResetDeltaCalculations(copyChannelConfig));

        copyChannelConfig.setJobLevels(testJobLevelsDifferent);
        Assert.assertTrue(originalChannelConfig.shouldResetDeltaCalculations(copyChannelConfig));

        originalChannelConfig.setJobLevels(testJobLevelsSame);
        copyChannelConfig.setJobFunctions(testJobFunctionsDifferent);
        Assert.assertTrue(originalChannelConfig.shouldResetDeltaCalculations(copyChannelConfig));
    }
}
