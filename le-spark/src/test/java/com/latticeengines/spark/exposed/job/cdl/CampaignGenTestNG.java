package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.playmakercore.NonStandardRecColumnName;
import com.latticeengines.domain.exposed.playmakercore.RecommendationColumnName;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext;
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext.PlayLaunchSparkContextBuilder;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig;
import com.latticeengines.spark.testframework.TestJoinTestNGBase;

public class CampaignGenTestNG extends TestJoinTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CampaignGenTestNG.class);

    private static final String destinationAccountId = "D41000001Q3z4EAC";
    private static final int accountLimit = 25;

    @Override
    protected String getJobName() {
        return "campaign";
    }

    @Override
    protected String getScenarioName() {
        return "cdl";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("account", "contact");
    }

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional", dataProvider = "destinationProvider")
    public void runTest(final CDLExternalSystemName destination, boolean accountDataOnly) {
        CreateRecommendationConfig createRecConfig = new CreateRecommendationConfig();
        PlayLaunchSparkContext playLaunchContext = generatePlayContext(destination);
        createRecConfig.setPlayLaunchSparkContext(playLaunchContext);
        SparkJobResult result = runSparkJob(CreateRecommendationsJob.class, createRecConfig);
        verifyResult(result);
    }

    @Override
    protected void verifyResult(SparkJobResult result) {
        verifyOutput(result.getOutput());
        List<HdfsDataUnit> targets = result.getTargets();
        verifyExternalId(targets);
    }

    private void verifyExternalId(List<HdfsDataUnit> targets) {
        Assert.assertEquals(targets.size(), CreateRecommendationConfig.NUM_TARGETS);
        HdfsDataUnit recommendationDataUnit = targets.get(0);
        Assert.assertEquals(recommendationDataUnit.getCount().intValue(), accountLimit);
        HdfsDataUnit csvDataUnit = targets.get(1);
        Assert.assertEquals(recommendationDataUnit.getCount(), csvDataUnit.getCount());
        Map<String, String> accountIdToExternalIdMap = new HashMap<>();
        verifyAndReadTarget(recommendationDataUnit).forEachRemaining(record -> {
            Object externalIdObject = record.get(RecommendationColumnName.EXTERNAL_ID.name());
            Object accountIdObject = record.get(RecommendationColumnName.EXTERNAL_ID.name());
            accountIdToExternalIdMap.put(accountIdObject.toString(), externalIdObject.toString());
        });
        verifyAndReadTarget(csvDataUnit).forEachRemaining(record -> {
            Object externalIdObject = record.get(RecommendationColumnName.EXTERNAL_ID.name());
            Object accountIdObject = record.get(RecommendationColumnName.EXTERNAL_ID.name());
            Assert.assertTrue(accountIdToExternalIdMap.containsKey(accountIdObject.toString()));
            Assert.assertEquals(accountIdToExternalIdMap.get(accountIdObject.toString()), externalIdObject.toString());
        });
    }

    @Override
    protected void verifyOutput(String output) {
        Assert.assertEquals(Integer.parseInt(output), 86);
    }

    private PlayLaunchSparkContext generatePlayContext(CDLExternalSystemName destination) {
        return generateS3PlayLaunchSparkContext();
    }

    private PlayLaunchSparkContext generateS3PlayLaunchSparkContext() {
        PlayLaunchSparkContext playLaunchSparkContext = generateBasicPlayLaunchSparkContext();
        playLaunchSparkContext.setAccountColsRecIncluded(generateAccountColsRecIncludedForS3());
        playLaunchSparkContext.setAccountColsRecNotIncludedStd(generateAccountColsRecNotIncludedStdForS3());
        playLaunchSparkContext.setAccountColsRecNotIncludedNonStd(generateAccountColsRecNotIncludedNonStdForS3());
        playLaunchSparkContext.setContactCols(generateContactColsForS3());
        return playLaunchSparkContext;
    }

    private List<String> generateAccountColsRecIncludedForS3() {
        return Arrays.asList(RecommendationColumnName.COMPANY_NAME.name(), RecommendationColumnName.ACCOUNT_ID.name(),
                RecommendationColumnName.DESCRIPTION.name(), RecommendationColumnName.PLAY_ID.name(),
                RecommendationColumnName.LAUNCH_DATE.name(), RecommendationColumnName.LAUNCH_ID.name(),
                RecommendationColumnName.DESTINATION_ORG_ID.name(),
                RecommendationColumnName.DESTINATION_SYS_TYPE.name(),
                RecommendationColumnName.LAST_UPDATED_TIMESTAMP.name(), RecommendationColumnName.MONETARY_VALUE.name(),
                RecommendationColumnName.PRIORITY_ID.name(), RecommendationColumnName.PRIORITY_DISPLAY_NAME.name(),
                RecommendationColumnName.LIKELIHOOD.name(), RecommendationColumnName.LIFT.name(),
                RecommendationColumnName.RATING_MODEL_ID.name(), RecommendationColumnName.EXTERNAL_ID.name()).stream()
                .map(col -> RecommendationColumnName.RECOMMENDATION_COLUMN_TO_INTERNAL_NAME_MAP.getOrDefault(col, col))
                .collect(Collectors.toList());
    }

    private List<String> generateAccountColsRecNotIncludedStdForS3() {
        return Arrays.asList(InterfaceName.Website.name(), "PostalCode", "City", "State", "PhoneNumber", "Country");
    }

    private List<String> generateAccountColsRecNotIncludedNonStdForS3() {
        return Arrays.asList(NonStandardRecColumnName.DESTINATION_SYS_NAME.name(),
                NonStandardRecColumnName.PLAY_NAME.name(), NonStandardRecColumnName.RATING_MODEL_NAME.name(),
                NonStandardRecColumnName.SEGMENT_NAME.name(), RecommendationColumnName.SFDC_ACCOUNT_ID.name());
    }

    private List<String> generateContactColsForS3() {
        return ImmutableList.<String> builder().add(InterfaceName.Email.name())
                .add(InterfaceName.Address_Street_1.name()).add(InterfaceName.PhoneNumber.name())
                .add(InterfaceName.State.name()).add(InterfaceName.PostalCode.name()).add(InterfaceName.Country.name())
                .add(InterfaceName.SalesforceContactID.name()).add(InterfaceName.City.name())
                .add(InterfaceName.ContactId.name()).add(InterfaceName.Name.name()).add(InterfaceName.FirstName.name())
                .add(InterfaceName.LastName.name()).build();
    }

    // ---- end of S3 Account and Contact columns---

    private PlayLaunchSparkContext generateBasicPlayLaunchSparkContext() {
        Tenant tenant = new Tenant("TestRecommendationGenTenant");
        tenant.setPid(1L);
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setCreated(new Date());
        playLaunch.setId(PlayLaunch.generateLaunchId());
        playLaunch.setDestinationAccountId(destinationAccountId);
        playLaunch.setDestinationSysType(CDLExternalSystemType.CRM);
        playLaunch.setTopNCount(Integer.toUnsignedLong(accountLimit));
        MetadataSegment segment = new MetadataSegment();
        Play play = new Play();
        play.setTargetSegment(segment);
        play.setDescription("play description");
        play.setName(UUID.randomUUID().toString());
        playLaunch.setPlay(play);
        long launchTime = new Date().getTime();
        String saltHint = CipherUtils.generateKey();
        String key = CipherUtils.generateKey();
        String pw = CipherUtils.encrypt(dataDbPassword, key, saltHint);

        PlayLaunchSparkContext sparkContext = new PlayLaunchSparkContextBuilder()//
                .tenant(tenant) //
                .playName(play.getName()) //
                .playLaunchId(playLaunch.getId()) //
                .playLaunch(playLaunch) //
                .play(play) //
                .segment(segment) //
                .launchTimestampMillis(launchTime) //
                .dataDbDriver(dataDbDriver) //
                .dataDbUrl(dataDbUrl) //
                .dataDbUser(dataDbUser) //
                .saltHint(saltHint) //
                .encryptionKey(key) //
                .dataDbPassword(pw) //
                .build();
        return sparkContext;
    }

    @DataProvider
    public Object[][] destinationProvider() {
        return new Object[][] { { CDLExternalSystemName.AWS_S3, false } };
    }
}
