package com.latticeengines.spark.exposed.job.cdl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmakercore.RecommendationColumnName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext;
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext.PlayLaunchSparkContextBuilder;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig;
import com.latticeengines.spark.testframework.TestJoinTestNGBase;

public class TestRecommendationGenTestNG extends TestJoinTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TestRecommendationGenTestNG.class);

    private static final String ratingId = RatingEngine.generateIdStr();
    private static final String destinationAccountId = "D41000001Q3z4EAC";
    private static final int contactPerAccount = 10;
    private String accountData;
    private String contactData;
    private Object[][] accounts;
    private Object[][] contacts;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        uploadInputAvro();
    }

    @Test(groups = "functional", dataProvider = "destinationProvider")
    public void runTest(final CDLExternalSystemName destination, boolean accountDataOnly) {
        overwriteInputs(accountDataOnly);
        CreateRecommendationConfig createRecConfig = new CreateRecommendationConfig();
        PlayLaunchSparkContext playLaunchContext = generatePlayContext(destination);
        createRecConfig.setPlayLaunchSparkContext(playLaunchContext);
        SparkJobResult result = runSparkJob(CreateRecommendationsJob.class, createRecConfig);
        List<List<Pair<List<String>, Boolean>>> expectedColumns = generateExpectedColumns(destination, accountDataOnly);
        verifyResult(result, expectedColumns);
    }

    @Override
    protected void verifyOutput(String output) {
        log.info("Contact count is " + output);
        if (inputs.size() == 2) {
            // there is one account that does not have any mapped contact
            Assert.assertEquals(output, String.valueOf(contactPerAccount * (accounts.length - 1)));
        } else {
            Assert.assertEquals(output, "0");
        }
    }

    @Override
    protected List<BiFunction<HdfsDataUnit, List<Pair<List<String>, Boolean>>, Boolean>> getTargetVerifiers(
            List<List<Pair<List<String>, Boolean>>> expectedColumns) {
        List<BiFunction<HdfsDataUnit, List<Pair<List<String>, Boolean>>, Boolean>> list = new ArrayList<>();
        for (int i = 0; i < expectedColumns.size(); i++) {
            list.add(this::verifyOutput);
        }
        return list;
    }

    private Boolean verifyOutput(HdfsDataUnit target, List<Pair<List<String>, Boolean>> accountAndContextExpectedCols) {
        Pair<List<String>, Boolean> accountExpectedCols = accountAndContextExpectedCols.get(0);
        Pair<List<String>, Boolean> contactExpectedCols = accountAndContextExpectedCols.get(1);
        AtomicInteger count = new AtomicInteger();

        verifyAndReadTarget(target).forEachRemaining(record -> {
            count.incrementAndGet();
            if (count.get() == 1) {
                log.info(record.toString());
            }
            Object contactObject = record.get(RecommendationColumnName.CONTACTS.name());
            if (contactObject != null) {
                String contacts = contactObject.toString();
                if (count.get() == 1) {
                    List<String> accountCols = record.getSchema().getFields().stream().map(field -> field.name())
                            .collect(Collectors.toList());
                    verifyCols(accountCols, accountExpectedCols.getLeft(), accountExpectedCols.getRight());
                    ObjectMapper jsonParser = new ObjectMapper();
                    try {
                        if (StringUtils.isNotBlank(contacts)) {
                            JsonNode jsonObject = jsonParser.readTree(contacts);
                            Assert.assertTrue(jsonObject.isArray());
                            List<String> contactCols = new ArrayList<>();
                            if (jsonObject.size() > 0) {
                                jsonObject.get(0).fieldNames().forEachRemaining(col -> contactCols.add(col));
                                verifyCols(contactCols, contactExpectedCols.getLeft(), contactExpectedCols.getRight());
                            }
                        }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        });
        Assert.assertEquals(count.get(), accounts.length);
        return true;
    }

    private void verifyCols(List<String> cols, List<String> expectedCols, boolean checkOrder) {
        log.info("actual Cols:" + Arrays.toString(cols.toArray()));
        log.info("expected Cols:" + Arrays.toString(expectedCols.toArray()));
        if (cols.size() != expectedCols.size()) {
            Assert.fail(cols.size() + " size does not match with " + expectedCols.size());
        }
        if (checkOrder) {
            IntStream.range(0, cols.size()).forEach(i -> {
                Assert.assertTrue(cols.get(i).equals(expectedCols.get(i)),
                        "At " + i + ": " + cols.get(i) + "not equal to " + expectedCols.get(i));
            });
        } else {
            Set<String> colsSet = new HashSet<>(cols);
            IntStream.range(0, cols.size()).forEach(i -> {
                Assert.assertTrue(colsSet.contains(expectedCols.get(i)), expectedCols.get(i) + "not included.");
            });
        }
    }

    @Override
    protected void uploadInputAvro() {
        List<Pair<String, Class<?>>> accountFields = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.CustomerAccountId.name(), String.class), //
                Pair.of(destinationAccountId, String.class), //
                Pair.of(InterfaceName.CompanyName.name(), String.class), //
                Pair.of(InterfaceName.LDC_Name.name(), String.class), //
                Pair.of(ratingId + PlaymakerConstants.RatingScoreColumnSuffix, Integer.class), //
                Pair.of(ratingId, String.class), //
                Pair.of(ratingId + PlaymakerConstants.RatingEVColumnSuffix, String.class), //
                Pair.of(InterfaceName.Website.name(), String.class), //
                Pair.of(InterfaceName.CreatedDate.name(), String.class) //
        );
        accounts = new Object[][] { //
                { "0L", "0000", "destinationAccountId", "Lattice", "Lattice Engines", 98, "A", "1000",
                        "www.lattice-engines.com", "01/01/2019" }, //
                { "1L", "0001", "destinationAccountId", "DnB", "DnB", 97, "B", "2000", "www.dnb.com", "01/01/2019" }, //
                { "2L", "0002", "destinationAccountId", "Google", "Google", 98, "C", "3000", "www.google.com",
                        "01/01/2019" }, //
                { "3L", "0003", "destinationAccountId", "Facebook", "FB", 93, "E", "1000000", "www.facebook.com",
                        "01/01/2019" }, //
                { "4L", "0004", "destinationAccountId", "Apple", "Apple", null, null, null, "www.apple.com",
                        "01/01/2019" }, //
                { "5L", "0005", "destinationAccountId", "SalesForce", "SalesForce", null, "A", null,
                        "www.salesforce.com", "01/01/2019" }, //
                { "6L", "0006", "destinationAccountId", "Adobe", "Adobe", 98, null, "1000", "www.adobe.com",
                        "01/01/2019" }, //
                { "7L", "0007", "destinationAccountId", "Eloqua", "Eloqua", 40, "F", "100", "www.eloqua.com",
                        "01/01/2019" }, //
                { "8L", "0008", "destinationAccountId", "Dell", "Dell", 8, "F", "10", "www.dell.com", "01/01/2019" }, //
                { "9L", "0009", "destinationAccountId", "HP", "HP", 38, "E", "500", "www.hp.com", "01/01/2019" }, //
                // the following account has no matched contacts
                { "100L", "0100", "destinationAccountId", "Fake Co", "Fake Co", 3, "F", "5", "", "" } //
        };
        accountData = uploadHdfsDataUnit(accounts, accountFields);

        // the contact schema does not have the Address_Street_1.name for
        // testing the the case where contact schema is not complete
        List<Pair<String, Class<?>>> contactfields = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.ContactId.name(), String.class), //
                Pair.of(InterfaceName.CustomerContactId.name(), String.class), //
                Pair.of(InterfaceName.CompanyName.name(), String.class), //
                Pair.of(InterfaceName.Email.name(), String.class), //
                Pair.of(InterfaceName.ContactName.name(), String.class), //
                Pair.of(InterfaceName.City.name(), String.class), //
                Pair.of(InterfaceName.State.name(), String.class), //
                Pair.of(InterfaceName.Country.name(), String.class), //
                Pair.of(InterfaceName.PostalCode.name(), String.class), //
                Pair.of(InterfaceName.PhoneNumber.name(), String.class), //
                Pair.of(InterfaceName.Title.name(), String.class), //
                Pair.of(InterfaceName.FirstName.name(), String.class), //
                Pair.of(InterfaceName.LastName.name(), String.class), //
                Pair.of(InterfaceName.CreatedDate.name(), String.class) //
        );

        // the last contact does not have corresponding account
        contacts = new Object[accounts.length * contactPerAccount][contactfields.size()];
        for (int i = 0; i < accounts.length; i++) {
            for (int j = 0; j < contactPerAccount; j++) {
                contacts[contactPerAccount * i + j][0] = String.valueOf(i) + "L";
                contacts[contactPerAccount * i + j][1] = String.valueOf(contactPerAccount * i + j);
                contacts[contactPerAccount * i + j][2] = String.valueOf(accounts.length * i + j) + "L";
                contacts[contactPerAccount * i + j][3] = "Kind Inc.";
                contacts[contactPerAccount * i + j][4] = "michael@kind.com";
                contacts[contactPerAccount * i + j][5] = "Michael Jackson";
                contacts[contactPerAccount * i + j][6] = "SMO";
                contacts[contactPerAccount * i + j][7] = "CA";
                contacts[contactPerAccount * i + j][8] = "US";
                contacts[contactPerAccount * i + j][9] = "94404";
                contacts[contactPerAccount * i + j][10] = "650-898-3928";
                contacts[contactPerAccount * i + j][11] = "CEO";
                contacts[contactPerAccount * i + j][12] = "Michael";
                contacts[contactPerAccount * i + j][13] = "Jackson";
                contacts[contactPerAccount * i + j][14] = "08/08/2019";
            }
        }

        contactData = uploadHdfsDataUnit(contacts, contactfields);
    }

    private void overwriteInputs(boolean accountDataOnly) {
        if (!accountDataOnly) {
            inputs = Arrays.asList(accountData, contactData);
        } else {
            inputs = Arrays.asList(accountData);
        }
    }

    private PlayLaunchSparkContext generatePlayContext(CDLExternalSystemName destination) {
        switch (destination) {
        case Salesforce:
            return generateSfdcPlayLaunchSparkContext();
        case Marketo:
            return generateMarketoPlayLaunchSparkContext();
        case AWS_S3:
            return generateS3PlayLaunchSparkContext();
        case LinkedIn:
            return generateLinkedInPlayLaunchSparkContext();
        case GoogleAds:
            return generateGooglePlayLaunchSparkContext();
        case Facebook:
            return generateFacebookPlayLaunchSparkContext();
        default:
            return generateSfdcPlayLaunchSparkContext();
        }
    }

    private List<List<Pair<List<String>, Boolean>>> generateExpectedColumns(CDLExternalSystemName destination,
            boolean accountDataOnly) {
        switch (destination) {
        case Salesforce:
            return generateSfdcExpectedColumns(accountDataOnly);
        case Marketo:
            return generateMarketoExpectedColumns(accountDataOnly);
        case AWS_S3:
            return generateS3ExpectedColumns(accountDataOnly);
        case LinkedIn:
            return generateLinkedInExpectedColumns(accountDataOnly);
        case GoogleAds:
            return generateGoogleExpectedColumns(accountDataOnly);
        case Facebook:
            return generateFacebookExpectedColumns(accountDataOnly);
        default:
            return generateSfdcExpectedColumns(accountDataOnly);
        }
    }

    private List<List<Pair<List<String>, Boolean>>> generateFacebookExpectedColumns(boolean accountDataOnly) {
        List<Pair<List<String>, Boolean>> standardList = Arrays.asList(
                Pair.of(standardRecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(CampaignLaunchUtils.facebookRecommendationContactColumns(), false));
        List<Pair<List<String>, Boolean>> customList = Arrays.asList(
                Pair.of(CampaignLaunchUtils.facebookRecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(CampaignLaunchUtils.facebookRecommendationContactColumns(), false));
        return Arrays.asList(standardList, customList);
    }

    private List<List<Pair<List<String>, Boolean>>> generateGoogleExpectedColumns(boolean accountDataOnly) {
        List<Pair<List<String>, Boolean>> standardList = Arrays.asList(
                Pair.of(standardRecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(CampaignLaunchUtils.googleRecommendationContactColumns(), false));
        List<Pair<List<String>, Boolean>> customList = Arrays.asList(
                Pair.of(CampaignLaunchUtils.googleRecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(CampaignLaunchUtils.googleRecommendationContactColumns(), false));
        return Arrays.asList(standardList, customList);
    }

    private List<List<Pair<List<String>, Boolean>>> generateLinkedInExpectedColumns(boolean accountDataOnly) {
        List<Pair<List<String>, Boolean>> standardList = Arrays.asList(
                Pair.of(standardRecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(CampaignLaunchUtils.linkedInRecommendationContactColumns(), false));
        List<Pair<List<String>, Boolean>> customList = Arrays.asList(
                Pair.of(CampaignLaunchUtils.linkedInRecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(CampaignLaunchUtils.linkedInRecommendationContactColumns(), false));
        return Arrays.asList(standardList, customList);
    }

    private List<List<Pair<List<String>, Boolean>>> generateS3ExpectedColumns(boolean accountDataOnly) {
        List<Pair<List<String>, Boolean>> standardList = Arrays.asList(
                Pair.of(standardRecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(CampaignLaunchUtils.s3RecommendationContactColumns(), false));
        List<Pair<List<String>, Boolean>> customList = Arrays.asList(
                Pair.of(CampaignLaunchUtils.s3RecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(CampaignLaunchUtils.s3RecommendationContactColumns(), false));
        return Arrays.asList(standardList, customList);
    }

    private List<List<Pair<List<String>, Boolean>>> generateMarketoExpectedColumns(boolean accountDataOnly) {
        List<Pair<List<String>, Boolean>> standardList = Arrays.asList(
                Pair.of(standardRecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(CampaignLaunchUtils.marketoRecommendationContactColumns(), false));
        List<Pair<List<String>, Boolean>> customList = Arrays.asList(
                Pair.of(CampaignLaunchUtils.marketoRecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(CampaignLaunchUtils.marketoRecommendationContactColumns(), false));
        return Arrays.asList(standardList, customList);
    }

    private List<List<Pair<List<String>, Boolean>>> generateSfdcExpectedColumns(boolean accountDataOnly) {
        List<Pair<List<String>, Boolean>> list = standardAccountAndContactExpectedColumns(accountDataOnly);
        return Arrays.asList(list, list);
    }

    private List<Pair<List<String>, Boolean>> standardAccountAndContactExpectedColumns(boolean accountDataOnly) {
        return Arrays.asList(Pair.of(standardRecommendationAccountColumns(), false),
                accountDataOnly ? Pair.of(Collections.emptyList(), false)
                        : Pair.of(standardRecommendationContactColumns(), false));
    }

    private List<String> standardRecommendationAccountColumns() {
        return Arrays.asList(RecommendationColumnName.values()).stream().map(col -> col.name())
                .collect(Collectors.toList());
    }

    private List<String> standardRecommendationContactColumns() {
        return Arrays.asList(PlaymakerConstants.Email, PlaymakerConstants.Address, PlaymakerConstants.Phone,
                PlaymakerConstants.State, PlaymakerConstants.ZipCode, PlaymakerConstants.Country,
                PlaymakerConstants.SfdcContactID, PlaymakerConstants.City, PlaymakerConstants.ContactID,
                PlaymakerConstants.Name);
    }

    private PlayLaunchSparkContext generateMarketoPlayLaunchSparkContext() {
        PlayLaunchSparkContext playLaunchSparkContext = generateBasicPlayLaunchSparkContext();
        playLaunchSparkContext
                .setAccountColsRecIncluded(CampaignLaunchUtils.generateAccountColsRecIncludedForMarketo());
        playLaunchSparkContext
                .setAccountColsRecNotIncludedStd(CampaignLaunchUtils.generateAccountColsRecNotIncludedStdForMarketo());
        playLaunchSparkContext.setAccountColsRecNotIncludedNonStd(
                CampaignLaunchUtils.generateAccountColsRecNotIncludedNonStdForMarketo());
        playLaunchSparkContext.setContactCols(CampaignLaunchUtils.generateContactColsForMarketo());
        return playLaunchSparkContext;
    }

    private PlayLaunchSparkContext generateS3PlayLaunchSparkContext() {
        PlayLaunchSparkContext playLaunchSparkContext = generateBasicPlayLaunchSparkContext();
        playLaunchSparkContext.setAccountColsRecIncluded(CampaignLaunchUtils.generateAccountColsRecIncludedForS3());
        playLaunchSparkContext
                .setAccountColsRecNotIncludedStd(CampaignLaunchUtils.generateAccountColsRecNotIncludedStdForS3());
        playLaunchSparkContext
                .setAccountColsRecNotIncludedNonStd(CampaignLaunchUtils.generateAccountColsRecNotIncludedNonStdForS3());
        playLaunchSparkContext.setContactCols(CampaignLaunchUtils.generateContactColsForS3());
        return playLaunchSparkContext;
    }

    private PlayLaunchSparkContext generateLinkedInPlayLaunchSparkContext() {
        PlayLaunchSparkContext playLaunchSparkContext = generateBasicPlayLaunchSparkContext();
        playLaunchSparkContext
                .setAccountColsRecIncluded(CampaignLaunchUtils.generateAccountColsRecIncludedForLinkedIn());
        playLaunchSparkContext
                .setAccountColsRecNotIncludedStd(CampaignLaunchUtils.generateAccountColsRecNotIncludedStdForLinkedIn());
        playLaunchSparkContext.setAccountColsRecNotIncludedNonStd(
                CampaignLaunchUtils.generateAccountColsRecNotIncludedNonStdForLinkedIn());
        playLaunchSparkContext.setContactCols(CampaignLaunchUtils.generateContactColsForLinkedIn());
        return playLaunchSparkContext;
    }

    private PlayLaunchSparkContext generateGooglePlayLaunchSparkContext() {
        PlayLaunchSparkContext playLaunchSparkContext = generateBasicPlayLaunchSparkContext();
        playLaunchSparkContext.setAccountColsRecIncluded(CampaignLaunchUtils.generateAccountColsRecIncludedForGoogle());
        playLaunchSparkContext
                .setAccountColsRecNotIncludedStd(CampaignLaunchUtils.generateAccountColsRecNotIncludedStdForGoogle());
        playLaunchSparkContext.setAccountColsRecNotIncludedNonStd(
                CampaignLaunchUtils.generateAccountColsRecNotIncludedNonStdForGoogle());
        playLaunchSparkContext.setContactCols(CampaignLaunchUtils.generateContactColsForGoogle());
        return playLaunchSparkContext;
    }

    private PlayLaunchSparkContext generateFacebookPlayLaunchSparkContext() {
        PlayLaunchSparkContext playLaunchSparkContext = generateBasicPlayLaunchSparkContext();
        playLaunchSparkContext
                .setAccountColsRecIncluded(CampaignLaunchUtils.generateAccountColsRecIncludedForFacebook());
        playLaunchSparkContext
                .setAccountColsRecNotIncludedStd(CampaignLaunchUtils.generateAccountColsRecNotIncludedStdForFacebook());
        playLaunchSparkContext.setAccountColsRecNotIncludedNonStd(
                CampaignLaunchUtils.generateAccountColsRecNotIncludedNonStdForFacebook());
        playLaunchSparkContext.setContactCols(CampaignLaunchUtils.generateContactColsForFacebook());
        return playLaunchSparkContext;
    }

    private PlayLaunchSparkContext generateSfdcPlayLaunchSparkContext() {
        return generateBasicPlayLaunchSparkContext();
    }

    private PlayLaunchSparkContext generateBasicPlayLaunchSparkContext() {
        Tenant tenant = new Tenant("TestRecommendationGenTenant");
        tenant.setPid(1L);
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setCreated(new Date());
        playLaunch.setId(PlayLaunch.generateLaunchId());
        playLaunch.setDestinationAccountId(destinationAccountId);
        playLaunch.setDestinationSysType(CDLExternalSystemType.CRM);
        MetadataSegment segment = new MetadataSegment();
        Play play = new Play();
        play.setTargetSegment(segment);
        play.setDescription("play description");
        play.setName(UUID.randomUUID().toString());
        playLaunch.setPlay(play);
        long launchTime = new Date().getTime();
        RatingEngine ratingEngine = new RatingEngine();
        play.setRatingEngine(ratingEngine);
        ratingEngine.setId(ratingId);
        ratingEngine.setType(RatingEngineType.CROSS_SELL);
        AIModel aiModel = new AIModel();
        aiModel.setId(AIModel.generateIdStr());
        aiModel.setCreatedBy(ratingEngine.getCreatedBy());
        aiModel.setUpdatedBy(ratingEngine.getUpdatedBy());
        aiModel.setRatingEngine(ratingEngine);
        ratingEngine.setLatestIteration(aiModel);
        String saltHint = CipherUtils.generateKey();
        String key = CipherUtils.generateKey();
        String pw = CipherUtils.encrypt(dataDbPassword, key, saltHint);

        PlayLaunchSparkContext sparkContext = new PlayLaunchSparkContextBuilder()//
                .tenant(tenant) //
                .playName(play.getName()) //
                .playLaunchId(playLaunch.getId()) //
                .playLaunch(playLaunch) //
                .play(play) //
                .ratingEngine(ratingEngine) //
                .segment(segment) //
                .launchTimestampMillis(launchTime) //
                .ratingId(ratingId) //
                .publishedIteration(aiModel) //
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
        return new Object[][] { //
                { CDLExternalSystemName.Salesforce, false }, //
                { CDLExternalSystemName.Salesforce, true }, //
                { CDLExternalSystemName.Marketo, false }, //
                { CDLExternalSystemName.AWS_S3, false }, //
                { CDLExternalSystemName.GoogleAds, false }, //
                { CDLExternalSystemName.Facebook, false }, //
                { CDLExternalSystemName.LinkedIn, false } //
        };
    }

}
