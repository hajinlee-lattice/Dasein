package com.latticeengines.spark.exposed.job.cdl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmakercore.RecommendationColumnName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.DeltaCampaignLaunchSparkContext;
import com.latticeengines.domain.exposed.pls.DeltaCampaignLaunchSparkContext.DeltaCampaignLaunchSparkContextBuilder;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CreateDeltaRecommendationConfig;
import com.latticeengines.spark.testframework.TestJoinTestNGBase;

public class DeltaCampaignLaunchTestNG extends TestJoinTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchTestNG.class);

    private static final String ratingId = RatingEngine.generateIdStr();
    private static final String destinationAccountId = "D41000001Q3z4EAC";
    private static final int completeContactPerAccount = 10;
    private static final int addOrDeleteContactPerAccount = 5;
    private String addAccountData;
    private String addContactData;
    private String deleteAccountData;
    private String deleteContactData;
    private String completeContactData;
    private Object[][] addAccounts;
    private Object[][] addContacts;
    private Object[][] deleteAccounts;
    private Object[][] deleteContacts;
    private Object[][] completeContacts;
    private int targetNum;
    private boolean createRecommendationDataFrame;
    private boolean createAddCsvDataFrame;
    private boolean createDeleteCsvDataFrame;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        uploadInputAvro();
    }

    @Test(groups = "functional", dataProvider = "dataFrameProvider")
    public void runTest(boolean createRecommendationDataFrameVal, boolean createAddCsvDataFrameVal,
            boolean createDeleteCsvDataFrameVal) {
        createRecommendationDataFrame = createRecommendationDataFrameVal;
        createAddCsvDataFrame = createAddCsvDataFrameVal;
        createDeleteCsvDataFrame = createDeleteCsvDataFrameVal;
        overwriteInputUnits();
        CreateDeltaRecommendationConfig sparkConfig = generateCreateDeltaRecommendationConfig();
        SparkJobResult result = runSparkJob(CreateDeltaRecommendationsJob.class, sparkConfig);
        verifyResult(result);
    }

    private void overwriteInputUnits() {
        if (createRecommendationDataFrame && createAddCsvDataFrame && createDeleteCsvDataFrame) {
            // do nothing
        } else if (!createRecommendationDataFrame && !createAddCsvDataFrame && createDeleteCsvDataFrame) {
            // only have delete Accounts and delete contacts
            Map<String, DataUnit> inputUnits = new HashMap<>();
            Assert.assertNotNull(getInputUnits());
            getInputUnits().forEach((k, v) -> {
                inputUnits.put(k, v);
            });
            inputUnits.put("Input0", null);
            inputUnits.put("Input1", null);
            inputUnits.put("Input4", null);
            setInputUnits(inputUnits);
        }
    }

    private CreateDeltaRecommendationConfig generateCreateDeltaRecommendationConfig() {
        if (createRecommendationDataFrame && createAddCsvDataFrame && createDeleteCsvDataFrame) {
            targetNum = 3;
        } else if (!createRecommendationDataFrame && !createAddCsvDataFrame && createDeleteCsvDataFrame) {
            targetNum = 1;
        }
        CreateDeltaRecommendationConfig sparkConfig = new CreateDeltaRecommendationConfig();
        DeltaCampaignLaunchSparkContext deltaCampaignLaunchSparkContext = generateDeltaCampaignLaunchSparkContextForS3();
        sparkConfig.setDeltaCampaignLaunchSparkContext(deltaCampaignLaunchSparkContext);
        sparkConfig.setTargetNums(targetNum);
        return sparkConfig;
    }

    @Override
    public void verifyResult(SparkJobResult result) {
        Assert.assertEquals(result.getTargets().size(), targetNum);
        if (createRecommendationDataFrame && createAddCsvDataFrame && createDeleteCsvDataFrame) {
            HdfsDataUnit recDf = result.getTargets().get(0);
            HdfsDataUnit addCsvDf = result.getTargets().get(1);
            HdfsDataUnit deleteCsvDf = result.getTargets().get(2);

            // Account number assertion
            Assert.assertEquals(recDf.getCount(), addCsvDf.getCount());
            Assert.assertEquals(recDf.getCount().intValue(), addAccounts.length);
            Assert.assertEquals(deleteCsvDf.getCount().intValue(), deleteAccounts.length);

            // Contact number assertion
            try {
                Iterator<GenericRecord> recDfIter = AvroUtils.iterateAvroFiles(yarnConfiguration,
                        PathUtils.toAvroGlob(recDf.getPath()));
                GenericRecord record = recDfIter.next();
                Object contactObject = record.get(RecommendationColumnName.CONTACTS.name());
                ObjectMapper jsonParser = new ObjectMapper();
                JsonNode jsonObject = jsonParser.readTree(contactObject.toString());
                Assert.assertTrue(jsonObject.isArray());
                Assert.assertEquals(jsonObject.size(), completeContactPerAccount);

                Iterator<GenericRecord> addCsvDfIter = AvroUtils.iterateAvroFiles(yarnConfiguration,
                        PathUtils.toAvroGlob(addCsvDf.getPath()));
                record = addCsvDfIter.next();
                contactObject = record.get(RecommendationColumnName.CONTACTS.name());
                jsonObject = jsonParser.readTree(contactObject.toString());
                Assert.assertTrue(jsonObject.isArray());
                Assert.assertEquals(jsonObject.size(), addOrDeleteContactPerAccount);

                Iterator<GenericRecord> deleteCsvDfIter = AvroUtils.iterateAvroFiles(yarnConfiguration,
                        PathUtils.toAvroGlob(deleteCsvDf.getPath()));
                record = deleteCsvDfIter.next();
                contactObject = record.get(RecommendationColumnName.CONTACTS.name());
                jsonObject = jsonParser.readTree(contactObject.toString());
                Assert.assertTrue(jsonObject.isArray());
                Assert.assertEquals(jsonObject.size(), addOrDeleteContactPerAccount);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // external Id assertion
            Map<String, String> accountIdToExternalIdMap = new HashMap<>();
            verifyAndReadTarget(recDf).forEachRemaining(record -> {
                Object externalIdObject = record.get(RecommendationColumnName.EXTERNAL_ID.name());
                Object accountIdObject = record.get(RecommendationColumnName.EXTERNAL_ID.name());
                accountIdToExternalIdMap.put(accountIdObject.toString(), externalIdObject.toString());
            });
            verifyAndReadTarget(addCsvDf).forEachRemaining(record -> {
                Object externalIdObject = record.get(RecommendationColumnName.EXTERNAL_ID.name());
                Object accountIdObject = record.get(RecommendationColumnName.EXTERNAL_ID.name());
                Assert.assertTrue(accountIdToExternalIdMap.containsKey(accountIdObject.toString()));
                Assert.assertEquals(accountIdToExternalIdMap.get(accountIdObject.toString()),
                        externalIdObject.toString());
            });
        } else if (!createRecommendationDataFrame && !createAddCsvDataFrame && createDeleteCsvDataFrame) {
            HdfsDataUnit deleteCsvDf = result.getTargets().get(0);
            Assert.assertEquals(deleteCsvDf.getCount().intValue(), deleteAccounts.length);
            Iterator<GenericRecord> deleteCsvDfIter = AvroUtils.iterateAvroFiles(yarnConfiguration,
                    PathUtils.toAvroGlob(deleteCsvDf.getPath()));
            GenericRecord record = deleteCsvDfIter.next();
            record = deleteCsvDfIter.next();
            Object contactObject = record.get(RecommendationColumnName.CONTACTS.name());
            ObjectMapper jsonParser = new ObjectMapper();
            JsonNode jsonObject;
            try {
                jsonObject = jsonParser.readTree(contactObject.toString());
                Assert.assertTrue(jsonObject.isArray());
                Assert.assertEquals(jsonObject.size(), addOrDeleteContactPerAccount);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    private DeltaCampaignLaunchSparkContext generateDeltaCampaignLaunchSparkContextForS3() {

        Tenant tenant = new Tenant("DeltaCampaignLaunchTestNG");
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

        DeltaCampaignLaunchSparkContext deltaCampaignLaunchSparkContext = new DeltaCampaignLaunchSparkContextBuilder()//
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

        deltaCampaignLaunchSparkContext
                .setAccountColsRecIncluded(CampaignLaunchUtils.generateAccountColsRecIncludedForS3());
        deltaCampaignLaunchSparkContext
                .setAccountColsRecNotIncludedStd(CampaignLaunchUtils.generateAccountColsRecNotIncludedStdForS3());
        deltaCampaignLaunchSparkContext
                .setAccountColsRecNotIncludedNonStd(CampaignLaunchUtils.generateAccountColsRecNotIncludedNonStdForS3());
        deltaCampaignLaunchSparkContext.setContactCols(CampaignLaunchUtils.generateContactColsForS3());
        deltaCampaignLaunchSparkContext.setCreateRecommendationDataFrame(createRecommendationDataFrame);
        deltaCampaignLaunchSparkContext.setCreateAddCsvDataFrame(createAddCsvDataFrame);
        deltaCampaignLaunchSparkContext.setCreateDeleteCsvDataFrame(createDeleteCsvDataFrame);

        return deltaCampaignLaunchSparkContext;
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
        addAccounts = new Object[][] { //
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
        addAccountData = uploadHdfsDataUnit(addAccounts, accountFields);

        // the contact schema does not have the Address_Street_1.name for
        // testing the the case where contact schema is not complete
        List<Pair<String, Class<?>>> contactFields = Arrays.asList( //
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

        addContacts = new Object[(addAccounts.length - 1) * addOrDeleteContactPerAccount][contactFields.size()];
        for (int i = 0; i < (addAccounts.length - 1); i++) {
            for (int j = 0; j < addOrDeleteContactPerAccount; j++) {
                addContacts[addOrDeleteContactPerAccount * i + j][0] = String.valueOf(i) + "L";
                addContacts[addOrDeleteContactPerAccount * i + j][1] = String
                        .valueOf(addOrDeleteContactPerAccount * i + j);
                addContacts[addOrDeleteContactPerAccount * i + j][2] = String.valueOf(addAccounts.length * i + j) + "L";
                addContacts[addOrDeleteContactPerAccount * i + j][3] = "Kind Inc.";
                addContacts[addOrDeleteContactPerAccount * i + j][4] = "michael@kind.com";
                addContacts[addOrDeleteContactPerAccount * i + j][5] = "Michael Jackson";
                addContacts[addOrDeleteContactPerAccount * i + j][6] = "SMO";
                addContacts[addOrDeleteContactPerAccount * i + j][7] = "CA";
                addContacts[addOrDeleteContactPerAccount * i + j][8] = "US";
                addContacts[addOrDeleteContactPerAccount * i + j][9] = "94404";
                addContacts[addOrDeleteContactPerAccount * i + j][10] = "650-898-3928";
                addContacts[addOrDeleteContactPerAccount * i + j][11] = "CEO";
                addContacts[addOrDeleteContactPerAccount * i + j][12] = "Michael";
                addContacts[addOrDeleteContactPerAccount * i + j][13] = "Jackson";
                addContacts[addOrDeleteContactPerAccount * i + j][14] = "08/08/2019";
            }
        }
        addContactData = uploadHdfsDataUnit(addContacts, contactFields);

        deleteAccounts = new Object[][] { //
                { "10L", "0010", "destinationAccountId", "Some Com", "Some Company", 8, "F", "10", "www.some-com.com",
                        "01/01/2019" }, //
                { "11L", "0011", "destinationAccountId", "Random Com", "Random Company", 7, "F", "2",
                        "www.random-com.com", "01/01/2019" } };
        deleteAccountData = uploadHdfsDataUnit(deleteAccounts, accountFields);

        deleteContacts = new Object[(deleteAccounts.length) * addOrDeleteContactPerAccount][contactFields.size()];
        for (int i = 0; i < deleteAccounts.length; i++) {
            for (int j = 0; j < addOrDeleteContactPerAccount; j++) {
                deleteContacts[addOrDeleteContactPerAccount * i + j][0] = String.valueOf(i + 10) + "L";
                deleteContacts[addOrDeleteContactPerAccount * i + j][1] = String
                        .valueOf(addOrDeleteContactPerAccount * i + j);
                deleteContacts[addOrDeleteContactPerAccount * i + j][2] = String.valueOf(deleteAccounts.length * i + j)
                        + "L";
                deleteContacts[addOrDeleteContactPerAccount * i + j][3] = "Kind Inc.";
                deleteContacts[addOrDeleteContactPerAccount * i + j][4] = "michael@kind.com";
                deleteContacts[addOrDeleteContactPerAccount * i + j][5] = "Michael Jackson";
                deleteContacts[addOrDeleteContactPerAccount * i + j][6] = "SMO";
                deleteContacts[addOrDeleteContactPerAccount * i + j][7] = "CA";
                deleteContacts[addOrDeleteContactPerAccount * i + j][8] = "US";
                deleteContacts[addOrDeleteContactPerAccount * i + j][9] = "94404";
                deleteContacts[addOrDeleteContactPerAccount * i + j][10] = "650-898-3928";
                deleteContacts[addOrDeleteContactPerAccount * i + j][11] = "CEO";
                deleteContacts[addOrDeleteContactPerAccount * i + j][12] = "Michael";
                deleteContacts[addOrDeleteContactPerAccount * i + j][13] = "Jackson";
                deleteContacts[addOrDeleteContactPerAccount * i + j][14] = "08/08/2019";
            }
        }
        deleteContactData = uploadHdfsDataUnit(deleteContacts, contactFields);

        completeContacts = new Object[(addAccounts.length - 1) * completeContactPerAccount][contactFields.size()];
        for (int i = 0; i < (addAccounts.length - 1); i++) {
            for (int j = 0; j < completeContactPerAccount; j++) {
                completeContacts[completeContactPerAccount * i + j][0] = String.valueOf(i) + "L";
                completeContacts[completeContactPerAccount * i + j][1] = String
                        .valueOf(completeContactPerAccount * i + j);
                completeContacts[completeContactPerAccount * i + j][2] = String.valueOf(addAccounts.length * i + j)
                        + "L";
                completeContacts[completeContactPerAccount * i + j][3] = "Kind Inc.";
                completeContacts[completeContactPerAccount * i + j][4] = "michael@kind.com";
                completeContacts[completeContactPerAccount * i + j][5] = "Michael Jackson";
                completeContacts[completeContactPerAccount * i + j][6] = "SMO";
                completeContacts[completeContactPerAccount * i + j][7] = "CA";
                completeContacts[completeContactPerAccount * i + j][8] = "US";
                completeContacts[completeContactPerAccount * i + j][9] = "94404";
                completeContacts[completeContactPerAccount * i + j][10] = "650-898-3928";
                completeContacts[completeContactPerAccount * i + j][11] = "CEO";
                completeContacts[completeContactPerAccount * i + j][12] = "Michael";
                completeContacts[completeContactPerAccount * i + j][13] = "Jackson";
                completeContacts[completeContactPerAccount * i + j][14] = "08/08/2019";
            }
        }
        completeContactData = uploadHdfsDataUnit(completeContacts, contactFields);
    }

    @DataProvider
    public Object[][] dataFrameProvider() {
        return new Object[][] { //
                { true, true, true }, // generate all three dataFrames
                { false, false, true } // only generate delete csv dataFrame
        };
    }

}
