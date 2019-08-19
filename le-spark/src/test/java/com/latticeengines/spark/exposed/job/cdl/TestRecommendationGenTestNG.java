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

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
            Assert.assertEquals(output, "100");
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
            String accountId = record.get(RecommendationColumnName.ACCOUNT_ID.name()).toString();
            String contacts = record.get(RecommendationColumnName.CONTACTS.name()).toString();
            if (count.get() == 1) {
                List<String> accountCols = record.getSchema().getFields().stream().map(field -> field.name())
                        .collect(Collectors.toList());
                verifyCols(accountCols, accountExpectedCols.getLeft(), accountExpectedCols.getRight());
                ObjectMapper jsonParser = new ObjectMapper();
                try {
                    JsonNode jsonObject = jsonParser.readTree(contacts);
                    Assert.assertTrue(jsonObject.isArray());
                    List<String> contactCols = new ArrayList<>();
                    if (jsonObject.size() > 0) {
                        jsonObject.get(0).fieldNames().forEachRemaining(col -> contactCols.add(col));
                        verifyCols(contactCols, contactExpectedCols.getLeft(), contactExpectedCols.getRight());
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                log.info(String.format("For account %s, contacts are: %s", accountId, contacts));
            }
        });
        Assert.assertEquals(count.get(), 10);
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
                Pair.of(destinationAccountId, String.class), //
                Pair.of(InterfaceName.CompanyName.name(), String.class), //
                Pair.of(InterfaceName.LDC_Name.name(), String.class), //
                Pair.of(ratingId + PlaymakerConstants.RatingScoreColumnSuffix, Integer.class), //
                Pair.of(ratingId, String.class), //
                Pair.of(ratingId + PlaymakerConstants.RatingEVColumnSuffix, String.class) //
        );
        Object[][] accounts = new Object[][] { //
                { "0L", "destinationAccountId", "Lattice", "Lattice Engines", 98, "A", "1000" }, //
                { "1L", "destinationAccountId", "DnB", "DnB", 97, "B", "2000" }, //
                { "2L", "destinationAccountId", "Google", "Google", 98, "C", "3000" }, //
                { "3L", "destinationAccountId", "Facebook", "FB", 93, "E", "1000000" }, //
                { "4L", "destinationAccountId", "Apple", "Apple", null, null, null }, //
                { "5L", "destinationAccountId", "SalesForce", "SalesForce", null, "A", null }, //
                { "6L", "destinationAccountId", "Adobe", "Adobe", 98, null, "1000" }, //
                { "7L", "destinationAccountId", "Eloqua", "Eloqua", 40, "F", "100" }, //
                { "8L", "destinationAccountId", "Dell", "Dell", 8, "F", "10" }, //
                { "9L", "destinationAccountId", "HP", "HP", 38, "E", "500" }, //
        };
        accountData = uploadHdfsDataUnit(accounts, accountFields);

        // the contact schema does not have the Address_Street_1.name for
        // testing the the case where contact schema is not complete
        List<Pair<String, Class<?>>> contactfields = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.ContactId.name(), String.class), //
                Pair.of(InterfaceName.CompanyName.name(), String.class), //
                Pair.of(InterfaceName.Email.name(), String.class), //
                Pair.of(InterfaceName.ContactName.name(), String.class), //
                Pair.of(InterfaceName.City.name(), String.class), //
                Pair.of(InterfaceName.State.name(), String.class), //
                Pair.of(InterfaceName.Country.name(), String.class), //
                Pair.of(InterfaceName.PostalCode.name(), String.class), //
                Pair.of(InterfaceName.PhoneNumber.name(), String.class), //
                Pair.of(InterfaceName.Title.name(), String.class) //
        );

        Object[][] contacts = new Object[accounts.length * contactPerAccount][contactfields.size()];
        for (int i = 0; i < accounts.length; i++) {
            for (int j = 0; j < contactPerAccount; j++) {
                contacts[accounts.length * i + j][0] = String.valueOf(i) + "L";
                contacts[accounts.length * i + j][1] = String.valueOf(accounts.length * i + j);
                contacts[accounts.length * i + j][2] = "Kind Inc.";
                contacts[accounts.length * i + j][3] = "michael@kind.com";
                contacts[accounts.length * i + j][4] = "Michael";
                contacts[accounts.length * i + j][5] = "SMO";
                contacts[accounts.length * i + j][6] = "CA";
                contacts[accounts.length * i + j][7] = "US";
                contacts[accounts.length * i + j][8] = "94404";
                contacts[accounts.length * i + j][9] = "650-898-3928";
                contacts[accounts.length * i + j][10] = "CEO";
            }
        }
        contactData = uploadHdfsDataUnit(contacts, contactfields);
        // inputs = Arrays.asList(accountData, contactData);
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
        default:
            return generateSfdcPlayLaunchSparkContext();
        }
    }

    private List<List<Pair<List<String>, Boolean>>> generateExpectedColumns(CDLExternalSystemName destination,
            boolean accountDataOnly) {
        switch (destination) {
        case Salesforce:
            return generateSfdcExpectedColumns(accountDataOnly);
        default:
            return generateSfdcExpectedColumns(accountDataOnly);
        }
    }

    private List<List<Pair<List<String>, Boolean>>> generateSfdcExpectedColumns(boolean accountDataOnly) {
        if (!accountDataOnly) {
            List<Pair<List<String>, Boolean>> list = Arrays.asList(
                    Pair.of(standardRecommendationAccountColumns(), true),
                    Pair.of(standardRecommendationContactColumns(), false));
            return Arrays.asList(list, list);
        } else {
            List<Pair<List<String>, Boolean>> list = Arrays.asList(
                    Pair.of(standardRecommendationAccountColumns(), true), Pair.of(Collections.emptyList(), false));
            return Arrays.asList(list, list);
        }
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

    private PlayLaunchSparkContext generateSfdcPlayLaunchSparkContext() {
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

        PlayLaunchSparkContext sparkContext = new PlayLaunchSparkContextBuilder()//
                .tenant(tenant)//
                .playName(play.getName())//
                .playLaunchId(playLaunch.getId())//
                .playLaunch(playLaunch)//
                .play(play)//
                .ratingEngine(ratingEngine)//
                .segment(segment)//
                .launchTimestampMillis(launchTime)//
                .ratingId(ratingId)//
                .publishedIteration(aiModel)//
                .build();
        return sparkContext;
    }

    @DataProvider
    public Object[][] destinationProvider() {
        return new Object[][] { { CDLExternalSystemName.Salesforce, false }, //
                { CDLExternalSystemName.Salesforce, true } };
    }

}
