package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext;
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

    @Test(groups = "functional")
    public void runTest() {
        uploadInputAvro();
        CreateRecommendationConfig createRecConfig = new CreateRecommendationConfig();
        PlayLaunchSparkContext playLaunchContext = generatePlayLaunchSparkContext();
        createRecConfig.setPlayLaunchSparkContext(playLaunchContext);
        SparkJobResult result = runSparkJob(CreateRecommendationsJob.class, createRecConfig);
        verifyResult(result);
    }

    private PlayLaunchSparkContext generatePlayLaunchSparkContext() {
        Tenant tenant = new Tenant("TestRecommendationGenTenant");
        tenant.setPid(1L);
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setCreated(new Date());
        playLaunch.setId(PlayLaunch.generateLaunchId());
        playLaunch.setDestinationAccountId(destinationAccountId);
        playLaunch.setDestinationSysType(CDLExternalSystemType.CRM);
        Play play = new Play();
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
        aiModel.setRatingEngine(ratingEngine);
        ratingEngine.setLatestIteration(aiModel);

        PlayLaunchSparkContext sparkContext = new PlayLaunchSparkContext(tenant, play.getName(), playLaunch.getId(),
                playLaunch, play, launchTime, ratingId, aiModel);
        return sparkContext;
    }

    @Override
    protected void verifyOutput(String output) {
        log.info("Contact count is " + output);
        Assert.assertEquals(output, "100");
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifyOutput);
    }

    private Boolean verifyOutput(HdfsDataUnit target) {
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(target).forEachRemaining(record -> {
            count.incrementAndGet();
            String accountId = record.get("ACCOUNT_ID").toString();
            String contacts = record.get("CONTACTS").toString();
            if (count.get() == 1) {
                log.info(String.format("For account %s, contacts are: %s", accountId, contacts));
            }
        });
        Assert.assertEquals(count.get(), 10);
        return true;
    }

    @Override
    protected void uploadInputAvro() {
        List<Pair<String, Class<?>>> accountFields = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(destinationAccountId, String.class), //
                Pair.of(InterfaceName.CompanyName.name(), String.class), //
                Pair.of(InterfaceName.LDC_Name.name(), String.class), //
                Pair.of(ratingId + PlaymakerConstants.RatingScoreColumnSuffix, String.class), //
                Pair.of(ratingId, String.class), //
                Pair.of(ratingId + PlaymakerConstants.RatingEVColumnSuffix, String.class) //
        );
        Object[][] accounts = new Object[][] { //
                { "0L", "destinationAccountId", "Lattice", "Lattice Engines", "98", "A", "1000" }, //
                { "1L", "destinationAccountId", "DnB", "DnB", "97", "B", "2000" }, //
                { "2L", "destinationAccountId", "Google", "Google", "98", "C", "3000" }, //
                { "3L", "destinationAccountId", "Facebook", "FB", "93", "E", "1000000" }, //
                { "4L", "destinationAccountId", "Apple", "Apple", null, null, null }, //
                { "5L", "destinationAccountId", "SalesForce", "SalesForce", null, "A", null }, //
                { "6L", "destinationAccountId", "Adobe", "Adobe", "98", null, "1000" }, //
                { "7L", "destinationAccountId", "Eloqua", "Eloqua", "40", "F", "100" }, //
                { "8L", "destinationAccountId", "Dell", "Dell", "8", "F", "10" }, //
                { "9L", "destinationAccountId", "HP", "HP", "38", "E", "500" }, //
        };
        String accountData = uploadHdfsDataUnit(accounts, accountFields);

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
                Pair.of(InterfaceName.Title.name(), String.class), //
                Pair.of(InterfaceName.Address_Street_1.name(), String.class) //
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
                contacts[accounts.length * i + j][11] = "100 San Mateo Dr";
            }
        }
        String contactData = uploadHdfsDataUnit(contacts, contactfields);
        inputs = Arrays.asList(accountData, contactData);
    }

}
