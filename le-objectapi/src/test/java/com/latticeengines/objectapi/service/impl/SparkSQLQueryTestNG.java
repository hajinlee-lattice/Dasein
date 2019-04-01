package com.latticeengines.objectapi.service.impl;


import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.objectapi.service.RatingQueryService;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.exposed.service.SparkSQLService;
import com.latticeengines.spark.exposed.service.LivySessionService;

/**
 * This is a skeleton test to check basic functionality of spark sql
 * If all the tests in EntityQuery, RatingQuery or EventQuery ServiceImplTestNG
 * can pass on both redshift and spark, should merge tests into those service tests.
 */
public class SparkSQLQueryTestNG extends QueryServiceImplTestNGBase {

    private final class AccountAttr {
        static final String CompanyName = "CompanyName";
    }

    private final class ContactAttr {
        static final String Occupation = "Occupation";
    }

    @Inject
    private EntityQueryService entityQueryService;

    @Inject
    private EventQueryService eventQueryService;

    @Inject
    private RatingQueryService ratingQueryService;

    @Inject
    private LivySessionService sessionService;

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private SparkSQLService sparkSQLService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private LivySession session;
    private int reuseLivySession = 39; // set the session id to reuse.

    @BeforeClass(groups = "functional")
    public void setupBase() {
        setupTestDataWithSpark(3);
        if (reuseLivySession > 0) {
            reuseLivyEnvironment(reuseLivySession);
        } else {
            setupLivyEnvironment();
        }
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void teardown() {
        if (reuseLivySession == 0) {
            // comment out this statement to reuse the livy session in next run
            sessionService.stopSession(session);
        }
    }

    @Test(groups = "functional", enabled = false)
    public void testScoreData() {
        RatingModel model = ruleBasedModel();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName).gte("D").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Analyst"));
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, ContactAttr.Occupation), contactBkt);
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));
        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, AccountAttr.CompanyName)), false));

        DataPage dataPage = ratingQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(dataPage);
        List<Map<String, Object>> data = dataPage.getData();
        Assert.assertEquals(CollectionUtils.size(data), 10);
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(QueryEvaluator.SCORE), JsonUtils.serialize(row));
            String score = (String) row.get(QueryEvaluator.SCORE);
            Assert.assertNotNull(score);
            Assert.assertTrue(
                    Arrays.asList(RatingBucketName.A.getName(), RatingBucketName.C.getName()).contains(score));
        });

        HdfsDataUnit sparkResult = ratingQueryService.getDataViaSparkSQL(frontEndQuery, //
                DataCollection.Version.Blue, session);
        Assert.assertEquals(sparkResult.getCount(), Long.valueOf(10)); // spark result has count
        String avroPath = sparkResult.getPath();
        AvroUtils.AvroFilesIterator iterator = AvroUtils.avroFileIterator(yarnConfiguration, avroPath + "/*.avro");
        iterator.forEachRemaining(record -> {
            System.out.println(record);
//            Assert.assertNotNull(record.get(QueryEvaluator.SCORE));
//            String score = record.get(QueryEvaluator.SCORE).toString();
//            Assert.assertTrue(
//                    Arrays.asList(RatingBucketName.A.getName(), RatingBucketName.C.getName()).contains(score));
        });
    }

    private RuleBasedModel ruleBasedModel() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = RatingRule.constructDefaultRule();

        Map<String, Restriction> ruleA = new HashMap<>();
        ruleA.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                new BucketRestriction(BusinessEntity.Account, AccountAttr.CompanyName, Bucket.rangeBkt("B", "G")));
        ruleA.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                new BucketRestriction(BusinessEntity.Contact, ContactAttr.Occupation, Bucket.rangeBkt("A", "N")));
        rule.getBucketToRuleMap().put(RatingBucketName.A.getName(), ruleA);

        Map<String, Restriction> ruleC = new HashMap<>();
        ruleC.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                new BucketRestriction(BusinessEntity.Account, AccountAttr.CompanyName, Bucket.rangeBkt("H", "N")));
        ruleC.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                new BucketRestriction(BusinessEntity.Contact, ContactAttr.Occupation, Bucket.rangeBkt("A", "N")));
        rule.getBucketToRuleMap().put(RatingBucketName.C.getName(), ruleC);

        rule.setDefaultBucketName(RatingBucketName.A.getName());

        model.setRatingRule(rule);

        return model;
    }

    private void setupLivyEnvironment() {
        session = sparkSQLService.initializeLivySession(attrRepo, tblPathMap);
    }

    private void reuseLivyEnvironment(int sessionId) {
        String livyHost;
        if (Boolean.TRUE.equals(useEmr)) {
            livyHost = emrCacheService.getLivyUrl();
        } else {
            livyHost = "http://localhost:8998";
        }
        session = sessionService.getSession(new LivySession(livyHost, sessionId));
    }

}
