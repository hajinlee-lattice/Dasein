package com.latticeengines.objectapi.service.impl.sparksql;

import static com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor.SPARK_TEST_GROUP;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.objectapi.service.RatingQueryService;
import com.latticeengines.objectapi.service.impl.QueryServiceImplTestNGBase;
import com.latticeengines.objectapi.service.sparksql.impl.RatingQueryServiceSparkSQLImpl;
import com.latticeengines.query.evaluator.sparksql.SparkSQLQueryTester;
import com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor;

@Listeners(SparkSQLTestInterceptor.class)
public class RatingQueryServiceImplSparkSQLTestNG extends QueryServiceImplTestNGBase
        implements RedshiftAndSparkQueryObjectAPITester {

    private static Logger log = LoggerFactory.getLogger(RatingQueryServiceImplSparkSQLTestNG.class);

    private final class AccountAttr {
        static final String CompanyName = "CompanyName";
        static final String Intent = "BmbrSurge_AccountOverdrafts_Intent";
    }

    private final class ContactAttr {
        static final String Occupation = "Occupation";
    }

    @Inject
    private SparkSQLQueryTester sparkSQLQueryTester;

    @Resource(name = "ratingQueryServiceSparkSQL")
    private RatingQueryService ratingQueryServiceSparkSQL;

    @Inject
    private RatingQueryService ratingQueryService;

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public SparkSQLQueryTester getSparkSQLQueryTester() {
        return sparkSQLQueryTester;
    }

    @DataProvider(name = "userContexts", parallel = false)
    private Object[][] provideSqlUserContexts() {
        return new Object[][] { { SEGMENT_USER, "Redshift" }, { SPARK_BATCH_USER, "Spark" } };
    }

    @BeforeClass(groups = SPARK_TEST_GROUP)
    public void setupBase() {
        setupTestDataWithSpark(3);
        setupQueryTester(customerSpace, attrRepo, tblPathMap);
        ((RatingQueryServiceSparkSQLImpl) ratingQueryServiceSparkSQL)
                .setLivySession(getSparkSQLQueryTester().getLivySession());
        // Init Mocks
        mockDataCollectionProxy(
                ((RatingQueryServiceSparkSQLImpl) ratingQueryServiceSparkSQL).getQueryEvaluatorService());
    }

    @AfterClass(groups = SPARK_TEST_GROUP, alwaysRun = true)
    public void teardown() {
        teardownQueryTester();
    }

    @Test(groups = SPARK_TEST_GROUP, dataProvider = "userContexts")
    public void testPhAndPh(String sqlUser, String queryContext) {
        AttributeLookup phAttr1 = new AttributeLookup(BusinessEntity.PurchaseHistory, //
                "AM_k4Pb7AhrIccPjW5jXiWzpwWX2mTvY7I__M_7_12__AS");
        AttributeLookup phAttr2 = new AttributeLookup(BusinessEntity.PurchaseHistory, //
                "AM_DkhSLVHpu5iIS5jw4aWOmojNlz7CvAa__Q_2_2__TS");
        AttributeLookup phAttr3 = new AttributeLookup(BusinessEntity.PurchaseHistory, //
                "AM_Hd3c9G01LBGBF7LKRGu9Oc0dS8HnuRdv__W_3__W_4_9__SC");
        Bucket bkt1 = Bucket.rangeBkt(1, null);
        Bucket bkt2 = Bucket.rangeBkt(null, 1);
        Bucket bkt3 = Bucket.chgBkt(Bucket.Change.Direction.INC, Bucket.Change.ComparisonType.AS_MUCH_AS,
                Collections.singletonList(1));
        BucketRestriction res1 = new BucketRestriction(phAttr1, bkt1);
        BucketRestriction res2 = new BucketRestriction(phAttr2, bkt2);
        BucketRestriction res3 = new BucketRestriction(phAttr3, bkt3);
        Restriction res23 = Restriction.builder().or(res2, res3).build();
        Restriction res = Restriction.builder().and(res1, res23).build();
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setAccountRestriction(new FrontEndRestriction(res));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        long count;
        if (SPARK_BATCH_USER.equals(sqlUser)) {
            count = ratingQueryServiceSparkSQL.getCount(frontEndQuery, DataCollection.Version.Blue, //
                    sqlUser);
        } else {
            count = ratingQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        }
        testAndAssertCountFromTester(sqlUser, count, 5806L);
    }

    @Test(groups = SPARK_TEST_GROUP, dataProvider = "userContexts")
    public void testRatingCount(String sqlUser, String queryContext) {
        RatingModel model = ruleBasedModel();
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction accountRestriction = Restriction.builder() //
                .let(BusinessEntity.Account, AccountAttr.CompanyName).gte("A") //
                .build();
        frontEndRestriction.setRestriction(accountRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Analyst"));
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, ContactAttr.Occupation), contactBkt);
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));
        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setMainEntity(BusinessEntity.Account);

        RatingQueryService queryService = SPARK_BATCH_USER.equals(sqlUser) ? //
                ratingQueryServiceSparkSQL : ratingQueryService;
        Map<String, Long> ratingCounts = queryService.getRatingCount(frontEndQuery, DataCollection.Version.Blue, //
                sqlUser);
        Assert.assertTrue(ratingCounts.containsKey("A"), JsonUtils.serialize(ratingCounts));
        Assert.assertTrue(ratingCounts.containsKey("C"), JsonUtils.serialize(ratingCounts));
        Assert.assertTrue(ratingCounts.containsKey("D"), JsonUtils.serialize(ratingCounts));
        testAndAssertCountFromTester(sqlUser, ratingCounts.get("A"), 1687L);
        testAndAssertCountFromTester(sqlUser, ratingCounts.get("C"), 371L);
        testAndAssertCountFromTester(sqlUser, ratingCounts.get("D"), 401L);
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

        Map<String, Restriction> ruleD = new HashMap<>();
        ruleD.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                new BucketRestriction(BusinessEntity.Account, AccountAttr.Intent, Bucket.nullBkt()));
        ruleD.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                new BucketRestriction(BusinessEntity.Contact, ContactAttr.Occupation, Bucket.notNullBkt()));
        rule.getBucketToRuleMap().put(RatingBucketName.D.getName(), ruleD);

        rule.setDefaultBucketName(RatingBucketName.A.getName());

        model.setRatingRule(rule);

        return model;
    }

}
