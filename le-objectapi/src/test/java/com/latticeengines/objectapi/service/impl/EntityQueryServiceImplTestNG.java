package com.latticeengines.objectapi.service.impl;

import static org.mockito.ArgumentMatchers.any;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.objectapi.functionalframework.ObjectApiFunctionalTestNGBase;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class EntityQueryServiceImplTestNG extends ObjectApiFunctionalTestNGBase {

    @Autowired
    private EntityQueryService entityQueryService;

    @Autowired
    private QueryEvaluatorService queryEvaluatorService;

    @BeforeClass(groups = "functional")
    public void setup() {
        mockDataCollectionProxy();
        MultiTenantContext.setTenant(new Tenant("LocalTest"));
    }

    @Test(groups = "functional")
    public void testAccountCount() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "LDC_Name").gte("a").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(1513L));
    }

    @Test(groups = "functional")
    public void testScoreData() {
        RatingModel model = ruleBasedModel();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "LDC_Name").gte("a").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        frontEndQuery.setMainEntity(BusinessEntity.Account);

        DataPage dataPage = entityQueryService.getData(frontEndQuery);
        Assert.assertNotNull(dataPage);
        List<Map<String, Object>> data = dataPage.getData();
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey("Score"));
            String score = (String) row.get("Score");
            Assert.assertNotNull(score);
            Assert.assertTrue(Arrays.asList(RuleBucketName.A.getName(), RuleBucketName.C.getName()).contains(score));
        });

        frontEndQuery.setLookups(Arrays.asList(
                new AttributeLookup(BusinessEntity.Account, "LDC_City"),
                new AttributeLookup(BusinessEntity.Account, "LDC_Country"),
                new AttributeLookup(BusinessEntity.Rating, model.getId())
        ));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        System.out.println(JsonUtils.pprint(frontEndQuery));
        dataPage = entityQueryService.getData(frontEndQuery);
        Assert.assertNotNull(dataPage);
        data = dataPage.getData();
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(model.getId()));
            String score = (String) row.get(model.getId());
            Assert.assertNotNull(score);
            Assert.assertTrue(Arrays.asList(RuleBucketName.A.getName(), RuleBucketName.C.getName()).contains(score));
        });
    }

    @Test(groups = "functional")
    public void testScoreCount() {
        RatingModel model = ruleBasedModel();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "LDC_Name").gte("a").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setMainEntity(BusinessEntity.Account);

        Map<String, Long> ratingCounts = entityQueryService.getRatingCount(frontEndQuery);
        Assert.assertNotNull(ratingCounts);
        Assert.assertFalse(ratingCounts.isEmpty());
        ratingCounts.forEach((score, count) -> {
            if (RuleBucketName.A.getName().equals(score)) {
                Assert.assertEquals((long) count, 420L);
            } else if (RuleBucketName.C.getName().equals(score)) {
                Assert.assertEquals((long) count, 1093L);
            }
        });
    }

    private void mockDataCollectionProxy() {
        DataCollectionProxy proxy = Mockito.mock(DataCollectionProxy.class);
        Mockito.when(proxy.getAttrRepo(any())).thenReturn(getAttrRepo());
        queryEvaluatorService.setDataCollectionProxy(proxy);
    }

    private AttributeRepository getAttrRepo() {
        if (attrRepo == null) {
            synchronized (QueryFunctionalTestNGBase.class) {
                InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("attrrepo.json");
                attrRepo = JsonUtils.deserialize(is, AttributeRepository.class);
            }
        }
        return attrRepo;
    }

    private RuleBasedModel ruleBasedModel() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = new RatingRule();

        Map<String, Restriction> ruleA = new HashMap<>();
        ruleA.put(RatingRule.ACCOUNT_RULE, Restriction.builder().let(BusinessEntity.Account, "LDC_Name").in("b", "g").build());
        rule.getBucketToRuleMap().put(RuleBucketName.A.getName(), ruleA);

        Map<String, Restriction> ruleC = new HashMap<>();
        ruleC.put(RatingRule.ACCOUNT_RULE, Restriction.builder().let(BusinessEntity.Account, "LDC_Name").in("h", "n").build());
        rule.getBucketToRuleMap().put(RuleBucketName.C.getName(), ruleC);

        model.setRatingRule(rule);

        return model;
    }

}
