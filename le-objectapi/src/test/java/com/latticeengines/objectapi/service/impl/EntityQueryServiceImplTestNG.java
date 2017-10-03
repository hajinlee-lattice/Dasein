package com.latticeengines.objectapi.service.impl;

import static org.mockito.ArgumentMatchers.any;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
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
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.objectapi.functionalframework.ObjectApiFunctionalTestNGBase;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
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
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "CompanyName").gte("a").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(2510L));
    }

    @Test(groups = "functional")
    public void testAccountContactCount() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction1 = new FrontEndRestriction();
        Restriction restriction1 = Restriction.builder().let(BusinessEntity.Account, "CompanyName").gte("a").build();
        frontEndRestriction1.setRestriction(restriction1);
        frontEndQuery.setAccountRestriction(frontEndRestriction1);

        FrontEndRestriction frontEndRestriction2 = new FrontEndRestriction();
        Restriction restriction2 = Restriction.builder().let(BusinessEntity.Contact, "Title").gte("VP").build();
        frontEndRestriction2.setRestriction(restriction2);
        frontEndQuery.setContactRestriction(frontEndRestriction2);

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(25L));

        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        count = entityQueryService.getCount(frontEndQuery);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(42L));
    }

    @Test(groups = "functional")
    public void testContactDataWithAccountIds() {
        // mimic a segment get from metadata api
        MetadataSegment segment = new MetadataSegment();
        Restriction accountRestrictionInternal = Restriction.builder().let(BusinessEntity.Account, "CompanyName")
                .gte("a").build();
        segment.setAccountRestriction(accountRestrictionInternal);
        Restriction contactRestrictionInternal = Restriction.builder().let(BusinessEntity.Contact, "Title").gte("VP")
                .build();
        segment.setContactRestriction(contactRestrictionInternal);

        // front end query from segment restrictions
        FrontEndQuery queryFromSegment = FrontEndQuery.fromSegment(segment);

        // get account ids first
        queryFromSegment.setMainEntity(BusinessEntity.Account);
        queryFromSegment.setLookups( //
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())));
        queryFromSegment.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())),
                false));
        queryFromSegment.setPageFilter(new PageFilter(0, 10));
        DataPage dataPage = entityQueryService.getData(queryFromSegment);
        List<Object> accountIds = dataPage.getData().stream().map(m -> m.get(InterfaceName.AccountId.name())) //
                .collect(Collectors.toList());
        Assert.assertEquals(accountIds.size(), 10);

        // extract contact restriction
        Restriction extractedContactRestriction = queryFromSegment.getContactRestriction().getRestriction();
        Restriction accountIdRestriction = Restriction.builder()
                .let(BusinessEntity.Contact, InterfaceName.AccountId.name()).inCollection(accountIds).build();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction(
                Restriction.builder().and(extractedContactRestriction, accountIdRestriction).build());
        FrontEndQuery contactQuery = new FrontEndQuery();
        contactQuery.setMainEntity(BusinessEntity.Contact);
        contactQuery.setContactRestriction(frontEndRestriction);
        contactQuery.setLookups(Arrays.asList( //
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.AccountId.name()),
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactId.name())));
        contactQuery.setPageFilter(new PageFilter(0, 100));
        dataPage = entityQueryService.getData(contactQuery);
        Assert.assertEquals(dataPage.getData().size(), 19);
        for (Map<String, Object> contact : dataPage.getData()) {
            Object accountId = contact.get(InterfaceName.AccountId.name());
            Assert.assertTrue(accountIds.contains(accountId));
        }
        FrontEndQuery emptyContactQuery = new FrontEndQuery();
        emptyContactQuery.setMainEntity(BusinessEntity.Contact);
        emptyContactQuery.setContactRestriction(frontEndRestriction);
        dataPage = entityQueryService.getData(emptyContactQuery);
        Assert.assertEquals(dataPage.getData().size(), 19);
        for (Map<String, Object> contact : dataPage.getData()) {
            Assert.assertTrue(contact.containsKey(InterfaceName.CompanyName.toString()));
        }
    }

    @Test(groups = "functional")
    public void testScoreData() {
        RatingModel model = ruleBasedModel();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "CompanyName").gte("D").build();
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

        frontEndQuery.setLookups(Arrays.asList(new AttributeLookup(BusinessEntity.Account, "AccountId"),
                new AttributeLookup(BusinessEntity.Rating, model.getId())));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        dataPage = entityQueryService.getData(frontEndQuery);
        Assert.assertNotNull(dataPage);
        data = dataPage.getData();
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(model.getId()));
            String score = (String) row.get(model.getId());
            Assert.assertNotNull(score);
            Assert.assertTrue(Arrays.asList(RuleBucketName.A.getName(), RuleBucketName.C.getName()).contains(score));
        });

        // only get scores for A, A- and B
        Restriction selectedScores = Restriction.builder().let(BusinessEntity.Rating, model.getId()).inCollection(
                Arrays.asList(RuleBucketName.A.getName(), RuleBucketName.A_MINUS.getName(), RuleBucketName.B.getName()))
                .build();
        Restriction restriction2 = Restriction.builder().and(restriction, selectedScores).build();
        frontEndRestriction.setRestriction(restriction2);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        dataPage = entityQueryService.getData(frontEndQuery);
        Assert.assertNotNull(dataPage);
        data = dataPage.getData();
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(model.getId()));
            String score = (String) row.get(model.getId());
            Assert.assertEquals(score, RuleBucketName.A.getName());
        });
    }

    @Test(groups = "functional")
    public void testScoreCount() {
        RatingModel model = ruleBasedModel();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "CompanyName").gte("A").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setMainEntity(BusinessEntity.Account);

        Map<String, Long> ratingCounts = entityQueryService.getRatingCount(frontEndQuery);
        Assert.assertNotNull(ratingCounts);
        Assert.assertFalse(ratingCounts.isEmpty());
        ratingCounts.forEach((score, count) -> {
            if (RuleBucketName.A.getName().equals(score)) {
                Assert.assertEquals((long) count, 8576L);
            } else if (RuleBucketName.C.getName().equals(score)) {
                Assert.assertEquals((long) count, 55643L);
            }
        });
    }

    private void mockDataCollectionProxy() {
        DataCollectionProxy proxy = Mockito.mock(DataCollectionProxy.class);
        Mockito.when(proxy.getAttrRepo(any())).thenReturn(attrRepo);
        queryEvaluatorService.setDataCollectionProxy(proxy);
    }

    private RuleBasedModel ruleBasedModel() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = RatingRule.constructDefaultRule();

        Map<String, Restriction> ruleA = new HashMap<>();
        ruleA.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                Restriction.builder().let(BusinessEntity.Account, "CompanyName").in("B", "G").build());
        ruleA.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                Restriction.builder().let(BusinessEntity.Contact, "Title").in("A", "N").build());
        rule.getBucketToRuleMap().put(RuleBucketName.A.getName(), ruleA);

        Map<String, Restriction> ruleC = new HashMap<>();
        ruleC.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                Restriction.builder().let(BusinessEntity.Account, "CompanyName").in("H", "N").build());
        rule.getBucketToRuleMap().put(RuleBucketName.C.getName(), ruleC);

        model.setRatingRule(rule);

        return model;
    }

}
