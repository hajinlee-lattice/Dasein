package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;

public class RatingEngineServiceImplUnitTestNG {

    private static final String TENANT = "tenant";
    private static final String ID = "ratingId";
    private static final String DISPLAY_NAME = "rating engine";
    private static final String DESCRIPTION = "description";
    private static final String CREATED_BY = "testuser@lattice-engines.com";
    private static final Date DATE = new Date();
    private static final String id1 = "id1";
    private static final String id2 = "id2";

    @Mock
    private DataFeedService dataFeedService;

    @Mock
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Mock
    private BucketedScoreProxy bucketedScoreProxy;

    @InjectMocks
    @Spy
    private RatingEngineServiceImpl ratingEngineService;

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockDataFeedProxy();
        mockRatingEngineEntityMgr();
        mockRatingEngineService();
        mockTenantContext();
        ForkJoinPool tpForParallelStream = ThreadPoolUtils.getForkJoinThreadPool("rating-details-fetcher", 2);
        ratingEngineService.setTpForParallelStream(tpForParallelStream);
    }

    @Test(groups = "unit")
    public void testConstructRatingEngineSummary() {
        RatingEngineSummary ratingEngineSummary = ratingEngineService
                .constructRatingEngineSummary(createDefaultRatingEngine(), TENANT);
        validateRatingEngineSummary(ratingEngineSummary);
    }

    @Test(groups = "unit")
    public void testGetAllRatingEngineSummaries() {
        when(bucketedScoreProxy.getAllPublishedBucketMetadataByModelSummaryIdList(anyString(), anyList()))
                .thenReturn(Collections.emptyMap());
        List<RatingEngineSummary> summaryList = ratingEngineService.getAllRatingEngineSummaries(
                RatingEngineType.RULE_BASED.name(), RatingEngineStatus.ACTIVE.toString(), true);
        Assert.assertEquals(summaryList.size(), 1);
        Assert.assertEquals(summaryList.get(0).getId(), id1);
    }

    @Test(groups = "unit")
    public void testAIRatingModel() {
        AIModel aiModel = new AIModel();
        aiModel.setId(AIModel.generateIdStr());
    }

    @Test(groups = "unit")
    public void testGenerateReplicaName() {
        String name = "Rating Engine";
        String replicaName = RatingEngineServiceImpl.generateReplicaName(name);
        Assert.assertTrue(replicaName.startsWith("Replica"), replicaName);
        Assert.assertTrue(replicaName.endsWith(name), replicaName);

        replicaName = RatingEngineServiceImpl.generateReplicaName(replicaName);
        Assert.assertFalse(replicaName.replace("Replica", "").contains("Replica"), replicaName);
    }

    private void mockDataFeedProxy() {
        DataFeed dataFeed = new DataFeed();
        dataFeed.setLastPublished(DATE);
        when(dataFeedService.getDefaultDataFeed(anyString())).thenReturn(dataFeed);
    }

    private void mockRatingEngineEntityMgr() {
        when(ratingEngineEntityMgr.findAllByTypeAndStatus(anyString(), anyString()))
                .thenReturn(generateRatingEngineList());
    }

    private void mockRatingEngineService() {
        doReturn(Collections.singleton(id1)).when(ratingEngineService).getPublishedRatingEngineIds();
    }

    private RatingEngine createDefaultRatingEngine() {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ID);
        ratingEngine.setDisplayName(DISPLAY_NAME);
        ratingEngine.setDescription(DESCRIPTION);
        ratingEngine.setNote(null);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setStatus(RatingEngineStatus.INACTIVE);
        ratingEngine.setSegment(null);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setUpdatedBy(CREATED_BY);
        ratingEngine.setCreated(DATE);
        ratingEngine.setUpdated(DATE);
        AIModel am = new AIModel();
        am.setId(AIModel.generateIdStr());
        ratingEngine.setLatestIteration(am);
        return ratingEngine;
    }

    private void mockTenantContext() {
        Tenant tenant = new Tenant();
        tenant.setId("tenant");
        tenant.setPid(1L);
        MultiTenantContext.setTenant(tenant);
    }

    private void validateRatingEngineSummary(RatingEngineSummary ratingEngineSummary) {
        Assert.assertNotNull(ratingEngineSummary);
        Assert.assertEquals(ratingEngineSummary.getId(), ID);
        Assert.assertEquals(ratingEngineSummary.getDisplayName(), DISPLAY_NAME);
        Assert.assertNull(ratingEngineSummary.getNote());
        Assert.assertEquals(ratingEngineSummary.getType(), RatingEngineType.RULE_BASED);
        Assert.assertEquals(ratingEngineSummary.getStatus(), RatingEngineStatus.INACTIVE);
        Assert.assertNull(ratingEngineSummary.getSegmentDisplayName());
        Assert.assertNull(ratingEngineSummary.getSegmentName());
        Assert.assertEquals(ratingEngineSummary.getCreatedBy(), CREATED_BY);
        Assert.assertEquals(ratingEngineSummary.getCreated(), DATE);
        Assert.assertEquals(ratingEngineSummary.getUpdated(), DATE);
        Assert.assertEquals(ratingEngineSummary.getLastRefreshedDate(), DATE);
    }

    private List<RatingEngine> generateRatingEngineList() {
        List<RatingEngine> ratingEngineList;
        RatingEngine r1 = new RatingEngine();
        r1.setId(id1);
        r1.setType(RatingEngineType.RULE_BASED);
        RuleBasedModel rm = new RuleBasedModel();
        rm.setId(RuleBasedModel.generateIdStr());
        r1.setLatestIteration(rm);

        RatingEngine r2 = new RatingEngine();
        r2.setId(id2);
        r2.setType(RatingEngineType.RULE_BASED);
        rm = new RuleBasedModel();
        rm.setId(RuleBasedModel.generateIdStr());
        r1.setLatestIteration(rm);
        ratingEngineList = Arrays.asList(r1, r2);
        return ratingEngineList;
    }
}
