package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

public class RatingEngineServiceImplUnitTestNG {

    private static final String TENANT = "tenant";
    private static final String ID = "ratingId";
    private static final String DISPLAY_NAME = "rating engine";
    private static final String CREATED_BY = "testuser@lattice-engines.com";
    private static final Date DATE = new Date();
    private static final Set<RatingModel> ratingModels = new HashSet<>();

    @Mock
    private DataFeedProxy dataFeedProxy;

    @InjectMocks
    private RatingEngineServiceImpl ratingEngineService;

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockDataFeedProxy();
        ratingModels.add(new RuleBasedModel());
    }

    @Test(groups = "unit")
    public void testConstructRatingEngineSummary() {
        RatingEngineSummary ratingEngineSummary = ratingEngineService
                .constructRatingEngineSummary(createDefaultRatingEngine(), TENANT);
        validateRatingEngineSummary(ratingEngineSummary);
    }

    private void mockDataFeedProxy() {
        DataFeed dataFeed = new DataFeed();
        dataFeed.setLastPublished(DATE);
        when(dataFeedProxy.getDataFeed(anyString())).thenReturn(dataFeed);
    }

    private RatingEngine createDefaultRatingEngine() {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ID);
        ratingEngine.setDisplayName(DISPLAY_NAME);
        ratingEngine.setNote(null);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setStatus(RatingEngineStatus.INACTIVE);
        ratingEngine.setSegment(null);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setCreated(DATE);
        ratingEngine.setUpdated(DATE);
        ratingEngine.setRatingModels(ratingModels);
        return ratingEngine;
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
        Assert.assertNotNull(ratingEngineSummary.getRatingModels());
        Assert.assertEquals(ratingEngineSummary.getRatingModels().size(), 1);
        Assert.assertEquals(ratingEngineSummary.getLastRefreshedDate(), DATE);
    }
}
