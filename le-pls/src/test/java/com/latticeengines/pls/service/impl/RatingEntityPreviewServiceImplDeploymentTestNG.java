package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.pls.service.RatingEngineService;
import com.latticeengines.pls.service.RatingEntityPreviewService;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public class RatingEntityPreviewServiceImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final String RATING_BUCKET_FIELD = "RATING_BUCKET_FIELD";

    @Autowired
    private RatingEntityPreviewService ratingEntityPreviewService;

    @Autowired
    private RatingEngineService ratingEngineService;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    private Play play;

    private EntityProxy entityProxy;

    private RatingEngine ratingEngine;

    private RatingRule ratingRule;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndCreatePlay();

        entityProxy = testPlayCreationHelper.initEntityProxy();

        play = testPlayCreationHelper.getPlay();

        ((RatingEntityPreviewServiceImpl) ratingEntityPreviewService).setEntityProxy(entityProxy);

        ratingEngine = ratingEngineService.getRatingEngineById(play.getRatingEngine().getId(), false);

        Assert.assertNotNull(ratingEngine);
        Set<RatingModel> ratingModels = ratingEngine.getRatingModels();
        Assert.assertNotNull(ratingModels);
        Assert.assertTrue(ratingModels.size() > 0);
        Assert.assertTrue(new ArrayList<>(ratingModels).get(0) instanceof RuleBasedModel);
        RuleBasedModel ruleBasedModel = (RuleBasedModel) new ArrayList<>(ratingModels).get(0);

        ratingRule = ruleBasedModel.getRatingRule();

        Assert.assertNotNull(ratingRule);
        Assert.assertNotNull(ratingRule.getBucketToRuleMap());
        Assert.assertNotNull(ratingRule.getDefaultBucketName());

    }

    @Test(groups = "deployment")
    public void testEntityPreview() {
        DataPage response = ratingEntityPreviewService.getEntityPreview(ratingEngine, 0L, 5L, BusinessEntity.Account,
                InterfaceName.AccountId.name(), false, RATING_BUCKET_FIELD, null, false, null, null);
        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getData());
        Assert.assertTrue(response.getData().size() > 0);

        for (Map<String, Object> row : response.getData()) {
            Assert.assertNotNull(row.get(RATING_BUCKET_FIELD));
            Assert.assertNotNull(row.get(InterfaceName.AccountId.name()));
        }
    }
}
