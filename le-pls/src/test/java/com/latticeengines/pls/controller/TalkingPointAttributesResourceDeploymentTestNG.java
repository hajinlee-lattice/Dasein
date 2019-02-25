package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.TalkingPointAttribute;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class TalkingPointAttributesResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        switchToExternalUser();
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment")
    public void testGetRecommendationAttributes() {
        List<TalkingPointAttribute> rawAttributes = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/attributes/recommendationattributes", //
                List.class);
        List<TalkingPointAttribute> attributes =
                JsonUtils.convertList(rawAttributes, TalkingPointAttribute.class);

        Assert.assertNotNull(attributes);
        Assert.assertEquals(8, attributes.size());
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment")
    public void testAttributes() {
        List<String> notions = Arrays.asList("RecoMMendation", "something", "invalid", "Variable");
        TalkingPointNotionAttributes notionAttributes = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/attributes", //
                notions, TalkingPointNotionAttributes.class);
        Assert.assertNotNull(notionAttributes);
        Assert.assertNotNull(notionAttributes.getInvalidNotions());
        Assert.assertEquals(notionAttributes.getInvalidNotions().size(), 2);
        Assert.assertEquals(notionAttributes.getNotionAttributes().size(), 2);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("recommendation").size(), 8);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("variable").size(), 5);
    }

    @AfterClass(groups = "deployment")
    public void teardown() throws Exception {}
}
