//package com.latticeengines.dante.controller;
//
//import java.util.Arrays;
//import java.util.List;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.testng.Assert;
//import org.testng.annotations.AfterClass;
//import org.testng.annotations.BeforeClass;
//import org.testng.annotations.Test;
//import com.latticeengines.common.exposed.util.JsonUtils;
//import com.latticeengines.dante.testframework.DanteTestNGBase;
//import com.latticeengines.domain.exposed.dante.TalkingPointAttribute;
//import com.latticeengines.domain.exposed.dante.TalkingPointNotionAttributes;
//import com.latticeengines.proxy.exposed.dante.TalkingPointsAttributesProxy;
//
//public class TalkingPointAttributesResourceDeploymentTestNG extends DanteTestNGBase {
//
//    @Autowired
//    private TalkingPointsAttributesProxy talkingPointsAttributesProxy;
//
//    @BeforeClass(groups = "deployment")
//    public void setup() {
//        super.setupRunEnvironment();
//    }
//
//    @Test(groups = "deployment")
//    public void testGetRecommendationAttributes() {
//        List<TalkingPointAttribute> rawAttributes = talkingPointsAttributesProxy
//                .getRecommendationAttributes(mainTestCustomerSpace.toString());
//        List<TalkingPointAttribute> attributes =
//                JsonUtils.convertList(rawAttributes, TalkingPointAttribute.class);
//
//        Assert.assertNotNull(attributes);
//        Assert.assertEquals(8, attributes.size());
//    }
//
//    @Test(groups = "deployment")
//    public void testAttributes() {
//        List<String> notions = Arrays.asList( //
//                "RecoMMendation", "something", "invalid", "Variable");
//        TalkingPointNotionAttributes notionAttributes = talkingPointsAttributesProxy
//                .getAttributesByNotions(notions, mainTestCustomerSpace.toString());
//        Assert.assertNotNull(notionAttributes);
//        Assert.assertNotNull(notionAttributes.getInvalidNotions());
//        Assert.assertEquals(notionAttributes.getInvalidNotions().size(), 2);
//        Assert.assertEquals(notionAttributes.getNotionAttributes().size(), 2);
//        Assert.assertEquals(notionAttributes.getNotionAttributes().get("recommendation").size(), 8);
//        Assert.assertEquals(notionAttributes.getNotionAttributes().get("variable").size(), 5);
//    }
//
//    @AfterClass(groups = "deployment")
//    public void teardown() throws Exception {
//
//    }
//}
