package com.latticeengines.dante.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dante.service.TalkingPointAttributeService;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dante.TalkingPointAttribute;
import com.latticeengines.domain.exposed.dante.TalkingPointNotionAttributes;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class TalkingPointAttributeServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private TalkingPointAttributeService talkingPointAttributeService;

    private InternalResourceRestApiProxy spiedInternalResourceRestApiProxy;

    private final Path metadataDocumentPath = new Path("/MetadataDocument.json");

    private final String tenantName = "TalkingPointAttributeTestTenant";

    @BeforeClass(groups = "functional")
    public void setup() {

        spiedInternalResourceRestApiProxy = spy(new InternalResourceRestApiProxy("doesn't matter"));
        ((TalkingPointAttributeServiceImpl) talkingPointAttributeService)
                .setInternalResourceRestApiProxy(spiedInternalResourceRestApiProxy);

        List<ColumnMetadata> attrs = new ArrayList<>();
        ColumnMetadata at = new ColumnMetadata();
        at.setColumnId("something");
        at.setDisplayName("something more");
        attrs.add(at);
        at = new ColumnMetadata();
        at.setColumnId("something1");
        at.setDisplayName("something more 1");
        attrs.add(at);
        doReturn(attrs).when(spiedInternalResourceRestApiProxy).getAttributesInPredefinedGroup(
                ColumnSelection.Predefined.TalkingPoint, tenantName + "." + tenantName + "." + "Production");
    }

    @Test(groups = "functional")
    public void testGetAccountAttributes() {
        List<TalkingPointAttribute> attributes = talkingPointAttributeService.getAccountAttributes(tenantName);
        Assert.assertNotNull(attributes);
        Assert.assertEquals(2, attributes.size());
    }

    @Test(groups = "functional")
    public void testGetRecommendationAttributes() {
        List<TalkingPointAttribute> attributes = talkingPointAttributeService.getRecommendationAttributes(tenantName);
        Assert.assertNotNull(attributes);
        Assert.assertEquals(8, attributes.size());
    }

    @Test(groups = "functional")
    public void testAttributes() {
        List<String> notions = Arrays.asList( //
                "RecoMMendation", "accOUNT", "something", "invalid", "account", "account", "Variable");
        TalkingPointNotionAttributes notionAttributes = talkingPointAttributeService.getAttributesForNotions(notions,
                tenantName);
        Assert.assertNotNull(notionAttributes);
        Assert.assertNotNull(notionAttributes.getInvalidNotions());
        Assert.assertEquals(notionAttributes.getInvalidNotions().size(), 2);
        Assert.assertEquals(notionAttributes.getNotionAttributes().size(), 3);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("account").size(), 2);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("recommendation").size(), 8);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("variable").size(), 5);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
    }
}
