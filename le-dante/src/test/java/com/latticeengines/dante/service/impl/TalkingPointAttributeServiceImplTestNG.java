package com.latticeengines.dante.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

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
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class TalkingPointAttributeServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private TalkingPointAttributeService talkingPointAttributeService;

    private ServingStoreProxy spiedServingStoreProxy;

    private final Path metadataDocumentPath = new Path("/MetadataDocument.json");

    private final String tenantName = "TalkingPointAttributeTestTenant";
    private final String accountAttributePrefix = "Account.";

    @BeforeClass(groups = "functional")
    public void setup() {

        spiedServingStoreProxy = spy(new ServingStoreProxy() {
            @Override
            public Mono<Long> getDecoratedMetadataCount(String customerSpace, BusinessEntity entity) {
                return null;
            }

            @Override
            public Mono<Long> getDecoratedMetadataCount(String customerSpace, BusinessEntity entity,
                    List<ColumnSelection.Predefined> groups) {
                return null;
            }

            @Override
            public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity) {
                return null;
            }

            @Override
            public List<ColumnMetadata> getDecoratedMetadataFromCache(String customerSpace, BusinessEntity entity) {
                return null;
            }

            @Override
            public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
                    List<ColumnSelection.Predefined> groups) {
                return null;
            }

            @Override
            public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity, List<ColumnSelection.Predefined> groups, DataCollection.Version version) {
                return null;
            }
        });
        ((TalkingPointAttributeServiceImpl) talkingPointAttributeService).setServingStoreProxy(spiedServingStoreProxy);

        ColumnMetadata at = new ColumnMetadata();
        at.setColumnId("something");
        at.setAttrName("something");
        at.setDisplayName("something more");
        Flux<ColumnMetadata> attrFlux = Flux.just(at);

        at = new ColumnMetadata();
        at.setColumnId("something1");
        at.setAttrName("something1");
        at.setDisplayName("something more 1");
        attrFlux = Flux.concat(attrFlux, Flux.just(at));

        doReturn(attrFlux).when(spiedServingStoreProxy).getDecoratedMetadata(tenantName, BusinessEntity.Account,
                Arrays.asList(ColumnSelection.Predefined.TalkingPoint));
    }

    @Test(groups = "functional")
    public void testGetAccountAttributes() {
        List<TalkingPointAttribute> attributes = talkingPointAttributeService.getAccountAttributes(tenantName);
        Assert.assertNotNull(attributes);
        Assert.assertEquals(2, attributes.size());
        Assert.assertTrue(attributes.get(0).getValue().startsWith(accountAttributePrefix));
        Assert.assertTrue(attributes.get(1).getValue().startsWith(accountAttributePrefix));
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
        Assert.assertTrue(notionAttributes.getNotionAttributes().get("account").get(0).getValue()
                .startsWith(accountAttributePrefix));
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("recommendation").size(), 8);
        Assert.assertEquals(notionAttributes.getNotionAttributes().get("variable").size(), 5);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
    }
}