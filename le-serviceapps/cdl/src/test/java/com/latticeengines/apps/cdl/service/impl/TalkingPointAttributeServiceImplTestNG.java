package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.TalkingPointAttributeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.TalkingPointAttribute;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

public class TalkingPointAttributeServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private TalkingPointAttributeService talkingPointAttributeService;

    private ServingStoreProxy spiedServingStoreProxy;

    private final String tenantName = "TalkingPointAttributeTestTenant";
    private final String accountAttributePrefix = "Account.";

    @BeforeClass(groups = "functional")
    public void setup() {

        spiedServingStoreProxy = spy(new ServingStoreProxy() {
            @Override
            public List<ColumnMetadata> getDecoratedMetadataFromCache(String customerSpace,
                    BusinessEntity entity) {
                return null;
            }

            @Override
            public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace,
                    BusinessEntity entity, List<ColumnSelection.Predefined> groups) {
                return null;
            }

            @Override
            public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace,
                    BusinessEntity entity, List<ColumnSelection.Predefined> groups,
                    DataCollection.Version version) {
                return null;
            }

            @Override
            public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace) {
                return null;
            }

            @Override
            public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, Version version) {
                return null;
            }

            @Override
            public Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace) {
                return null;
            }

            @Override
            public Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace,
                    Boolean allCustomerAttrs, Version version) {
                return null;
            }

            @Override
            public Set<String> getServingStoreColumnsFromCache(String customerSpace,
                    BusinessEntity entity) {
                return null;
            }
        });
        ((TalkingPointAttributeServiceImpl) talkingPointAttributeService)
                .setServingStoreProxy(spiedServingStoreProxy);

        ColumnMetadata at = new ColumnMetadata();
        at.setAttrName("something");
        at.setDisplayName("something more");
        at.setCategoryByString("My Attributes");
        at.enableGroupIfNotPresent(ColumnSelection.Predefined.TalkingPoint);
        List<ColumnMetadata> attrList = new ArrayList<>();
        attrList.add(at);

        at = new ColumnMetadata();
        at.setAttrName("something1");
        at.setDisplayName("something more 1");
        at.setCategoryByString("My Attributes");
        at.enableGroupIfNotPresent(ColumnSelection.Predefined.TalkingPoint);
        attrList.add(at);

        at = new ColumnMetadata();
        at.setAttrName("something3");
        at.setDisplayName("something more 2");
        at.enableGroupIfNotPresent(ColumnSelection.Predefined.CompanyProfile);
        attrList.add(at);

        doReturn(attrList).when(spiedServingStoreProxy).getDecoratedMetadataFromCache(tenantName,
                BusinessEntity.Account);
    }

    @Test(groups = "functional")
    public void testGetAccountAttributes() {
        List<TalkingPointAttribute> attributes =
                talkingPointAttributeService.getAccountAttributes(tenantName);
        Assert.assertNotNull(attributes);
        Assert.assertEquals(2, attributes.size());
        Assert.assertTrue(attributes.get(0).getValue().startsWith(accountAttributePrefix));
        Assert.assertEquals(attributes.get(0).getCategory(), "My Attributes");
        Assert.assertTrue(attributes.get(1).getValue().startsWith(accountAttributePrefix));
        Assert.assertEquals(attributes.get(1).getCategory(), "My Attributes");
    }

    @Test(groups = "functional")
    public void testGetRecommendationAttributes() {
        List<TalkingPointAttribute> attributes =
                talkingPointAttributeService.getRecommendationAttributes(tenantName);
        Assert.assertNotNull(attributes);
        Assert.assertEquals(8, attributes.size());
    }

    @Test(groups = "functional")
    public void testAttributes() {
        List<String> notions = Arrays.asList( //
                "RecoMMendation", "accOUNT", "something", "invalid", "account", "account",
                "Variable");
        TalkingPointNotionAttributes notionAttributes =
                talkingPointAttributeService.getAttributesForNotions(notions, tenantName);
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
    public void teardown() throws Exception {}
}
