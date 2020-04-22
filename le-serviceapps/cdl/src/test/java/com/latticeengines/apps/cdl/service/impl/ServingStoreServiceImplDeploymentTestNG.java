package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp
 */
public class ServingStoreServiceImplDeploymentTestNG extends ServingStoreDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ServingStoreServiceImplDeploymentTestNG.class);

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Test(groups = "deployment-app")
    public void testDecoratedMetadata() {
        testAccountMetadata();
        testContactMetadata();
    }

    @Test(groups = "deployment-app")
    public void testModelAttrs() {
        Flux<ColumnMetadata> newModelingAttrs = servingStoreProxy.getNewModelingAttrs(mainTestTenant.getId());
        Predicate<ColumnMetadata> p = attr -> ApprovedUsage.MODEL_ALLINSIGHTS.equals(attr.getApprovedUsageList().get(0))
                && attr.getTagList() != null;
        Assert.assertTrue(newModelingAttrs.all(p).block());

        Flux<ColumnMetadata> allModelingAttrs = servingStoreProxy.getAllowedModelingAttrs(mainTestTenant.getId());
        p = ColumnMetadata::getCanModel;
        Assert.assertTrue(allModelingAttrs.all(p).block());
    }

    @Test(groups = "deployment-app")
    public void testCustomerAttrs() {
        Flux<ColumnMetadata> customerAccountAttrs = servingStoreProxy.getDecoratedMetadata(mainTestTenant.getId(),
                BusinessEntity.Account, null, null, StoreFilter.NON_LDC);
        Map<String, String> nameMap = customerAccountAttrs.filter(
                clm -> StringUtils.isNotEmpty(clm.getAttrName()) && StringUtils.isNotEmpty(clm.getDisplayName()))
                .collectMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName).block();
        Assert.assertNotNull(nameMap);
        Assert.assertTrue(nameMap.containsKey(ACCOUNT_SYSTEM_ID));
        Assert.assertEquals(nameMap.get(ACCOUNT_SYSTEM_ID), "DefaultSystem Account ID");
        Flux<ColumnMetadata> customerContactAttrs = servingStoreProxy.getDecoratedMetadata(mainTestTenant.getId(),
                BusinessEntity.Contact, null, null, StoreFilter.NON_LDC);
        nameMap = customerContactAttrs.filter(
                clm -> StringUtils.isNotEmpty(clm.getAttrName()) && StringUtils.isNotEmpty(clm.getDisplayName()))
                .collectMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName).block();
        Assert.assertNotNull(nameMap);
        Assert.assertTrue(nameMap.containsKey(OTHERSYSTEM_ACCOUNT_SYSTEM_ID));
        Assert.assertEquals(nameMap.get(OTHERSYSTEM_ACCOUNT_SYSTEM_ID), "DefaultSystem_2 Account ID");

    }

    @Test(groups = "deployment-app")
    public void testGetAttributesUsage() {
        List<String> contactAttrs = Arrays.asList(InterfaceName.FirstName.name(), InterfaceName.LastName.name());
        Map<String, Boolean> attrUsage = servingStoreProxy.getAttrsUsage(mainTestTenant.getId(), BusinessEntity.Contact,
                Predefined.Enrichment, contactAttrs.stream().collect(Collectors.toSet()), null);
        Assert.assertFalse(attrUsage.get(InterfaceName.FirstName.name()));
        Assert.assertFalse(attrUsage.get(InterfaceName.LastName.name()));

        List<String> accountAttrs = Arrays.asList(InterfaceName.LDC_Name.name(), "LDC_Domain");
        attrUsage = servingStoreProxy.getAttrsUsage(mainTestTenant.getId(), BusinessEntity.Account,
                Predefined.Enrichment, accountAttrs.stream().collect(Collectors.toSet()), null);
        Assert.assertTrue(attrUsage.get(InterfaceName.LDC_Name.name()));
        Assert.assertTrue(attrUsage.get("LDC_Domain"));
    }

    // AttributeName -> ColumnMetadata (Only involve columns to verify, not
    // complete)
    @Override
    protected Map<String, ColumnMetadata> getAccountMetadataToVerify() {
        Map<String, ColumnMetadata> cms = new HashMap<>();
        cms.put(InterfaceName.AccountId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.AccountId.name()) //
                .withCategory(Category.ACCOUNT_ATTRIBUTES) //
                .withSubcategory(Category.SUB_CAT_ACCOUNT_IDS) //
                .withGroups(ColumnSelection.Predefined.TalkingPoint, ColumnSelection.Predefined.Enrichment,
                        ColumnSelection.Predefined.Segment) //
                .build());
        return cms;
    }

    // AttributeName -> ColumnMetadata (Only involve columns to verify, not
    // complete)
    @Override
    protected Map<String, ColumnMetadata> getContactMetadataToVerify() {
        Map<String, ColumnMetadata> cms = new HashMap<>();
        cms.put(InterfaceName.ContactId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.AccountId.name()) //
                .withCategory(Category.CONTACT_ATTRIBUTES) //
                .withSubcategory("Other") //
                .withGroups(ColumnSelection.Predefined.TalkingPoint, ColumnSelection.Predefined.Enrichment,
                        ColumnSelection.Predefined.Segment) //
                .canModel(Boolean.FALSE) //
                .build());
        cms.put(InterfaceName.AccountId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.ContactId.name()) //
                .withCategory(Category.CONTACT_ATTRIBUTES) //
                .withSubcategory("Other") //
                .build());
        return cms;
    }

}
