package com.latticeengines.apps.cdl.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

import reactor.core.publisher.Flux;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp
 */
abstract class ServingStoreServiceImplEntityMatchDeploymentTestNGBase extends ServingStoreDeploymentTestNGBase {

    @Test(groups = "deployment-app")
    private void testDecoratedMetadata() {
        testAccountMetadata();
        testContactMetadata();
        testCustomerAttrs();
        testModelAttrs();
        testGetAttributesUsage();
        testGetDecoratedMetadata();
    }

    // AttributeName -> ColumnMetadata (Only involve columns to verify, not
    // complete)
    @Override
    protected Map<String, ColumnMetadata> getAccountMetadataToVerify() {
        Map<String, ColumnMetadata> cms = new HashMap<>();
        cms.put(InterfaceName.CustomerAccountId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.CustomerAccountId.name()) //
                .withCategory(Category.ACCOUNT_ATTRIBUTES) //
                .withSubcategory(Category.SUB_CAT_ACCOUNT_IDS) //
                .withGroups(ColumnSelection.Predefined.Enrichment) //
                .canEnrich(Boolean.TRUE) //
                .canSegment(Boolean.TRUE) //
                .canModel(Boolean.FALSE) //
                .build());
        cms.put(InterfaceName.AccountId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.AccountId.name()) //
                .withCategory(Category.ACCOUNT_ATTRIBUTES) //
                .withSubcategory(Category.SUB_CAT_ACCOUNT_IDS) //
                .withGroups(ColumnSelection.Predefined.Enrichment) //
                .canEnrich(Boolean.TRUE) //
                .canSegment(Boolean.FALSE) //
                .canModel(Boolean.FALSE) //
                .build());
        return cms;
    }

    // AttributeName -> ColumnMetadata (Only involve columns to verify, not
    // complete)
    @Override
    protected Map<String, ColumnMetadata> getContactMetadataToVerify() {
        Map<String, ColumnMetadata> cms = new HashMap<>();
        cms.put(InterfaceName.CustomerContactId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.CustomerContactId.name()) //
                .withCategory(Category.CONTACT_ATTRIBUTES) //
                .withSubcategory(Category.SUB_CAT_OTHER) //
                .withGroups(ColumnSelection.Predefined.Enrichment) //
                .canEnrich(Boolean.TRUE) //
                .canSegment(Boolean.TRUE) //
                .canModel(Boolean.TRUE) //
                .build());
        cms.put(InterfaceName.CustomerAccountId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.CustomerAccountId.name()) //
                .withCategory(Category.CONTACT_ATTRIBUTES) //
                .withSubcategory(Category.SUB_CAT_OTHER) //
                .build());
        cms.put(InterfaceName.ContactId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.ContactId.name()) //
                .withCategory(Category.CONTACT_ATTRIBUTES) //
                .withSubcategory(Category.SUB_CAT_OTHER) //
                .withGroups(ColumnSelection.Predefined.Enrichment) //
                .canEnrich(Boolean.TRUE) //
                .canSegment(Boolean.FALSE) //
                .canModel(Boolean.FALSE) //
                .build());
        return cms;
    }

    protected void testCustomerAttrs() {
        Flux<ColumnMetadata> customerAccountAttrs = servingStoreService.getDecoratedMetadata(mainCustomerSpace,
                BusinessEntity.Account, null, null, null, StoreFilter.NON_LDC);
        Map<String, String> nameMap = customerAccountAttrs.filter(
                clm -> StringUtils.isNotEmpty(clm.getAttrName()) && StringUtils.isNotEmpty(clm.getDisplayName()))
                .collectMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName).block();
        Assert.assertNotNull(nameMap);
        Assert.assertTrue(nameMap.containsKey(ACCOUNT_SYSTEM_ID));
        Assert.assertEquals(nameMap.get(ACCOUNT_SYSTEM_ID), "DefaultSystem Account ID");
        Flux<ColumnMetadata> customerContactAttrs = servingStoreService.getDecoratedMetadata(mainCustomerSpace,
                BusinessEntity.Contact, null, null, null, StoreFilter.NON_LDC);
        nameMap = customerContactAttrs.filter(
                clm -> StringUtils.isNotEmpty(clm.getAttrName()) && StringUtils.isNotEmpty(clm.getDisplayName()))
                .collectMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName).block();
        Assert.assertNotNull(nameMap);
        Assert.assertTrue(nameMap.containsKey(OTHERSYSTEM_ACCOUNT_SYSTEM_ID));
        Assert.assertEquals(nameMap.get(OTHERSYSTEM_ACCOUNT_SYSTEM_ID), "DefaultSystem_2 Account ID");
    }

    @Override
    protected int expectedAttrsInSet() {
        return 3; // CustomerContactId ?
    }

}
