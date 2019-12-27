package com.latticeengines.apps.cdl.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp
 */
public class ServingStoreServiceImplDeploymentTestNG extends ServingStoreDeploymentTestNGBase {

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
                BusinessEntity.Account, null, null, StoreFilter.NO_LDC);
        Map<String, String> nameMap = customerAccountAttrs
                .filter(clm -> StringUtils.isNotEmpty(clm.getAttrName()) && StringUtils.isNotEmpty(clm.getDisplayName()))
                .collectMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName)
                .block();
        Assert.assertNotNull(nameMap);
        Assert.assertTrue(nameMap.containsKey(ACCOUNT_SYSTEM_ID));
        Assert.assertEquals(nameMap.get(ACCOUNT_SYSTEM_ID), "DefaultSystem Account ID");
        Flux<ColumnMetadata> customerContactAttrs = servingStoreProxy.getDecoratedMetadata(mainTestTenant.getId(),
                BusinessEntity.Contact, null, null, StoreFilter.NO_LDC);
        nameMap = customerContactAttrs
                .filter(clm -> StringUtils.isNotEmpty(clm.getAttrName()) && StringUtils.isNotEmpty(clm.getDisplayName()))
                .collectMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName)
                .block();
        Assert.assertNotNull(nameMap);
        Assert.assertTrue(nameMap.containsKey(OTHERSYSTEM_ACCOUNT_SYSTEM_ID));
        Assert.assertEquals(nameMap.get(OTHERSYSTEM_ACCOUNT_SYSTEM_ID), "DefaultSystem_2 Account ID");

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
                .build());
        cms.put(InterfaceName.AccountId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.ContactId.name()) //
                .withCategory(Category.CONTACT_ATTRIBUTES) //
                .withSubcategory("Other") //
                .build());
        return cms;
    }


}
