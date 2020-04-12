package com.latticeengines.apps.cdl.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp
 */
public class ServingStoreServiceImplEntityMatchGADeploymentTestNG extends ServingStoreServiceImplEntityMatchDeploymentTestNGBase {

    @Override
    protected void overwriteFeatureFlag() {
        testBed.overwriteFeatureFlag(mainTestTenant, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
    }

    @Override
    protected void verifyAccountMetadata(List<ColumnMetadata> cms) {
        ColumnMetadata accountId = cms.stream() //
                .filter(cm -> InterfaceName.AccountId.name().equals(cm.getAttrName())) //
                .findFirst().orElse(null);
        Assert.assertNotNull(accountId);
        Assert.assertFalse(accountId.isEnabledFor(ColumnSelection.Predefined.Enrichment),
                JsonUtils.serialize(accountId));
        Assert.assertNotEquals(accountId.getCanEnrich(), Boolean.TRUE, JsonUtils.serialize(accountId));
    }

    @Override
    protected void verifyContactMetadata(List<ColumnMetadata> cms) {
        ColumnMetadata contactId = cms.stream() //
                .filter(cm -> InterfaceName.ContactId.name().equals(cm.getAttrName())) //
                .findFirst().orElse(null);
        Assert.assertNotNull(contactId);
        Assert.assertFalse(contactId.isEnabledFor(ColumnSelection.Predefined.Enrichment),
                JsonUtils.serialize(contactId));
        Assert.assertNotEquals(contactId.getCanEnrich(), Boolean.TRUE, JsonUtils.serialize(contactId));
    }

    // AttributeName -> ColumnMetadata (Only involve columns to verify, not
    // complete)
    @Override
    protected Map<String, ColumnMetadata> getAccountMetadataToVerify() {
        Map<String, ColumnMetadata> cms = new HashMap<>();
        // PLS-15406 allow CustomerAccountId to be used in segmentation only for
        // EntityMatchGA tenant
        cms.put(InterfaceName.CustomerAccountId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.CustomerAccountId.name()) //
                .withCategory(Category.ACCOUNT_ATTRIBUTES) //
                .withSubcategory(Category.SUB_CAT_ACCOUNT_IDS) //
                .withGroups(ColumnSelection.Predefined.Enrichment, ColumnSelection.Predefined.Segment) //
                .canEnrich(Boolean.TRUE) //
                .canSegment(Boolean.TRUE) //
                .canModel(Boolean.TRUE) //
                .build());
        cms.put(InterfaceName.AccountId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.AccountId.name()) //
                .withCategory(Category.ACCOUNT_ATTRIBUTES) //
                .withSubcategory(Category.SUB_CAT_ACCOUNT_IDS) //
                .canEnrich(Boolean.FALSE) //
                .canSegment(Boolean.FALSE) //
                .canModel(Boolean.FALSE)
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
                .withGroups(ColumnSelection.Predefined.Enrichment, ColumnSelection.Predefined.Segment) //
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
                .canEnrich(Boolean.FALSE) //
                .canSegment(Boolean.FALSE) //
                .canModel(Boolean.FALSE) //
                .build());
        return cms;
    }
}
