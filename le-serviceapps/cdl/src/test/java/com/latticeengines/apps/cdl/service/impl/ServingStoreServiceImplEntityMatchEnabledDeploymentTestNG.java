package com.latticeengines.apps.cdl.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp
 */
public class ServingStoreServiceImplEntityMatchEnabledDeploymentTestNG extends ServingStoreDeploymentTestNGBase {

    @Test(groups = "deployment-app")
    private void testDecoratedMetadata() {
        testAccountMetadata();
        testContactMetadata();
    }

    // AttributeName -> ColumnMetadata (Only involve columns to verify, not
    // complete)
    @Override
    protected Map<String, ColumnMetadata> getAccountMetadataToVerify() {
        Map<String, ColumnMetadata> cms = new HashMap<>();
        cms.put(InterfaceName.CustomerAccountId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.CustomerAccountId.name()) //
                .withCategory(Category.ACCOUNT_ATTRIBUTES) //
                .withSubcategory("Account IDs") //
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
        cms.put(InterfaceName.CustomerContactId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.CustomerContactId.name()) //
                .withCategory(Category.CONTACT_ATTRIBUTES) //
                .withSubcategory("Other") //
                .withGroups(ColumnSelection.Predefined.TalkingPoint, ColumnSelection.Predefined.Enrichment,
                        ColumnSelection.Predefined.Segment) //
                .build());
        cms.put(InterfaceName.CustomerAccountId.name(), new ColumnMetadataBuilder() //
                .withAttrName(InterfaceName.CustomerAccountId.name()) //
                .withCategory(Category.CONTACT_ATTRIBUTES) //
                .withSubcategory("Other") //
                .build());
        return cms;
    }

    @Override
    protected void overwriteFeatureFlag() {
        testBed.overwriteFeatureFlag(mainTestTenant, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
    }
}
