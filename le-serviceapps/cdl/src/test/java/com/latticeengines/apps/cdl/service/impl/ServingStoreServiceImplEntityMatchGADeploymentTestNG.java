package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import org.testng.Assert;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
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

}
