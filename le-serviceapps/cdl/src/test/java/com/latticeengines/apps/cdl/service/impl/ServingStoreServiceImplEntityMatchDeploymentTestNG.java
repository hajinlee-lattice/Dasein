package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

import reactor.core.publisher.Flux;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp
 */
public class ServingStoreServiceImplEntityMatchDeploymentTestNG extends ServingStoreServiceImplEntityMatchDeploymentTestNGBase {


    @Override
    protected void overwriteFeatureFlag() {
        testBed.overwriteFeatureFlag(mainTestTenant, LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
    }

    @Override
    protected void verifyAccountMetadata(List<ColumnMetadata> cms) {
        ColumnMetadata accountId = cms.stream() //
                .filter(cm -> InterfaceName.AccountId.name().equals(cm.getAttrName())) //
                .findFirst().orElse(null);
        Assert.assertNotNull(accountId);
        Assert.assertTrue(accountId.isEnabledFor(ColumnSelection.Predefined.Enrichment),
                JsonUtils.serialize(accountId));
        Assert.assertEquals(accountId.getCanEnrich(), Boolean.TRUE, JsonUtils.serialize(accountId));
        ColumnMetadata customerAccountId = cms.stream() //
                .filter(cm -> InterfaceName.CustomerAccountId.name().equals(cm.getAttrName())) //
                .findFirst().orElse(null);
        Assert.assertNotNull(customerAccountId);
        Assert.assertTrue(customerAccountId.isEnabledFor(ColumnSelection.Predefined.Enrichment),
                JsonUtils.serialize(customerAccountId));
        Assert.assertEquals(customerAccountId.getCanEnrich(), Boolean.TRUE, JsonUtils.serialize(customerAccountId));
    }

    @Override
    protected void verifyContactMetadata(List<ColumnMetadata> cms) {
        ColumnMetadata contactId = cms.stream() //
                .filter(cm -> InterfaceName.ContactId.name().equals(cm.getAttrName())) //
                .findFirst().orElse(null);
        Assert.assertNotNull(contactId);
        Assert.assertTrue(contactId.isEnabledFor(ColumnSelection.Predefined.Enrichment),
                JsonUtils.serialize(contactId));
        Assert.assertEquals(contactId.getCanEnrich(), Boolean.TRUE, JsonUtils.serialize(contactId));
        ColumnMetadata customerContactId = cms.stream() //
                .filter(cm -> InterfaceName.CustomerContactId.name().equals(cm.getAttrName())) //
                .findFirst().orElse(null);
        Assert.assertNotNull(customerContactId);
        Assert.assertTrue(customerContactId.isEnabledFor(ColumnSelection.Predefined.Enrichment),
                JsonUtils.serialize(customerContactId));
        Assert.assertEquals(customerContactId.getCanEnrich(), Boolean.TRUE, JsonUtils.serialize(customerContactId));
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
}
