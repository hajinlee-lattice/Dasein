package com.latticeengines.app.exposed.service.impl;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp,objectapi
 */
public class DataLakeServiceImplEntityMatchEnabledDeploymentTestNG extends DataLakeServiceImplDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        mainTestTenant = testBed.getMainTestTenant();
        // Enable EntityMatch feature flag
        testBed.overwriteFeatureFlag(mainTestTenant, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
        MultiTenantContext.setTenant(mainTestTenant);

        setupRedshiftData();
    }

    @Test(groups = "deployment")
    public void testGetAttributes() {
        testAndVerifyGetAttributes();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Set<Pair<String, Category>> getExpectedAttrs() {
        return Collections.EMPTY_SET;
        /* Need to prepare test artifact with entity match enabled, otherwise CustomerAccountId & CustomerContactId do not exist
        return ImmutableSet.of( //
                Pair.of(InterfaceName.CustomerAccountId.name(), Category.ACCOUNT_ATTRIBUTES), //
                Pair.of(InterfaceName.CustomerContactId.name(), Category.CONTACT_ATTRIBUTES)); //
                */
    }

    @Override
    protected Set<Pair<String, Category>> getUnexpectedAttrs() {
        return ImmutableSet.of( //
                Pair.of(InterfaceName.AccountId.name(), Category.ACCOUNT_ATTRIBUTES), //
                Pair.of(InterfaceName.ContactId.name(), Category.CONTACT_ATTRIBUTES), //
                Pair.of(InterfaceName.AccountId.name(), Category.CONTACT_ATTRIBUTES));
    }
}
