package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.newSeed;
import static com.latticeengines.domain.exposed.datacloud.match.entity.AccountSeed.ENTITY;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDomainCountry;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDuns;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromExternalSystem;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromNameCountry;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;

import java.util.Collections;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.exposed.service.AccountEntityService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.entity.AccountSeed;
import com.latticeengines.domain.exposed.datacloud.match.entity.AccountSeedBuilder;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * {@link AccountEntityService} is only a wrapper that returns a specific entity, {@link AccountSeed} in this case.
 * Therefore, only need to do basic tests to make sure {@link EntityRawSeed} is transformed to {@link AccountSeed}
 * correctly. Underlying services already have tests covering other aspects.
 */
public class AccountEntityServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Tenant TEST_GET_TENANT = new Tenant("entity_match_get_account_seed_test_tenant");

    @Inject
    private AccountEntityService accountEntityService;

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    @Test(groups = "functional", dataProvider = "getAccountSeed")
    private void testGetSeed(@NotNull EntityRawSeed currentSeed, @NotNull AccountSeed expectedSeed) throws Exception {
        Tenant tenant = TEST_GET_TENANT;

        // clear and check that we get null seed
        clear(tenant, currentSeed);
        Assert.assertNull(accountEntityService.get(tenant, currentSeed.getId()));

        // set to serving and check we get the correct seed
        entityRawSeedService.setIfNotExists(SERVING, tenant, currentSeed, true,
                entityMatchVersionService.getCurrentVersion(SERVING, tenant));
        Thread.sleep(2000L);

        AccountSeed seed = accountEntityService.get(tenant, currentSeed.getId());
        Assert.assertEquals(seed, expectedSeed);

        // cleanup
        clear(tenant, currentSeed);
    }

    private void clear(@NotNull Tenant tenant, @NotNull EntityRawSeed seed) {
        for (EntityMatchEnvironment env : EntityMatchEnvironment.values()) {
            entityRawSeedService.delete(env, tenant, seed.getEntity(), seed.getId(),
                    entityMatchVersionService.getCurrentVersion(env, tenant));
        }
    }

    @DataProvider(name = "getAccountSeed")
    private Object[][] provideGetSeedTestData() {
        return new Object[][] {
                {
                        newSeed("dc", fromDomainCountry(ENTITY, "google.com", "USA")),
                        new AccountSeedBuilder()
                                .withId("dc")
                                .addDomainCountryPair("google.com", "USA")
                                .build()
                },
                {
                        newSeed("nc", fromNameCountry(ENTITY, "Google", "USA")),
                        new AccountSeedBuilder()
                                .withId("nc")
                                .addNameCountryPair("Google", "USA")
                                .build()
                },
                {
                        newSeed("duns", fromDuns(ENTITY, "123456789")),
                        new AccountSeedBuilder()
                                .withId("duns")
                                .withDuns("123456789")
                                .build()
                },
                {
                        newSeed("systemId", fromExternalSystem(ENTITY, "SFDC", "sfdc_1")),
                        new AccountSeedBuilder()
                                .withId("systemId")
                                .withExternalSystemIdMap(Collections.singletonMap("SFDC", "sfdc_1"))
                                .build()
                },
                {
                        newSeed("mixLookup",
                                fromDomainCountry(ENTITY, "google.com", "USA"),
                                fromNameCountry(ENTITY, "Google", "USA"),
                                fromDuns(ENTITY, "123456789"),
                                fromExternalSystem(ENTITY, "SFDC", "sfdc_1")),
                        new AccountSeedBuilder()
                                .withId("mixLookup")
                                .addDomainCountryPair("google.com", "USA")
                                .addNameCountryPair("Google", "USA")
                                .withDuns("123456789")
                                .withExternalSystemIdMap(Collections.singletonMap("SFDC", "sfdc_1"))
                                .build()
                },
                {
                        // test extra attributes
                        newSeed("latticeAccountId",
                                DataCloudConstants.LATTICE_ACCOUNT_ID, "fake_lattice_account_id"),
                        new AccountSeedBuilder()
                                .withId("latticeAccountId")
                                .withLatticeAccountId("fake_lattice_account_id")
                                .build()
                },
        };
    }
}
