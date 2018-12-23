package com.latticeengines.datacloud.match.service.impl;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class EntityLookupEntryServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_SERVING_TABLE = "CDLMatchServingDev_20181126";
    private static final String TEST_STAGING_TABLE = "CDLMatchDev_20181126";
    private static final Tenant TEST_TENANT = new Tenant("lookup_entry_service_test_tenant_1");
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final String MAIN_TEST_SEED_ID = "123";
    private static final String OTHER_TEST_SEED_ID = "456";

    // test lookup entries
    private static final EntityLookupEntry TEST_ENTRY_1 = EntityLookupEntryConverter
            .fromDomainCountry(TEST_ENTITY, "google.com", "USA");
    private static final EntityLookupEntry TEST_ENTRY_2 = EntityLookupEntryConverter
            .fromExternalSystem(TEST_ENTITY, "SFDC", "sfdc_5566");
    private static final EntityLookupEntry TEST_ENTRY_3 = EntityLookupEntryConverter
            .fromExternalSystem(TEST_ENTITY, "MARKETO", "mkt_1234");
    private static final EntityLookupEntry TEST_ENTRY_4 = EntityLookupEntryConverter
            .fromDuns(TEST_ENTITY, "999999999");
    private static final List<EntityLookupEntry> TEST_ENTRIES = Arrays.asList(
            TEST_ENTRY_1, TEST_ENTRY_2, TEST_ENTRY_3, TEST_ENTRY_4, TEST_ENTRY_1); // one duplicate at the end

    @Inject
    @InjectMocks
    private EntityLookupEntryServiceImpl entityLookupEntryService;

    @Mock
    private EntityMatchConfigurationServiceImpl entityMatchConfigurationService;

    @BeforeClass(groups = "functional")
    private void setupShared() {
        MockitoAnnotations.initMocks(this);
        entityMatchConfigurationService.setServingTableName(TEST_SERVING_TABLE);
        entityMatchConfigurationService.setStagingTableName(TEST_STAGING_TABLE);
        Mockito.when(entityMatchConfigurationService.getRetryTemplate(Mockito.any())).thenCallRealMethod();
    }

    @BeforeMethod(groups = "functional")
    @AfterMethod(groups = "functional")
    private void cleanupTestEntries() {
        // cleanup all test entries
        Arrays.stream(EntityMatchEnvironment.values()).forEach(env ->
                TEST_ENTRIES.forEach(entry -> cleanup(env, entry)));
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment")
    private void testCreateIfNotExists(EntityMatchEnvironment env) {
        // since no current entry, entry is created successfully
        Assert.assertTrue(entityLookupEntryService.createIfNotExists(env, TEST_TENANT, TEST_ENTRY_1, MAIN_TEST_SEED_ID));
        Assert.assertFalse(entityLookupEntryService.createIfNotExists(env, TEST_TENANT, TEST_ENTRY_1, MAIN_TEST_SEED_ID));

        // check the seed ID is set correctly
        Assert.assertEquals(entityLookupEntryService.get(env, TEST_TENANT, TEST_ENTRY_1), MAIN_TEST_SEED_ID);
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment")
    private void testSetIfEquals(EntityMatchEnvironment env) {
        // since no current entry, entry is created successfully
        Assert.assertTrue(entityLookupEntryService.setIfEquals(env, TEST_TENANT, TEST_ENTRY_1, MAIN_TEST_SEED_ID));
        // check the seed ID is set correctly
        Assert.assertEquals(entityLookupEntryService.get(env, TEST_TENANT, TEST_ENTRY_1), MAIN_TEST_SEED_ID);
        // set still succeeded since seed ID in the input is the same as mapped by the existing entry
        Assert.assertTrue(entityLookupEntryService.setIfEquals(env, TEST_TENANT, TEST_ENTRY_1, MAIN_TEST_SEED_ID));
        // fail to set if the seed ID is different
        Assert.assertFalse(entityLookupEntryService.setIfEquals(env, TEST_TENANT, TEST_ENTRY_1, OTHER_TEST_SEED_ID));
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment")
    private void testBatchGet(EntityMatchEnvironment env) throws Exception {
        // create entries for batch get
        Assert.assertTrue(entityLookupEntryService.createIfNotExists(env, TEST_TENANT, TEST_ENTRY_1, MAIN_TEST_SEED_ID));
        Assert.assertTrue(entityLookupEntryService.createIfNotExists(env, TEST_TENANT, TEST_ENTRY_4, OTHER_TEST_SEED_ID));
        Thread.sleep(2000L);

        List<String> seedIds = entityLookupEntryService.get(env, TEST_TENANT, TEST_ENTRIES);
        Assert.assertNotNull(seedIds);
        // result list size should equals input list size
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        // entry 1 & 4 exists, 2 & 3 does not exist
        Assert.assertEquals(seedIds.get(0), MAIN_TEST_SEED_ID);
        Assert.assertEquals(seedIds.get(4), MAIN_TEST_SEED_ID); // duplicate of entry 1
        Assert.assertEquals(seedIds.get(3), OTHER_TEST_SEED_ID);
        Assert.assertNull(seedIds.get(1));
        Assert.assertNull(seedIds.get(2));
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment")
    private void testBatchSet(EntityMatchEnvironment env) throws Exception {
        List<String> seedIds = entityLookupEntryService.get(env, TEST_TENANT, TEST_ENTRIES);
        Assert.assertNotNull(seedIds);
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        seedIds.forEach(Assert::assertNull);

        List<Pair<EntityLookupEntry, String>> pairs = TEST_ENTRIES
                .stream()
                .map(entry -> Pair.of(entry, MAIN_TEST_SEED_ID))
                .collect(Collectors.toList());
        entityLookupEntryService.set(env, TEST_TENANT, pairs);

        // wait a bit for eventual consistency
        Thread.sleep(3000);

        seedIds = entityLookupEntryService.get(env, TEST_TENANT, TEST_ENTRIES);

        Assert.assertNotNull(seedIds);
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        seedIds.forEach(id -> Assert.assertEquals(id, MAIN_TEST_SEED_ID));
    }

    @DataProvider(name = "entityMatchEnvironment")
    private Object[][] entityMatchEnvironment() {
        return new Object[][] {
                { EntityMatchEnvironment.STAGING },
                { EntityMatchEnvironment.SERVING },
        };
    }

    private void cleanup(@NotNull EntityMatchEnvironment env, @NotNull EntityLookupEntry entry) {
        entityLookupEntryService.delete(env, TEST_TENANT, entry);
    }
}
