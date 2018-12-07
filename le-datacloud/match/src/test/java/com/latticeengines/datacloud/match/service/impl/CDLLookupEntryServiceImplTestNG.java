package com.latticeengines.datacloud.match.service.impl;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/*
 * TODO add more test cases
 * TODO retry on test method to guard against eventual consistency failure, retryAnalyzer is not working for some reason
 */
public class CDLLookupEntryServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_SERVING_TABLE = "CDLMatchServingDev_20181126";
    private static final String TEST_STAGING_TABLE = "CDLMatchDev_20181126";
    private static final Tenant TEST_TENANT = getTestTenant();
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final String MAIN_TEST_SEED_ID = "123";
    private static final String OTHER_TEST_SEED_ID = "456";

    // test lookup entries
    private static final CDLLookupEntry TEST_ENTRY_1 = CDLLookupEntryConverter
            .fromDomainCountry(TEST_ENTITY, "google.com", "USA");
    private static final CDLLookupEntry TEST_ENTRY_2 = CDLLookupEntryConverter
            .fromExternalSystem(TEST_ENTITY, "SFDC", "sfdc_5566");
    private static final CDLLookupEntry TEST_ENTRY_3 = CDLLookupEntryConverter
            .fromExternalSystem(TEST_ENTITY, "MARKETO", "mkt_1234");
    private static final CDLLookupEntry TEST_ENTRY_4 = CDLLookupEntryConverter
            .fromDuns(TEST_ENTITY, "999999999");
    private static final List<CDLLookupEntry> TEST_ENTRIES = Arrays.asList(
            TEST_ENTRY_1, TEST_ENTRY_2, TEST_ENTRY_3, TEST_ENTRY_4, TEST_ENTRY_1); // one duplicate at the end

    @Inject
    @InjectMocks
    private CDLLookupEntryServiceImpl cdlLookupEntryService;

    @Mock
    private CDLConfigurationServiceImpl cdlConfigurationService;

    @BeforeClass(groups = "functional")
    private void setupShared() {
        MockitoAnnotations.initMocks(this);
        cdlConfigurationService.setServingTableName(TEST_SERVING_TABLE);
        cdlConfigurationService.setStagingTableName(TEST_STAGING_TABLE);
    }

    @BeforeMethod(groups = "functional")
    @AfterMethod(groups = "functional")
    private void cleanupTestEntries() {
        // cleanup all test entries
        Arrays.stream(CDLMatchEnvironment.values()).forEach(env ->
                TEST_ENTRIES.forEach(entry -> cleanup(env, entry)));
    }

    @Test(groups = "functional", dataProvider = "cdlMatchEnvironment")
    private void testCreateIfNotExists(CDLMatchEnvironment env) {
        // since no current entry, entry is created successfully
        Assert.assertTrue(cdlLookupEntryService.createIfNotExists(env, TEST_TENANT, TEST_ENTRY_1, MAIN_TEST_SEED_ID));
        Assert.assertFalse(cdlLookupEntryService.createIfNotExists(env, TEST_TENANT, TEST_ENTRY_1, MAIN_TEST_SEED_ID));

        // check the seed ID is set correctly
        Assert.assertEquals(cdlLookupEntryService.get(env, TEST_TENANT, TEST_ENTRY_1), MAIN_TEST_SEED_ID);
    }

    @Test(groups = "functional", dataProvider = "cdlMatchEnvironment")
    private void testSetIfEquals(CDLMatchEnvironment env) {
        // since no current entry, entry is created successfully
        Assert.assertTrue(cdlLookupEntryService.setIfEquals(env, TEST_TENANT, TEST_ENTRY_1, MAIN_TEST_SEED_ID));
        // check the seed ID is set correctly
        Assert.assertEquals(cdlLookupEntryService.get(env, TEST_TENANT, TEST_ENTRY_1), MAIN_TEST_SEED_ID);
        // set still succeeded since seed ID in the input is the same as mapped by the existing entry
        Assert.assertTrue(cdlLookupEntryService.setIfEquals(env, TEST_TENANT, TEST_ENTRY_1, MAIN_TEST_SEED_ID));
        // fail to set if the seed ID is different
        Assert.assertFalse(cdlLookupEntryService.setIfEquals(env, TEST_TENANT, TEST_ENTRY_1, OTHER_TEST_SEED_ID));
    }

    @Test(groups = "functional", dataProvider = "cdlMatchEnvironment")
    private void testBatchGet(CDLMatchEnvironment env) {
        // create entries for batch get
        Assert.assertTrue(cdlLookupEntryService.createIfNotExists(env, TEST_TENANT, TEST_ENTRY_1, MAIN_TEST_SEED_ID));
        Assert.assertTrue(cdlLookupEntryService.createIfNotExists(env, TEST_TENANT, TEST_ENTRY_4, OTHER_TEST_SEED_ID));

        List<String> seedIds = cdlLookupEntryService.get(env, TEST_TENANT, TEST_ENTRIES);
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

    @Test(groups = "functional", dataProvider = "cdlMatchEnvironment")
    private void testBatchSet(CDLMatchEnvironment env) throws Exception {
        List<String> seedIds = cdlLookupEntryService.get(env, TEST_TENANT, TEST_ENTRIES);
        Assert.assertNotNull(seedIds);
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        seedIds.forEach(Assert::assertNull);

        List<Pair<CDLLookupEntry, String>> pairs = TEST_ENTRIES
                .stream()
                .map(entry -> Pair.of(entry, MAIN_TEST_SEED_ID))
                .collect(Collectors.toList());
        cdlLookupEntryService.set(env, TEST_TENANT, pairs);

        // wait a bit for eventual consistency
        Thread.sleep(1000);

        seedIds = cdlLookupEntryService.get(env, TEST_TENANT, TEST_ENTRIES);

        Assert.assertNotNull(seedIds);
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        seedIds.forEach(id -> Assert.assertEquals(id, MAIN_TEST_SEED_ID));
    }

    @DataProvider(name = "cdlMatchEnvironment")
    private Object[][] provideCDLMatchEnv() {
        return new Object[][] {
                { CDLMatchEnvironment.STAGING },
                { CDLMatchEnvironment.SERVING },
        };
    }

    private void cleanup(@NotNull CDLMatchEnvironment env, @NotNull CDLLookupEntry entry) {
        cdlLookupEntryService.delete(env, TEST_TENANT, entry);
    }

    private static Tenant getTestTenant() {
        Tenant tenant = new Tenant("lookup_entry_service_test_tenant_1");
        tenant.setPid(429337581L);
        return tenant;
    }
}
