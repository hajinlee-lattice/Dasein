package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.match.util.EntityMatchUtils.prettyToString;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.ACCT_EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.ACCT_NAME_PHONE;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.C_ACCT_EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.C_ACCT_NAME_PHONE;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.DOMAIN_COUNTRY;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.DUNS;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.EXTERNAL_SYSTEM;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.NAME_COUNTRY;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.NAME_PHONE;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class EntityLookupEntryServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

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
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Value("${datacloud.match.entity.staging.table}")
    private String stagingTableName;
    @Value("${datacloud.match.entity.serving.table}")
    private String servingTableName;

    @BeforeClass(groups = "functional")
    private void setupShared() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityMatchConfigurationService.getTableName(EntityMatchEnvironment.STAGING))
                .thenReturn(stagingTableName);
        Mockito.when(entityMatchConfigurationService.getTableName(EntityMatchEnvironment.SERVING))
                .thenReturn(servingTableName);
        Mockito.when(entityMatchConfigurationService.getRetryTemplate(Mockito.any()))
                .thenReturn(RetryUtils.getRetryTemplate(3));
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testCreateIfNotExists(EntityMatchEnvironment env) {
        Tenant tenant = newTestTenant();
        // since no current entry, entry is created successfully
        Assert.assertTrue(entityLookupEntryService
                .createIfNotExists(env, tenant, TEST_ENTRY_1, MAIN_TEST_SEED_ID, true));
        Assert.assertFalse(entityLookupEntryService
                .createIfNotExists(env, tenant, TEST_ENTRY_1, MAIN_TEST_SEED_ID, true));

        // check the seed ID is set correctly
        Assert.assertEquals(entityLookupEntryService.get(env, tenant, TEST_ENTRY_1), MAIN_TEST_SEED_ID);
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testSetIfEquals(EntityMatchEnvironment env) {
        Tenant tenant = newTestTenant();
        // since no current entry, entry is created successfully
        Assert.assertTrue(entityLookupEntryService
                .setIfEquals(env, tenant, TEST_ENTRY_1, MAIN_TEST_SEED_ID, true));
        // check the seed ID is set correctly
        Assert.assertEquals(entityLookupEntryService.get(env, tenant, TEST_ENTRY_1), MAIN_TEST_SEED_ID);
        // set still succeeded since seed ID in the input is the same as mapped by the existing entry
        Assert.assertTrue(entityLookupEntryService
                .setIfEquals(env, tenant, TEST_ENTRY_1, MAIN_TEST_SEED_ID, true));
        // fail to set if the seed ID is different
        Assert.assertFalse(entityLookupEntryService
                .setIfEquals(env, tenant, TEST_ENTRY_1, OTHER_TEST_SEED_ID, true));
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testBatchGet(EntityMatchEnvironment env) {
        Tenant tenant = newTestTenant();
        // create entries for batch get
        Assert.assertTrue(entityLookupEntryService
                .createIfNotExists(env, tenant, TEST_ENTRY_1, MAIN_TEST_SEED_ID, true));
        Assert.assertTrue(entityLookupEntryService
                .createIfNotExists(env, tenant, TEST_ENTRY_4, OTHER_TEST_SEED_ID, true));

        List<String> seedIds = entityLookupEntryService.get(env, tenant, TEST_ENTRIES);
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

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testBatchSet(EntityMatchEnvironment env) throws Exception {
        Tenant tenant = newTestTenant();
        List<String> seedIds = entityLookupEntryService.get(env, tenant, TEST_ENTRIES);
        Assert.assertNotNull(seedIds);
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        seedIds.forEach(Assert::assertNull);

        List<Pair<EntityLookupEntry, String>> pairs = TEST_ENTRIES
                .stream()
                .map(entry -> Pair.of(entry, MAIN_TEST_SEED_ID))
                .collect(Collectors.toList());
        entityLookupEntryService.set(env, tenant, pairs, true);

        // wait a bit for eventual consistency
        Thread.sleep(500);

        seedIds = entityLookupEntryService.get(env, tenant, TEST_ENTRIES);

        Assert.assertNotNull(seedIds);
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        seedIds.forEach(id -> Assert.assertEquals(id, MAIN_TEST_SEED_ID));
    }

    /*
     * Test serializing/deserializing EntityLookupEntry.
     */
    @Test(groups = "functional", dataProvider = "serdeTestData", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testSerde(EntityLookupEntry.Type type, String[] keys, String[] values) {
        String entity = BusinessEntity.Account.name();
        EntityLookupEntry entry = new EntityLookupEntry(type, entity, keys, values);
        Tenant tenant = newTestTenant();

        for (EntityMatchEnvironment env : EntityMatchEnvironment.values()) {
            entityLookupEntryService.delete(env, tenant, entry);

            String id = entityLookupEntryService.get(env, tenant, entry);
            Assert.assertNull(id, String.format("EntityId should be null before inserting %s", prettyToString(entry)));

            entityLookupEntryService.set(env, tenant, ImmutableList.of(Pair.of(entry, MAIN_TEST_SEED_ID)), true);

            id = entityLookupEntryService.get(env, tenant, entry);
            Assert.assertEquals(id, MAIN_TEST_SEED_ID,
                    String.format("%s map to the wrong entityId", prettyToString(entry)));
        }
    }

    @DataProvider(name = "serdeTestData", parallel = true)
    private Object[][] provideSerdeTestData() {
        return new Object[][] { //
                { NAME_COUNTRY, new String[0], new String[] { "Google", "United States" } }, //
                { NAME_COUNTRY, new String[0], new String[] { "Google\"R\"", "United States" } }, //
                { DOMAIN_COUNTRY, new String[0], new String[] { "https://www.google.com", "United States" } }, //
                { DUNS, new String[0], new String[] { "999876543" } }, //
                { EXTERNAL_SYSTEM, new String[] { InterfaceName.CustomerAccountId.name() },
                        new String[] { "A123456789" } }, //
                { ACCT_EMAIL, new String[0], new String[] { "A123", "abc@gmail.com" } }, //
                { ACCT_NAME_PHONE, new String[0], new String[] { "A123", "Jason Bull", "(600)-123-4567" } }, //
                { C_ACCT_EMAIL, new String[0], new String[] { "CA123", "abc@gmail.com" } }, //
                { C_ACCT_NAME_PHONE, new String[0], new String[] { "CA124", "John Reese", "(999)-999-9999" } }, //
                { EMAIL, new String[0], new String[] { "fff@fb.com" } }, //
                { NAME_PHONE, new String[0], new String[] { "Jason Gideon", "(999)-876-5432" } }, //
        };
    }

    @DataProvider(name = "entityMatchEnvironment", parallel = true)
    private Object[][] entityMatchEnvironment() {
        return new Object[][] {
                { EntityMatchEnvironment.STAGING },
                { EntityMatchEnvironment.SERVING },
        };
    }

    private Tenant newTestTenant() {
        return new Tenant(
                EntityLookupEntryServiceImplTestNG.class.getSimpleName() + "_" + UUID.randomUUID().toString());
    }
}
