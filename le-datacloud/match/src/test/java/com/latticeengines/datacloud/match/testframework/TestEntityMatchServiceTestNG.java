package com.latticeengines.datacloud.match.testframework;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDomainCountry;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDuns;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromExternalSystem;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromSystemId;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class TestEntityMatchServiceTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_TENANT_ID = "test_entity_match_service_test_tenant";
    private static final Tenant TEST_TENANT = new Tenant(TEST_TENANT_ID);
    private static final String TEST_ENTITY = BusinessEntity.Account.name();

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    @Inject
    private TestEntityMatchService testEntityMatchService;

    @Test(groups = "functional")
    private void testPrepareLookupTestData() {
        Arrays.stream(EntityMatchEnvironment.values()).forEach(this::testPrepareLookupTestDataInEnv);
    }

    @Test(groups = "functional")
    private void testPrepareSeedTestData() {
        Arrays.stream(EntityMatchEnvironment.values()).forEach(this::testPrepareSeedTestDataInEnv);
    }

    /*
     * basic tests to make sure test data is indeed generated
     */

    private void testPrepareLookupTestDataInEnv(@NotNull EntityMatchEnvironment env) {
        try {
            testEntityMatchService.bumpVersion(TEST_TENANT.getId(), env);
            Object[] header = new Object[] { MatchKey.Domain, MatchKey.Country, "SFDC", InterfaceName.EntityId };
            Object[][] data = new Object[][] {
                    { "google.com", "USA", "sfdc_1", "adhfhhfhf" },
                    { null, null, "s1", "abbbcc" },
            };

            testEntityMatchService.prepareLookupTestData(
                    TEST_TENANT, env, TEST_ENTITY, header, data, true);

            Thread.sleep(2000L);

            // make sure the correct test data is populated
            List<EntityLookupEntry> entries = Arrays.asList(
                    fromDomainCountry(TEST_ENTITY, "google.com", "USA"),
                    fromSystemId(TEST_ENTITY, "SFDC", "sfdc_1"),
                    fromSystemId(TEST_ENTITY, "SFDC", "s1"));
            List<String> expectedEntityIds = Arrays.asList("adhfhhfhf", "adhfhhfhf", "abbbcc");
            List<String> entityIds = entityLookupEntryService.get(env, TEST_TENANT, entries,
                    entityMatchVersionService.getCurrentVersion(env, TEST_TENANT));
            Assert.assertEquals(entityIds, expectedEntityIds);
        } catch (Exception e) {
            Assert.fail("Failed to prepare lookup test data in env = " + env, e);
        }
    }

    private void testPrepareSeedTestDataInEnv(@NotNull EntityMatchEnvironment env) {
        try {
            testEntityMatchService.bumpVersion(TEST_TENANT.getId(), env);

            Object[] header = new Object[] {
                    MatchKey.Domain, MatchKey.Country, "SFDC", "MKTO", InterfaceName.EntityId, MatchKey.DUNS };
            Object[][] data = new Object[][] {
                    { "fb.com", "USA", null, "mkto_1", "sdlkjfl", null },
                    { "fb.com", "USA", "sfdc_1", null, "aabbabc", "999999999" },
                    // only way to add multiple domain/country to one seed for now..
                    { "google.com", "USA", null, null, "aabbabc", null },
            };

            testEntityMatchService.prepareSeedTestData(TEST_TENANT, env, TEST_ENTITY, header, data, true);

            Thread.sleep(2000L);

            // make sure the correct test data is populated
            EntityRawSeed seed1 = entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, "sdlkjfl",
                    entityMatchVersionService.getCurrentVersion(env, TEST_TENANT));
            EntityRawSeed seed2 = entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, "aabbabc",
                    entityMatchVersionService.getCurrentVersion(env, TEST_TENANT));
            List<EntityLookupEntry> entries1 = Arrays.asList(
                    fromDomainCountry(TEST_ENTITY, "fb.com", "USA"),
                    fromExternalSystem(TEST_ENTITY, "MKTO", "mkto_1"));
            List<EntityLookupEntry> entries2 = Arrays.asList(
                    fromDomainCountry(TEST_ENTITY, "fb.com", "USA"),
                    fromDomainCountry(TEST_ENTITY, "google.com", "USA"),
                    fromDuns(TEST_ENTITY, "999999999"),
                    fromSystemId(TEST_ENTITY, "SFDC", "sfdc_1"));
            Assert.assertTrue(TestEntityMatchUtils.equalsDisregardPriority(
                    seed1, new EntityRawSeed("sdlkjfl", TEST_ENTITY, entries1, null)));
            Assert.assertTrue(TestEntityMatchUtils.equalsDisregardPriority(
                    seed2, new EntityRawSeed("aabbabc", TEST_ENTITY, entries2, null)));
        } catch (Exception e) {
            Assert.fail("Failed to prepare lookup test data in env = " + env, e);
        }
    }
}
