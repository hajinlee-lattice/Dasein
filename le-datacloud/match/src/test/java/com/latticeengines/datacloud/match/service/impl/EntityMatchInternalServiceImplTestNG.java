package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.equalsDisregardPriority;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_NETFLIX_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_2;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDomainCountry;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDuns;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromExternalSystem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class EntityMatchInternalServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final long WAIT_INTERVAL = 500L;
    private static final int MAX_WAIT_TIMES = 60; // 30s
    private static final long MAX_LOOKUP_CACHE_LIMIT = 2;
    // set to 6 because weight is used here so it is hard to determine which seed will get evicted
    // only seed1 & seed2 has weight 3, seed3 has weight 2, so only two can be in cache
    private static final long MAX_SEED_CACHE_LIMIT = 6;

    private static final String TEST_SERVING_TABLE = "CDLMatchServingDev_20181126";
    private static final String TEST_STAGING_TABLE = "CDLMatchDev_20181126";
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final Tenant TEST_TENANT = new Tenant("entity_match_internal_service_test_tenant_1");
    private static final String EXT_SYSTEM_SFDC = "SFDC";
    private static final String EXT_SYSTEM_MARKETO = "MARKETO";
    private static final String TEST_COUNTRY = "USA";

    // test lookup entries
    private static final EntityLookupEntry TEST_ENTRY_1 = fromDomainCountry(
            TEST_ENTITY, "google.com", "USA");
    private static final EntityLookupEntry TEST_ENTRY_2 = fromExternalSystem(
            TEST_ENTITY, EXT_SYSTEM_SFDC, "sfdc_5566");
    private static final EntityLookupEntry TEST_ENTRY_3 = fromExternalSystem(
            TEST_ENTITY, EXT_SYSTEM_MARKETO, "mkt_1234");
    private static final EntityLookupEntry TEST_ENTRY_4 = fromDuns(TEST_ENTITY, "999999999");
    private static final List<EntityLookupEntry> TEST_ENTRIES = Arrays
            .asList(TEST_ENTRY_1, TEST_ENTRY_2, TEST_ENTRY_3, TEST_ENTRY_4);
    private static final String SEED_ID_FOR_LOOKUP = "seed_for_lookup_entry";

    private static final String SEED_ID_1 = "seed_1";
    private static final String SEED_ID_2 = "seed_2";
    private static final String SEED_ID_3 = "seed_3";
    private static final String SEED_ID_4 = "seed_4"; // not exists
    private static final String SEED_ID_5 = "seed_5"; // not exists
    private static final String ASSOCIATION_SEED_ID = "entity_association_seed_id";
    private static final String ANOTHER_SEED_ID = "another_seed_id";
    private static final List<String> SEED_IDS = Arrays.asList(SEED_ID_1, SEED_ID_2, SEED_ID_3, SEED_ID_4, SEED_ID_5);
    private static final EntityRawSeed TEST_SEED_1 = newSeed(SEED_ID_1, "sfdc_1", "google.com");
    private static final EntityRawSeed TEST_SEED_2 = newSeed(SEED_ID_2, null, "fb.com", "abc.com");
    private static final EntityRawSeed TEST_SEED_3 = newSeed(SEED_ID_3, "sfdc_2");

    @Inject
    @InjectMocks
    @Spy
    private EntityMatchInternalServiceImpl entityMatchInternalService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityMatchConfigurationServiceImpl entityMatchConfigurationService;

    @BeforeClass(groups = "functional")
    private void setupShared() {
        MockitoAnnotations.initMocks(this);
        entityMatchConfigurationService.setServingTableName(TEST_SERVING_TABLE);
        entityMatchConfigurationService.setStagingTableName(TEST_STAGING_TABLE);
        // mock cache limit
        Mockito.when(entityMatchInternalService.getMaxLookupCacheSize()).thenReturn(MAX_LOOKUP_CACHE_LIMIT);
        Mockito.when(entityMatchInternalService.getMaxSeedCacheWeight()).thenReturn(MAX_SEED_CACHE_LIMIT);

        cleanup(TEST_TENANT, TEST_ENTRIES.toArray(new EntityLookupEntry[0]));
    }

    @AfterClass(groups = "functional")
    private void tearDownShared() throws Exception {
        entityMatchInternalService.shutdownAndAwaitTermination();
        // test entries are sync asynchronously, cleanup here just in case
        cleanup(TEST_TENANT, TEST_ENTRIES.toArray(new EntityLookupEntry[0]));
    }

    @BeforeMethod(groups = "functional")
    private void setup() {
        // cleanup cache entries
        if (entityMatchInternalService.getLookupCache() != null) {
            entityMatchInternalService.getLookupCache().invalidateAll();
        }
        if (entityMatchInternalService.getSeedCache() != null) {
            entityMatchInternalService.getSeedCache().invalidateAll();
        }
    }

    @Test(groups = "functional")
    private void testLookupSeedBothMode() throws Exception {
        // test both mode
        testLookupSeed(true);
        testLookupSeed(false);
    }

    private void testLookupSeed(boolean isAllocateMode) throws Exception {
        entityMatchConfigurationService.setIsAllocateMode(isAllocateMode);
        cleanup(TEST_TENANT, TEST_ENTRIES.toArray(new EntityLookupEntry[0]));

        setupServing(TEST_ENTRY_1, TEST_ENTRY_3);
        Thread.sleep(2000L);

        List<String> seedIds = entityMatchInternalService.getIds(TEST_TENANT, TEST_ENTRIES);
        Assert.assertNotNull(seedIds);
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        // only entry 1 & 3 are set to serving
        Assert.assertEquals(seedIds, Arrays.asList(SEED_ID_FOR_LOOKUP, null, SEED_ID_FOR_LOOKUP, null));

        // check in-memory cache
        Cache<Pair<String, EntityLookupEntry>, String> lookupCache = entityMatchInternalService.getLookupCache();
        Assert.assertNotNull(lookupCache);
        Assert.assertEquals(lookupCache.getIfPresent(Pair.of(TEST_TENANT.getId(), TEST_ENTRY_1)), SEED_ID_FOR_LOOKUP);
        Assert.assertEquals(lookupCache.getIfPresent(Pair.of(TEST_TENANT.getId(), TEST_ENTRY_3)), SEED_ID_FOR_LOOKUP);
        Assert.assertNull(lookupCache.getIfPresent(TEST_ENTRY_2));
        Assert.assertNull(lookupCache.getIfPresent(TEST_ENTRY_4));
        // clean cache
        lookupCache.invalidateAll();

        // check staging (async update)
        waitForAllLookupEntriesPopulated();
        List<String> seedIdsInStaging = entityLookupEntryService.get(
                EntityMatchEnvironment.STAGING, TEST_TENANT, TEST_ENTRIES);
        if (isAllocateMode) {
            // only entry 1 & 3 are set to staging
            Assert.assertEquals(seedIdsInStaging, Arrays.asList(SEED_ID_FOR_LOOKUP, null, SEED_ID_FOR_LOOKUP, null));
        } else {
            Assert.assertNotNull(seedIdsInStaging);
            Assert.assertEquals(seedIdsInStaging.size(), TEST_ENTRIES.size());
            seedIdsInStaging.forEach(Assert::assertNull);
        }
    }

    @Test(groups = "functional")
    private void testSeedAllocateMode() throws Exception {
        entityMatchConfigurationService.setIsAllocateMode(true);
        cleanup(TEST_TENANT, SEED_IDS);
        setupServing(TEST_SEED_1, TEST_SEED_2, TEST_SEED_3);
        Thread.sleep(2000L);

        List<EntityRawSeed> results = entityMatchInternalService.get(TEST_TENANT, TEST_ENTITY, SEED_IDS);
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), SEED_IDS.size());
        // only test seed 1, 2 & 3 exists
        verifyEntityRawSeeds(results, SEED_ID_1, SEED_ID_2, SEED_ID_3, null, null);

        // check in-memory cache (should not use cache in bulk mode for seed)
        Pair<String, String> prefix = Pair.of(TEST_TENANT.getId(), TEST_ENTITY);
        Cache<Pair<Pair<String, String>, String>, EntityRawSeed> seedCache = entityMatchInternalService
                .getSeedCache();
        Assert.assertNotNull(seedCache);
        SEED_IDS.stream().map(id -> seedCache.getIfPresent(Pair.of(prefix, id))).forEach(Assert::assertNull);
        // clean cache
        seedCache.invalidateAll();

        // check staging
        List<EntityRawSeed> resultsInStaging = entityRawSeedService
                .get(EntityMatchEnvironment.STAGING, TEST_TENANT, TEST_ENTITY, SEED_IDS);
        verifyEntityRawSeeds(resultsInStaging, SEED_ID_1, SEED_ID_2, SEED_ID_3, null, null);

        cleanup(TEST_TENANT, SEED_IDS);
    }

    @Test(groups = "functional")
    private void testSeedLookupMode() throws Exception {
        entityMatchConfigurationService.setIsAllocateMode(false);
        cleanup(TEST_TENANT, SEED_IDS);
        setupServing(TEST_SEED_1, TEST_SEED_2);
        Thread.sleep(2000L);

        List<EntityRawSeed> results = entityMatchInternalService.get(TEST_TENANT, TEST_ENTITY, SEED_IDS);
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), SEED_IDS.size());
        // only test seed 1, 2 & 3 exists
        verifyEntityRawSeeds(results, SEED_ID_1, SEED_ID_2, null, null, null);

        // check in-memory cache
        Pair<String, String> prefix = Pair.of(TEST_TENANT.getId(), TEST_ENTITY);
        Cache<Pair<Pair<String, String>, String>, EntityRawSeed> seedCache = entityMatchInternalService
                .getSeedCache();
        Assert.assertNotNull(seedCache);
        Stream.of(SEED_ID_1, SEED_ID_2)
                .map(id -> seedCache.getIfPresent(Pair.of(prefix, id)))
                .forEach(Assert::assertNotNull);
        // clean cache
        seedCache.invalidateAll();

        // check staging (should not use staging in real time mode)
        List<EntityRawSeed> resultsInStaging = entityRawSeedService
                .get(EntityMatchEnvironment.STAGING, TEST_TENANT, TEST_ENTITY, SEED_IDS);
        Assert.assertNotNull(resultsInStaging);
        Assert.assertEquals(resultsInStaging.size(), SEED_IDS.size());
        // nothing in staging
        resultsInStaging.forEach(Assert::assertNull);

        cleanup(TEST_TENANT, SEED_IDS);
    }

    @Test(groups = "functional")
    private void testNullLookupEntries() throws Exception {
        entityMatchConfigurationService.setIsAllocateMode(false);
        EntityLookupEntry[] lookupEntries = new EntityLookupEntry[] { DC_FACEBOOK_1, DUNS_1, SFDC_1, SFDC_2 };
        List<String> expectedSeedIds = Arrays.asList(null, SEED_ID_FOR_LOOKUP, null, SEED_ID_FOR_LOOKUP);

        cleanup(TEST_TENANT, lookupEntries);
        setupServing(DUNS_1, SFDC_2);
        Thread.sleep(2000L);

        List<String> seedIds = entityMatchInternalService.getIds(TEST_TENANT, Arrays.asList(lookupEntries));
        Assert.assertEquals(seedIds, expectedSeedIds);

        cleanup(TEST_TENANT, lookupEntries);
    }

    @Test(groups = "functional")
    private void testNullSeedIds() throws Exception {
        entityMatchConfigurationService.setIsAllocateMode(false);
        String seedId1 = "testNullSeedIds1";
        String seedId2 = "testNullSeedIds2";

        cleanup(TEST_TENANT, Arrays.asList(seedId1, seedId2));
        setupServing(
                newSeed(seedId1, "s1", "google.com", "facebook.com"),
                newSeed(seedId2, "s2", "netflix.com"));
        Thread.sleep(2000L);

        List<String> expectedSeedIds = Arrays.asList(null, null, seedId1, null, seedId2);
        List<EntityRawSeed> seeds = entityMatchInternalService.get(TEST_TENANT, TEST_ENTITY, expectedSeedIds);
        Assert.assertNotNull(seeds);
        List<String> seedIds = seeds.stream().map(seed -> seed == null ? null : seed.getId())
                .collect(Collectors.toList());
        Assert.assertEquals(seedIds, expectedSeedIds);

        cleanup(TEST_TENANT, Arrays.asList(seedId1, seedId2));
    }

    /*
     * allocate ID in parallel
     */
    @Test(groups = "functional", dataProvider = "entityIdAllocation")
    private void testIDAllocation(int nAllocations, int nThreads) {
        entityMatchConfigurationService.setIsAllocateMode(true);
        ExecutorService service = Executors.newFixedThreadPool(nThreads);
        List<Future<String>> futures = IntStream
                .range(0, nAllocations)
                .mapToObj(idx -> service.submit(() ->
                        entityMatchInternalService.allocateId(TEST_TENANT, TEST_ENTITY)))
                .collect(Collectors.toList());
        List<String> entityIds = futures.stream().map(future -> {
            try {
                return future.get();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
                return null;
            }
        }).collect(Collectors.toList());
        Assert.assertNotNull(entityIds);
        Assert.assertEquals(entityIds.size(), nAllocations);
        entityIds.forEach(Assert::assertNotNull);

        // cleanup
        entityIds.forEach(id -> entityRawSeedService
                .delete(EntityMatchEnvironment.SERVING, TEST_TENANT, TEST_ENTITY, id));
    }

    @Test(groups = "functional")
    private void testAssociation() throws Exception {
        entityMatchConfigurationService.setIsAllocateMode(true); // association only works with allocate mode
        String seedId = "testAssociation"; // prevent conflict
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING;
        cleanupSeedAndLookup(env, seedId);

        boolean created = entityRawSeedService
                .createIfNotExists(env, TEST_TENANT, TEST_ENTITY, seedId, true);
        Assert.assertTrue(created);

        EntityRawSeed seedToUpdate1 = newSeed(seedId, "sfdc_1", "google.com");
        Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result1 = entityMatchInternalService
                .associate(TEST_TENANT, seedToUpdate1, false);
        Assert.assertNotNull(result1);
        // check state before update has no lookup entries
        Assert.assertTrue(equalsDisregardPriority(result1.getLeft(), newSeed(seedId, null)));
        // make sure there is no lookup entries failed to associate to seed or set lookup mapping
        verifyNoAssociationFailure(result1);

        EntityRawSeed seedToUpdate2 = newSeed(seedId, "sfdc_2", "facebook.com", "netflix.com");
        Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result2 = entityMatchInternalService
                .associate(TEST_TENANT, seedToUpdate2, false);
        Assert.assertNotNull(result2);
        // check state before update
        Assert.assertTrue(equalsDisregardPriority(result2.getLeft(), seedToUpdate1));
        Assert.assertNotNull(result2.getMiddle());
        Assert.assertNotNull(result2.getRight());
        Assert.assertEquals(result2.getMiddle().size(), 1);
        Assert.assertEquals(
                result2.getMiddle().get(0),
                fromExternalSystem(TEST_ENTITY, EXT_SYSTEM_SFDC, "sfdc_2"));
        Assert.assertTrue(result2.getRight().isEmpty());

        // update domain again and make sure it does not get reported in failure
        result2 = entityMatchInternalService.associate(TEST_TENANT,
                newSeed(seedId, null, "facebook.com", "netflix.com"), false);
        Assert.assertNotNull(result2);
        verifyNoAssociationFailure(result2);

        Thread.sleep(1000L);
        cleanupSeedAndLookup(env, seedId);
    }

    /**
     * Test association in detail (check actual seed & lookup entry set in staging)
     *
     * @param currSeed the seed that is currently in staging
     * @param currLookupMappings lookup entries that are currently in staging
     * @param seedToAssociate seed object that we want to associate to the current seed
     * @param finalSeed expected final state of the seed
     * @param entriesFailedToAssociate set of lookup entries that failed to associate with the seed
     * @param entriesFailedToSetLookup set of lookup entries that have no conflict with the seed content
     *                                 but not able to map to the seed
     */
    @Test(groups = "functional", dataProvider = "entityAssociation")
    private void testAssociationDetail(
            EntityRawSeed currSeed, @NotNull List<Pair<EntityLookupEntry, String>> currLookupMappings,
            @NotNull EntityRawSeed seedToAssociate, @NotNull EntityRawSeed finalSeed,
            @NotNull Set<EntityLookupEntry> entriesFailedToAssociate,
            @NotNull Set<EntityLookupEntry> entriesFailedToSetLookup) throws Exception {
        entityMatchConfigurationService.setIsAllocateMode(true); // association only works with allocate mode
        Assert.assertNotNull(seedToAssociate);
        String seedId = seedToAssociate.getId();
        String entity = seedToAssociate.getEntity();
        // association happens in staging
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING;
        // cleanup
        entityRawSeedService.delete(env, TEST_TENANT, entity, seedId);
        // prepare current state
        if (currSeed != null) {
            entityRawSeedService.setIfNotExists(env, TEST_TENANT, currSeed, true);
        }
        entityLookupEntryService.set(env, TEST_TENANT, currLookupMappings, true);

        Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result =
                entityMatchInternalService.associate(TEST_TENANT, seedToAssociate, false);
        Assert.assertNotNull(result);
        // currently, we only return updated old attribute, so no equals current seed
        Assert.assertNotNull(result.getLeft());
        Assert.assertNotNull(result.getMiddle());
        Assert.assertNotNull(result.getRight());
        Assert.assertEquals(new HashSet<>(result.getMiddle()), entriesFailedToAssociate);
        Assert.assertEquals(new HashSet<>(result.getRight()), entriesFailedToSetLookup);

        // just in case
        Thread.sleep(1000L);

        // verify final state
        EntityRawSeed seedAfterAssociation = entityRawSeedService.get(env, TEST_TENANT, entity, seedId);
        Assert.assertTrue(equalsDisregardPriority(seedAfterAssociation, finalSeed));
        // check x to one lookup entries in seed that does not failed to set lookup
        List<EntityLookupEntry> manyToXEntries = seedAfterAssociation.getLookupEntries()
                .stream()
                .filter(entry -> !result.getRight().contains(entry))
                .filter(entry -> entry.getType().mapping != EntityLookupEntry.Mapping.MANY_TO_MANY)
                .collect(Collectors.toList());
        List<String> seedIdsForEntriesInSeed = entityLookupEntryService
                .get(env, TEST_TENANT, manyToXEntries);
        Assert.assertNotNull(seedIdsForEntriesInSeed);
        Assert.assertEquals(seedIdsForEntriesInSeed.size(), manyToXEntries.size());
        // make sure the mapped seed ID is correct
        seedIdsForEntriesInSeed.forEach(id -> Assert.assertEquals(id, seedId));

        // check lookup entries failed to set lookup
        List<String> seedIdsForEntriesFailedToSetLookup = entityLookupEntryService
                .get(env, TEST_TENANT, new ArrayList<>(result.getRight()));
        Assert.assertNotNull(seedIdsForEntriesFailedToSetLookup);
        Assert.assertEquals(seedIdsForEntriesFailedToSetLookup.size(), entriesFailedToSetLookup.size());
        // should not mapped to the seed we try to associate
        seedIdsForEntriesFailedToSetLookup.forEach(id -> Assert.assertNotEquals(id, seedId));

        // cleanup both seed & entries
        entityRawSeedService.delete(env, TEST_TENANT, entity, seedId);
        seedToAssociate.getLookupEntries().forEach(entry -> entityLookupEntryService.delete(env, TEST_TENANT, entry));
        currLookupMappings.forEach(pair -> entityLookupEntryService.delete(env, TEST_TENANT, pair.getKey()));
    }

    @Test(groups = "functional")
    private void testLookupCacheLimit() throws Exception {
        entityMatchConfigurationService.setIsAllocateMode(false); // use lookup mode so the test will be faster
        setupServing(TEST_ENTRIES.toArray(new EntityLookupEntry[0]));
        Thread.sleep(2000L);

        // make sure we still can retrieve everything even if the cache limit exceeded
        List<String> seedIds = entityMatchInternalService.getIds(TEST_TENANT, TEST_ENTRIES);
        Assert.assertNotNull(seedIds);
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        seedIds.forEach(id -> Assert.assertEquals(id, SEED_ID_FOR_LOOKUP));

        // make sure cache entry that exceeds limit are clean up
        entityMatchInternalService.getLookupCache().cleanUp();
        Assert.assertEquals(entityMatchInternalService.getLookupCache().estimatedSize(), MAX_LOOKUP_CACHE_LIMIT);
    }

    @Test(groups = "functional")
    private void testSeedCacheLimit() throws Exception {
        entityMatchConfigurationService.setIsAllocateMode(false); // use lookup mode so the test will be faster
        cleanup(TEST_TENANT, SEED_IDS);
        setupServing(TEST_SEED_1, TEST_SEED_2, TEST_SEED_3);
        Thread.sleep(2000L);

        List<EntityRawSeed> results = entityMatchInternalService.get(TEST_TENANT, TEST_ENTITY, SEED_IDS);
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), SEED_IDS.size());
        // only test seed 1, 2 & 3 exists
        verifyEntityRawSeeds(results, SEED_ID_1, SEED_ID_2, SEED_ID_3, null, null);

        // make sure cache entry that exceeds limit are clean up
        entityMatchInternalService.getSeedCache().cleanUp();
        // limit is set to 6, so only two seeds can be in cache (can be any combination)
        Assert.assertEquals(entityMatchInternalService.getSeedCache().estimatedSize(), 2);
    }

    @Test(groups = "functional")
    private void testEntityPublish() {
        Tenant tenant1 = new Tenant(this.getClass().getSimpleName() + UUID.randomUUID().toString());
        Tenant tenant2 = new Tenant(this.getClass().getSimpleName() + UUID.randomUUID().toString());
        Tenant tenant3 = new Tenant(this.getClass().getSimpleName() + UUID.randomUUID().toString());

        // Test publish without data, expect to finish without exception
        entityMatchInternalService.publishEntity(TEST_ENTITY, tenant1, tenant1, EntityMatchEnvironment.STAGING,
                Boolean.TRUE);

        // Prepare data:
        // tenant 1 with seed & lookup entries to publish, no same lookup
        // entries among different seeds
        // tenant 2 with shuffled seed & lookup entries to verify publish is
        // tenant specific, also have same lookup entries among different seeds
        // to test lookup publish correctness (only publish lookup entries which
        // actually point to seed)
        EntityRawSeed seed1 = newSeed(SEED_ID_1, "sfdc_1", "google.com");
        EntityRawSeed seed2 = newSeed(SEED_ID_2, null, "fb.com", "abc.com");
        EntityRawSeed seed3 = newSeed(SEED_ID_3, "sfdc_3");
        List<EntityRawSeed> seeds = Arrays.asList(seed1, seed2, seed3);
        List<String> seedIds = seeds.stream() //
                .map(EntityRawSeed::getId) //
                .collect(Collectors.toList());
        // Pair<EntityLookupEntry, SeedId>
        List<Pair<EntityLookupEntry, String>> lookupPairs = seeds.stream() //
                .flatMap(
                        seed -> seed.getLookupEntries().stream() //
                                .map(lookupEntry -> Pair.of(lookupEntry, seed.getId()))) //
                .collect(Collectors.toList());
        List<EntityLookupEntry> lookups = lookupPairs.stream() //
                .map(pair -> pair.getLeft()) //
                .collect(Collectors.toList());
        setupLookupTable(EntityMatchEnvironment.STAGING, tenant1, lookupPairs);
        setupSeedTable(EntityMatchEnvironment.STAGING, tenant1, seeds);

        // Lookup entries are shuffled ()
        EntityRawSeed noiseSeed1 = newSeed(SEED_ID_1, "sfdc_1", "google.com");
        // All lookup entries will point to noiseSeed2 to test lookup entry
        // publish correctness
        EntityRawSeed noiseSeed2 = newSeed(SEED_ID_2, "sfdc_1", "google.com", "abc.com");
        EntityRawSeed noiseSeed3 = newSeed(SEED_ID_3, null, "abc.com");
        List<EntityRawSeed> noiseSeeds = Arrays.asList(noiseSeed1, noiseSeed2, noiseSeed3);
        List<String> noiseSeedIds = noiseSeeds.stream() //
                .map(EntityRawSeed::getId) //
                .collect(Collectors.toList());
        List<Pair<EntityLookupEntry, String>> noiseLookupPairs = noiseSeed2.getLookupEntries().stream() //
                .map(lookupEntry -> Pair.of(lookupEntry, SEED_ID_2)) //
                .collect(Collectors.toList());
        List<EntityLookupEntry> noiseLookups = noiseLookupPairs.stream() //
                .map(pair -> pair.getLeft()) //
                .collect(Collectors.toList());
        setupLookupTable(EntityMatchEnvironment.STAGING, tenant2, noiseLookupPairs);
        setupSeedTable(EntityMatchEnvironment.STAGING, tenant2, noiseSeeds);

        // Test checkpoint save & restore (staging -> staging with different
        // tenant)
        // Prepared data for tenant 1 & 2 in staging, and select tenant1's data
        // to publish to tenant3 in staging
        EntityPublishStatistics stats = entityMatchInternalService.publishEntity(TEST_ENTITY, tenant1, tenant3,
                EntityMatchEnvironment.STAGING, Boolean.TRUE);
        Assert.assertEquals(stats.getSeedCount(), seeds.size());
        // There are 5 possible lookup options in seeds
        Assert.assertEquals(stats.getLookupCount(), 5);
        waitForAllLookupEntriesPopulated();

        seeds.forEach(seed -> {
            List<String> matchedSeedIds = entityLookupEntryService.get(EntityMatchEnvironment.STAGING, tenant3,
                    seed.getLookupEntries());
            matchedSeedIds.forEach(seedId -> {
                Assert.assertEquals(seedId, seed.getId());
            });
        });
        List<EntityRawSeed> matchedSeeds = entityRawSeedService.get(EntityMatchEnvironment.STAGING, tenant3,
                TEST_ENTITY, seedIds);
        Assert.assertFalse(matchedSeeds.contains(null));

        // Test pa publish (staging -> serving with same tenant)
        // Prepared data for tenant 1 & 2 in staging and select tenant1's data
        // to publish to tenant1 in serving
        stats = entityMatchInternalService.publishEntity(TEST_ENTITY, tenant1, tenant1, EntityMatchEnvironment.SERVING,
                Boolean.TRUE);
        Assert.assertEquals(stats.getSeedCount(), seeds.size());
        // There are 5 possible lookup options in seeds
        Assert.assertEquals(stats.getLookupCount(), 5);
        waitForAllLookupEntriesPopulated();

        seeds.forEach(seed -> {
            List<String> matchedSeedIds = entityLookupEntryService.get(EntityMatchEnvironment.SERVING, tenant1,
                    seed.getLookupEntries());
            matchedSeedIds.forEach(seedId -> {
                Assert.assertEquals(seedId, seed.getId());
            });
        });
        matchedSeeds = entityRawSeedService.get(EntityMatchEnvironment.SERVING, tenant1, TEST_ENTITY, seedIds);
        Assert.assertFalse(matchedSeeds.contains(null));

        // Test seeds having same lookup entries but only lookup entries which
        // actually point to seed are published
        stats = entityMatchInternalService.publishEntity(TEST_ENTITY, tenant2, tenant2, EntityMatchEnvironment.SERVING,
                Boolean.TRUE);
        Assert.assertEquals(stats.getSeedCount(), noiseSeeds.size());
        // There are 3 possible lookup options in noiseSeeds
        Assert.assertEquals(stats.getLookupCount(), 3);
        waitForAllLookupEntriesPopulated();

        noiseSeeds.forEach(seed -> {
            if (CollectionUtils.isNotEmpty(seed.getLookupEntries())) {
                List<String> matchedSeedIds = entityLookupEntryService.get(EntityMatchEnvironment.SERVING, tenant2,
                        seed.getLookupEntries());
                matchedSeedIds.forEach(seedId -> {
                    Assert.assertEquals(seedId, SEED_ID_2);
                });
            }
        });
        matchedSeeds = entityRawSeedService.get(EntityMatchEnvironment.SERVING, tenant2, TEST_ENTITY, seedIds);
        Assert.assertFalse(matchedSeeds.contains(null));

        // Clean up data
        cleanup(tenant1, lookups.toArray(new EntityLookupEntry[lookups.size()]));
        cleanup(tenant2, noiseLookups.toArray(new EntityLookupEntry[noiseLookups.size()]));
        cleanup(tenant3, lookups.toArray(new EntityLookupEntry[lookups.size()]));
        cleanup(tenant1, seedIds);
        cleanup(tenant2, noiseSeedIds);
        cleanup(tenant3, seedIds);
    }

    // [ nAllocations, nThreads ]
    @DataProvider(name = "entityIdAllocation")
    private Object[][] provideEntityIdAllocationTests() {
        return new Object[][] {
                { 50, 10 },
        };
    }

    @DataProvider(name = "entityAssociation")
    private Object[][] provideEntityAssociationTestData() {
        return new Object[][] {
                /*
                 * conflict with DUNS
                 */
                {
                        // already has DUNS in seed
                        fromEntries(ASSOCIATION_SEED_ID, DUNS_1),
                        Collections.singletonList(Pair.of(DUNS_1, ASSOCIATION_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, DUNS_2),
                        fromEntries(ASSOCIATION_SEED_ID, DUNS_1),
                        Sets.newHashSet(DUNS_2),
                        Sets.newHashSet(),
                },
                {
                        // no DUNS in seed
                        // the DUNS we try to associate already mapped to another seed
                        fromEntries(ASSOCIATION_SEED_ID),
                        Collections.singletonList(Pair.of(DUNS_2, ANOTHER_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, DUNS_2),
                        fromEntries(ASSOCIATION_SEED_ID, DUNS_2),
                        Sets.newHashSet(),
                        Sets.newHashSet(DUNS_2),
                },
                {
                        // already has DUNS in seed
                        // the DUNS we try to associate already mapped to another seed
                        fromEntries(ASSOCIATION_SEED_ID, DUNS_1),
                        Arrays.asList(Pair.of(DUNS_1, ASSOCIATION_SEED_ID), Pair.of(DUNS_2, ANOTHER_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, DUNS_2),
                        fromEntries(ASSOCIATION_SEED_ID, DUNS_1),
                        Sets.newHashSet(DUNS_2), // failed to associate, not try to set lookup
                        Sets.newHashSet(),
                },
                /*
                 * conflict with external system
                 */
                {
                        fromEntries(ASSOCIATION_SEED_ID),
                        Collections.singletonList(Pair.of(SFDC_1, ANOTHER_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_1),
                        fromEntries(ASSOCIATION_SEED_ID), // SFDC_1 should be cleared in seed
                        Sets.newHashSet(),
                        Sets.newHashSet(SFDC_1),
                },
                {
                        // already has another ID in the same system
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_2),
                        Collections.singletonList(Pair.of(SFDC_2, ASSOCIATION_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_1),
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_2), // SFDC_1 not be in the seed
                        Sets.newHashSet(SFDC_1), // failed to associate SFDC_1, not even trying to set lookup
                        Sets.newHashSet(),
                },
                {
                        // already has another ID in the same system
                        // system ID we want to associate already mapped to another seed
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_2),
                        Arrays.asList(Pair.of(SFDC_1, ANOTHER_SEED_ID), Pair.of(SFDC_2, ASSOCIATION_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_1),
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_2), // SFDC_1 not be in the seed
                        Sets.newHashSet(SFDC_1), // failed to associate SFDC_1, not even trying to set lookup
                        Sets.newHashSet(),
                },
                /*
                 * conflict with domain/country & name/country (only possible in lookup)
                 */
                {
                        // name/country
                        fromEntries(ASSOCIATION_SEED_ID),
                        Collections.singletonList(Pair.of(NC_GOOGLE_1, ANOTHER_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, NC_GOOGLE_1),
                        // association to seed still succeeded since the mapping is many to many
                        fromEntries(ASSOCIATION_SEED_ID, NC_GOOGLE_1),
                        Sets.newHashSet(),
                        Sets.newHashSet(NC_GOOGLE_1), // failed to set lookup since it already mapped to another ID
                },
                {
                        // domain/country
                        fromEntries(ASSOCIATION_SEED_ID),
                        Collections.singletonList(Pair.of(DC_FACEBOOK_1, ANOTHER_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, DC_FACEBOOK_1),
                        // association to seed still succeeded since the mapping is many to many
                        fromEntries(ASSOCIATION_SEED_ID, DC_FACEBOOK_1),
                        Sets.newHashSet(),
                        Sets.newHashSet(DC_FACEBOOK_1), // failed to set lookup since it already mapped to another ID
                },
                {
                        // name/country, no conflict with other entries in current seed
                        fromEntries(ASSOCIATION_SEED_ID, NC_GOOGLE_2, NC_GOOGLE_3),
                        Collections.singletonList(Pair.of(NC_GOOGLE_1, ANOTHER_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, NC_GOOGLE_1),
                        // association to seed still succeeded since the mapping is many to many
                        fromEntries(ASSOCIATION_SEED_ID, NC_GOOGLE_1, NC_GOOGLE_2, NC_GOOGLE_3),
                        Sets.newHashSet(),
                        Sets.newHashSet(NC_GOOGLE_1), // failed to set lookup since it already mapped to another ID
                },
                {
                        // domain/country, no conflict with other entries in current seed
                        fromEntries(ASSOCIATION_SEED_ID, DC_FACEBOOK_2),
                        Collections.singletonList(Pair.of(DC_FACEBOOK_1, ANOTHER_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, DC_FACEBOOK_1),
                        // association to seed still succeeded since the mapping is many to many
                        fromEntries(ASSOCIATION_SEED_ID, DC_FACEBOOK_1, DC_FACEBOOK_2),
                        Sets.newHashSet(),
                        Sets.newHashSet(DC_FACEBOOK_1), // failed to set lookup since it already mapped to another ID
                },
                /*
                 * no conflict
                 */
                {
                        // SFDC_1 is set since it already maps to the same seed, no error
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_1),
                        Collections.singletonList(Pair.of(SFDC_1, ASSOCIATION_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_1),
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_1),
                        Sets.newHashSet(),
                        Sets.newHashSet(),
                },
                {
                        // update different lookup entry types with the same value
                        fromEntries(ASSOCIATION_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_2,
                                NC_NETFLIX_2, DUNS_1, MKTO_1, ELOQUA_4),
                        Arrays.asList(
                                Pair.of(DC_GOOGLE_1, ASSOCIATION_SEED_ID),
                                Pair.of(DC_GOOGLE_2, ASSOCIATION_SEED_ID),
                                Pair.of(NC_NETFLIX_2, ASSOCIATION_SEED_ID),
                                Pair.of(MKTO_1, ASSOCIATION_SEED_ID),
                                Pair.of(ELOQUA_4, ASSOCIATION_SEED_ID),
                                Pair.of(DUNS_1, ASSOCIATION_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, DC_GOOGLE_1, NC_NETFLIX_2, DUNS_1, MKTO_1, ELOQUA_4),
                        fromEntries(ASSOCIATION_SEED_ID,
                                DC_GOOGLE_1, NC_NETFLIX_2, DUNS_1, MKTO_1, DC_GOOGLE_2, ELOQUA_4),
                        Sets.newHashSet(),
                        Sets.newHashSet(),
                },
                {
                        // happy path
                        fromEntries(ASSOCIATION_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_2, NC_NETFLIX_2, DUNS_1),
                        Arrays.asList(
                                Pair.of(DC_GOOGLE_1, ASSOCIATION_SEED_ID),
                                Pair.of(DC_GOOGLE_2, ASSOCIATION_SEED_ID),
                                Pair.of(NC_NETFLIX_2, ASSOCIATION_SEED_ID),
                                Pair.of(DUNS_1, ASSOCIATION_SEED_ID)),
                        fromEntries(ASSOCIATION_SEED_ID, SFDC_1, MKTO_1, ELOQUA_3),
                        fromEntries(ASSOCIATION_SEED_ID,
                                DC_GOOGLE_1, DC_GOOGLE_2, NC_NETFLIX_2, DUNS_1, SFDC_1, MKTO_1, ELOQUA_3),
                        Sets.newHashSet(),
                        Sets.newHashSet(),
                },
        };
    }

    // Cleanup single seed in specified env
    private void cleanupSeedAndLookup(EntityMatchEnvironment env, String seedId) {
        EntityRawSeed seed = entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        if (seed == null) {
            return;
        }

        // cleanup seed
        entityRawSeedService.delete(env, TEST_TENANT, TEST_ENTITY, seedId);
        // cleanup corresponding lookup entries
        seed.getLookupEntries().forEach(entry -> entityLookupEntryService.delete(env, TEST_TENANT, entry));
    }

    // Cleanup lookup entries in all env
    private void cleanup(Tenant tenant, EntityLookupEntry... entries) {
        Arrays.stream(entries)
                .forEach(entry -> entityLookupEntryService.delete(EntityMatchEnvironment.STAGING, tenant, entry));
        Arrays.stream(entries)
                .forEach(entry -> entityLookupEntryService.delete(EntityMatchEnvironment.SERVING, tenant, entry));
    }

    // Cleanup seeds in all env
    private void cleanup(Tenant tenant, List<String> seedIds) {
        seedIds.forEach(id -> entityRawSeedService.delete(EntityMatchEnvironment.STAGING, tenant, TEST_ENTITY, id));
        seedIds.forEach(id -> entityRawSeedService.delete(EntityMatchEnvironment.SERVING, tenant, TEST_ENTITY, id));
    }

    // Setup lookup entries in specified env
    private void setupLookupTable(EntityMatchEnvironment env, Tenant tenant,
            List<Pair<EntityLookupEntry, String>> pairs) {
        entityLookupEntryService.set(env, tenant, pairs, true);
    }

    // Setup seeds in specified env
    private void setupSeedTable(EntityMatchEnvironment env, Tenant tenant, List<EntityRawSeed> seeds) {
        seeds.forEach(seed -> entityRawSeedService.setIfNotExists(env, tenant, seed, true));
    }

    // Setup lookup entries with fixed seed id in serving env
    private void setupServing(EntityLookupEntry... entries) {
        entityLookupEntryService.set(EntityMatchEnvironment.SERVING, TEST_TENANT,
                Arrays.stream(entries) //
                        .map(entry -> Pair.of(entry, SEED_ID_FOR_LOOKUP)) //
                        .collect(Collectors.toList()),
                true);
    }

    // Setup seeds in serving env
    private void setupServing(EntityRawSeed... seeds) {
        Arrays.stream(seeds) //
                .forEach(seed -> entityRawSeedService
                .setIfNotExists(EntityMatchEnvironment.SERVING, TEST_TENANT, seed, true));
    };

    /*
     * make sure association result has no entries that either fail to update seed or lookup
     */
    private void verifyNoAssociationFailure(
            @NotNull Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result) {
        Assert.assertNotNull(result.getMiddle());
        Assert.assertTrue(result.getMiddle().isEmpty());
        Assert.assertNotNull(result.getRight());
        Assert.assertTrue(result.getRight().isEmpty());
    }

    /*
     * helper to check seed, not using equals because list of EntityLookupEntry might be in different order
     */
    private void verifyEntityRawSeeds(List<EntityRawSeed> seeds, String... expectedIds) {
        Assert.assertEquals(seeds.size(), expectedIds.length);
        IntStream.range(0, seeds.size()).forEach(idx -> {
            if (expectedIds[idx] == null) {
                Assert.assertNull(seeds.get(idx));
            } else {
                Assert.assertNotNull(seeds.get(idx));
                Assert.assertEquals(seeds.get(idx).getId(), expectedIds[idx]);
            }
        });
    }

    private void waitForAllLookupEntriesPopulated() {
        for (int i = 0; i < MAX_WAIT_TIMES; i++) {
            try {
                Thread.sleep(WAIT_INTERVAL);
            } catch (Exception e) {
                Assert.fail("Failed to wait for all background lookup entries to be populated", e);
            }

            if (entityMatchInternalService.getProcessingLookupEntriesCount() == 0L) {
                return;
            }
        }
        Assert.fail(String.format("Max wait times (%d) for background lookup entries exceeded", MAX_WAIT_TIMES));
    }

    /*
     * wrapper to make function call shorter
     */
    private static EntityRawSeed fromEntries(String seedId, EntityLookupEntry... entries) {
        return TestEntityMatchUtils.newSeed(seedId, entries);
    }

    private static EntityRawSeed newSeed(String seedId, String sfdcId, String... domains) {
        List<EntityLookupEntry> entries = new ArrayList<>();
        Map<String, String> attributes = new HashMap<>();
        if (sfdcId != null) {
            entries.add(fromExternalSystem(TEST_ENTITY, EXT_SYSTEM_SFDC, sfdcId));
        }
        if (domains != null) {
            Arrays.stream(domains).forEach(domain ->
                    entries.add(fromDomainCountry(TEST_ENTITY, domain, TEST_COUNTRY)));
        }
        return new EntityRawSeed(seedId, TEST_ENTITY, 0, entries, attributes);
    }
}
