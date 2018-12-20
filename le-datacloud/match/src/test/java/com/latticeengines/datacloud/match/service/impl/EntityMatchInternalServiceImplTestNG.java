package com.latticeengines.datacloud.match.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.equalsDisregardPriority;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.*;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDomainCountry;

public class EntityMatchInternalServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final long WAIT_INTERVAL = 500L;
    private static final int MAX_WAIT_TIMES = 60; // 30s
    private static final String TEST_SERVING_TABLE = "CDLMatchServingDev_20181126";
    private static final String TEST_STAGING_TABLE = "CDLMatchDev_20181126";
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final Tenant TEST_TENANT = getTestTenant();
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
    private EntityMatchInternalServiceImpl entityMatchInternalService;

    @Inject
    @InjectMocks
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    @InjectMocks
    private EntityRawSeedService entityRawSeedService;

    @Mock
    private EntityMatchConfigurationServiceImpl entityMatchConfigurationService;

    @BeforeClass(groups = "functional")
    private void setupShared() {
        MockitoAnnotations.initMocks(this);
        entityMatchConfigurationService.setServingTableName(TEST_SERVING_TABLE);
        entityMatchConfigurationService.setStagingTableName(TEST_STAGING_TABLE);
        cleanup(TEST_ENTRIES.toArray(new EntityLookupEntry[0]));
    }

    @AfterClass(groups = "functional")
    private void tearDownShared() throws Exception {
        entityMatchInternalService.shutdownAndAwaitTermination();
        // test entries are sync asynchronously, cleanup here just in case
        cleanup(TEST_ENTRIES.toArray(new EntityLookupEntry[0]));
    }

    @Test(groups = "functional")
    private void testLookupSeedBulkMode() throws Exception {
        entityMatchInternalService.setRealTimeMode(false);
        cleanup(TEST_ENTRIES.toArray(new EntityLookupEntry[0]));

        setupServing(TEST_ENTRY_1, TEST_ENTRY_3);

        List<String> seedIds = entityMatchInternalService.getIds(TEST_TENANT, TEST_ENTRIES);
        Assert.assertNotNull(seedIds);
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        // only entry 1 & 3 are set to serving
        Assert.assertEquals(seedIds, Arrays.asList(SEED_ID_FOR_LOOKUP, null, SEED_ID_FOR_LOOKUP, null));

        // check in-memory cache
        Cache<Pair<Long, EntityLookupEntry>, String> lookupCache = entityMatchInternalService.getLookupCache();
        Assert.assertNotNull(lookupCache);
        Assert.assertEquals(lookupCache.getIfPresent(Pair.of(TEST_TENANT.getPid(), TEST_ENTRY_1)), SEED_ID_FOR_LOOKUP);
        Assert.assertEquals(lookupCache.getIfPresent(Pair.of(TEST_TENANT.getPid(), TEST_ENTRY_3)), SEED_ID_FOR_LOOKUP);
        Assert.assertNull(lookupCache.getIfPresent(TEST_ENTRY_2));
        Assert.assertNull(lookupCache.getIfPresent(TEST_ENTRY_4));
        // clean cache
        lookupCache.invalidateAll();

        // check staging (async update)
        waitForAllLookupEntriesPopulated();
        List<String> seedIdsInStaging = entityLookupEntryService.get(
                EntityMatchEnvironment.STAGING, TEST_TENANT, TEST_ENTRIES);
        // only entry 1 & 3 are set to serving
        Assert.assertEquals(seedIdsInStaging, Arrays.asList(SEED_ID_FOR_LOOKUP, null, SEED_ID_FOR_LOOKUP, null));
    }

    @Test(groups = "functional")
    private void testSeedBulkMode() {
        entityMatchInternalService.setRealTimeMode(false);
        cleanup(SEED_IDS);
        setupServing(TEST_SEED_1, TEST_SEED_2, TEST_SEED_3);

        List<EntityRawSeed> results = entityMatchInternalService.get(TEST_TENANT, TEST_ENTITY, SEED_IDS);
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), SEED_IDS.size());
        // only test seed 1, 2 & 3 exists
        verifyEntityRawSeeds(results, SEED_ID_1, SEED_ID_2, SEED_ID_3, null, null);

        // check in-memory cache (should not use cache in bulk mode for seed)
        Pair<Long, String> prefix = Pair.of(TEST_TENANT.getPid(), TEST_ENTITY);
        Cache<Pair<Pair<Long, String>, String>, EntityRawSeed> seedCache = entityMatchInternalService
                .getSeedCache();
        Assert.assertNotNull(seedCache);
        SEED_IDS.forEach(id -> seedCache.getIfPresent(Pair.of(prefix, id)));
        // clean cache
        seedCache.invalidateAll();

        // check staging
        List<EntityRawSeed> resultsInStaging = entityRawSeedService
                .get(EntityMatchEnvironment.STAGING, TEST_TENANT, TEST_ENTITY, SEED_IDS);
        verifyEntityRawSeeds(resultsInStaging, SEED_ID_1, SEED_ID_2, SEED_ID_3, null, null);

        cleanup(SEED_IDS);
    }

    /*
     * allocate ID in parallel
     */
    @Test(groups = "functional", dataProvider = "entityIdAllocation")
    private void testIDAllocation(int nAllocations, int nThreads) {
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
        entityIds.forEach(id -> entityRawSeedService.delete(EntityMatchEnvironment.SERVING, TEST_TENANT, TEST_ENTITY, id));
    }

    @Test(groups = "functional")
    private void testAssociation() throws Exception {
        String seedId = "testAssociation"; // prevent conflict
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING;
        cleanupSeedAndLookup(env, seedId);

        boolean created = entityRawSeedService
                .createIfNotExists(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertTrue(created);

        EntityRawSeed seedToUpdate1 = newSeed(seedId, "sfdc_1", "google.com");
        Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result1 = entityMatchInternalService
                .associate(TEST_TENANT, seedToUpdate1);
        Assert.assertNotNull(result1);
        // check state before update has no lookup entries
        Assert.assertTrue(equalsDisregardPriority(result1.getLeft(), newSeed(seedId, null)));
        // make sure there is no lookup entries failed to associate to seed or set lookup mapping
        verifyNoAssociationFailure(result1);

        EntityRawSeed seedToUpdate2 = newSeed(seedId, "sfdc_2", "facebook.com", "netflix.com");
        Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result2 = entityMatchInternalService
                .associate(TEST_TENANT, seedToUpdate2);
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
                newSeed(seedId, null, "facebook.com", "netflix.com"));
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
        Assert.assertNotNull(seedToAssociate);
        String seedId = seedToAssociate.getId();
        String entity = seedToAssociate.getEntity();
        // association happens in staging
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING;
        // cleanup
        entityRawSeedService.delete(env, TEST_TENANT, entity, seedId);
        // prepare current state
        if (currSeed != null) {
            entityRawSeedService.setIfNotExists(env, TEST_TENANT, currSeed);
        }
        entityLookupEntryService.set(env, TEST_TENANT, currLookupMappings);

        Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result =
                entityMatchInternalService.associate(TEST_TENANT, seedToAssociate);
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
        currLookupMappings.forEach(pair -> entityLookupEntryService.delete(env, TEST_TENANT, pair.getKey()));
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

    private void cleanup(EntityLookupEntry... entries) {
        Arrays.stream(entries).forEach(entry -> entityLookupEntryService
                .delete(EntityMatchEnvironment.STAGING, TEST_TENANT, entry));
        Arrays.stream(entries).forEach(entry -> entityLookupEntryService
                .delete(EntityMatchEnvironment.SERVING, TEST_TENANT, entry));
    }

    private void setupServing(EntityLookupEntry... entries) {
        entityLookupEntryService.set(
                EntityMatchEnvironment.SERVING, TEST_TENANT,
                Arrays.stream(entries).map(entry -> Pair.of(entry, SEED_ID_FOR_LOOKUP)).collect(Collectors.toList()));
    }

    private void cleanup(List<String> seedIds) {
        seedIds.forEach(id -> entityRawSeedService.delete(EntityMatchEnvironment.STAGING, TEST_TENANT, TEST_ENTITY, id));
        seedIds.forEach(id -> entityRawSeedService.delete(EntityMatchEnvironment.SERVING, TEST_TENANT, TEST_ENTITY, id));
    }

    private void setupServing(EntityRawSeed... seeds) {
        Arrays.stream(seeds).forEach(seed -> entityRawSeedService
                .setIfNotExists(EntityMatchEnvironment.SERVING, TEST_TENANT, seed));
    }

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

    private static Tenant getTestTenant() {
        Tenant tenant = new Tenant("entity_match_internal_service_test_tenant_1");
        tenant.setPid(713399053L);
        return tenant;
    }
}
