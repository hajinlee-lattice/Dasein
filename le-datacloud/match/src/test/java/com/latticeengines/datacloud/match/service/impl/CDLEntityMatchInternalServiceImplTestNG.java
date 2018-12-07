package com.latticeengines.datacloud.match.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.CDLLookupEntryService;
import com.latticeengines.datacloud.match.service.CDLRawSeedService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLRawSeed;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntryConverter.*;
import static com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntryConverter.fromDomainCountry;

public class CDLEntityMatchInternalServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_SERVING_TABLE = "CDLMatchServingDev_20181126";
    private static final String TEST_STAGING_TABLE = "CDLMatchDev_20181126";
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final Tenant TEST_TENANT = getTestTenant();
    private static final String EXT_SYSTEM_SFDC = "SFDC";
    private static final String EXT_SYSTEM_MARKETO = "MARKETO";
    private static final String TEST_COUNTRY = "USA";

    // test lookup entries
    private static final CDLLookupEntry TEST_ENTRY_1 = fromDomainCountry(
            TEST_ENTITY, "google.com", "USA");
    private static final CDLLookupEntry TEST_ENTRY_2 = fromExternalSystem(
            TEST_ENTITY, EXT_SYSTEM_SFDC, "sfdc_5566");
    private static final CDLLookupEntry TEST_ENTRY_3 = fromExternalSystem(
            TEST_ENTITY, EXT_SYSTEM_MARKETO, "mkt_1234");
    private static final CDLLookupEntry TEST_ENTRY_4 = fromDuns(TEST_ENTITY, "999999999");
    private static final List<CDLLookupEntry> TEST_ENTRIES = Arrays
            .asList(TEST_ENTRY_1, TEST_ENTRY_2, TEST_ENTRY_3, TEST_ENTRY_4);
    private static final String SEED_ID_FOR_LOOKUP = "seed_for_lookup_entry";

    private static final String SEED_ID_1 = "seed_1";
    private static final String SEED_ID_2 = "seed_2";
    private static final String SEED_ID_3 = "seed_3";
    private static final String SEED_ID_4 = "seed_4"; // not exists
    private static final String SEED_ID_5 = "seed_5"; // not exists
    private static final List<String> SEED_IDS = Arrays.asList(SEED_ID_1, SEED_ID_2, SEED_ID_3, SEED_ID_4, SEED_ID_5);
    private static final CDLRawSeed TEST_SEED_1 = newSeed(SEED_ID_1, "sfdc_1", "google.com");
    private static final CDLRawSeed TEST_SEED_2 = newSeed(SEED_ID_2, null, "fb.com", "abc.com");
    private static final CDLRawSeed TEST_SEED_3 = newSeed(SEED_ID_3, "sfdc_2");

    @Inject
    @InjectMocks
    private CDLEntityMatchInternalServiceImpl cdlEntityMatchInternalService;

    @Inject
    @InjectMocks
    private CDLLookupEntryService cdlLookupEntryService;

    @Inject
    @InjectMocks
    private CDLRawSeedService cdlRawSeedService;

    @Mock
    private CDLConfigurationServiceImpl cdlConfigurationService;

    @BeforeClass(groups = "functional")
    private void setupShared() {
        MockitoAnnotations.initMocks(this);
        cdlConfigurationService.setServingTableName(TEST_SERVING_TABLE);
        cdlConfigurationService.setStagingTableName(TEST_STAGING_TABLE);
        cleanup(TEST_ENTRIES.toArray(new CDLLookupEntry[0]));
    }

    @AfterClass(groups = "functional")
    private void tearDownShared() throws Exception {
        cdlEntityMatchInternalService.shutdownAndAwaitTermination();
        // test entries are sync asynchronously, cleanup here just in case
        cleanup(TEST_ENTRIES.toArray(new CDLLookupEntry[0]));
    }

    @Test(groups = "functional")
    private void testLookupSeedBulkMode() {
        cdlEntityMatchInternalService.setRealTimeMode(false);
        cleanup(TEST_ENTRIES.toArray(new CDLLookupEntry[0]));

        setupServing(TEST_ENTRY_1, TEST_ENTRY_3);

        List<String> seedIds = cdlEntityMatchInternalService.getIds(TEST_TENANT, TEST_ENTRIES);
        Assert.assertNotNull(seedIds);
        Assert.assertEquals(seedIds.size(), TEST_ENTRIES.size());
        // only entry 1 & 3 are set to serving
        Assert.assertEquals(seedIds.get(0), SEED_ID_FOR_LOOKUP);
        Assert.assertEquals(seedIds.get(2), SEED_ID_FOR_LOOKUP);
        Assert.assertNull(seedIds.get(1));
        Assert.assertNull(seedIds.get(3));

        // check in-memory cache
        Cache<Pair<Long, CDLLookupEntry>, String> lookupCache = cdlEntityMatchInternalService.getLookupCache();
        Assert.assertNotNull(lookupCache);
        Assert.assertEquals(lookupCache.getIfPresent(Pair.of(TEST_TENANT.getPid(), TEST_ENTRY_1)), SEED_ID_FOR_LOOKUP);
        Assert.assertEquals(lookupCache.getIfPresent(Pair.of(TEST_TENANT.getPid(), TEST_ENTRY_3)), SEED_ID_FOR_LOOKUP);
        Assert.assertNull(lookupCache.getIfPresent(TEST_ENTRY_2));
        Assert.assertNull(lookupCache.getIfPresent(TEST_ENTRY_4));
        // clean cache
        lookupCache.invalidateAll();

        // TODO check staging (currently its async update so need some work to check)
    }

    @Test(groups = "functional")
    private void testSeedBulkMode() {
        cdlEntityMatchInternalService.setRealTimeMode(false);
        cleanup(SEED_IDS);
        setupServing(TEST_SEED_1, TEST_SEED_2, TEST_SEED_3);

        List<CDLRawSeed> results = cdlEntityMatchInternalService.get(TEST_TENANT, TEST_ENTITY, SEED_IDS);
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), SEED_IDS.size());
        // only test seed 1, 2 & 3 exists
        verifyCDLRawSeeds(results, SEED_ID_1, SEED_ID_2, SEED_ID_3, null, null);

        // check in-memory cache (should not use cache in bulk mode for seed)
        Pair<Long, String> prefix = Pair.of(TEST_TENANT.getPid(), TEST_ENTITY);
        Cache<Pair<Pair<Long, String>, String>, CDLRawSeed> seedCache = cdlEntityMatchInternalService
                .getSeedCache();
        Assert.assertNotNull(seedCache);
        SEED_IDS.forEach(id -> seedCache.getIfPresent(Pair.of(prefix, id)));
        // clean cache
        seedCache.invalidateAll();

        // check staging
        List<CDLRawSeed> resultsInStaging = cdlRawSeedService
                .get(CDLMatchEnvironment.STAGING, TEST_TENANT, TEST_ENTITY, SEED_IDS);
        verifyCDLRawSeeds(resultsInStaging, SEED_ID_1, SEED_ID_2, SEED_ID_3, null, null);

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
                        cdlEntityMatchInternalService.allocateId(TEST_TENANT, TEST_ENTITY)))
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
        entityIds.forEach(id -> cdlRawSeedService.delete(CDLMatchEnvironment.SERVING, TEST_TENANT, TEST_ENTITY, id));
    }

    @Test(groups = "functional")
    private void testAssociation() throws Exception {
        String seedId = "testAssociation"; // prevent conflict
        CDLMatchEnvironment env = CDLMatchEnvironment.STAGING;
        cleanupSeedAndLookup(env, seedId);

        boolean created = cdlRawSeedService
                .createIfNotExists(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertTrue(created);

        CDLRawSeed seedToUpdate1 = newSeed(seedId, "sfdc_1", "google.com");
        Triple<CDLRawSeed, List<CDLLookupEntry>, List<CDLLookupEntry>> result1 = cdlEntityMatchInternalService
                .associate(TEST_TENANT, seedToUpdate1);
        Assert.assertNotNull(result1);
        // check state before update has no lookup entries
        Assert.assertTrue(equalsDisregardPriority(result1.getLeft(), newSeed(seedId, null)));
        // make sure there is no lookup entries failed to associate to seed or set lookup mapping
        verifyNoAssociationFailure(result1);

        CDLRawSeed seedToUpdate2 = newSeed(seedId, "sfdc_2", "facebook.com", "netflix.com");
        Triple<CDLRawSeed, List<CDLLookupEntry>, List<CDLLookupEntry>> result2 = cdlEntityMatchInternalService
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
        result2 = cdlEntityMatchInternalService.associate(TEST_TENANT,
                newSeed(seedId, null, "facebook.com", "netflix.com"));
        Assert.assertNotNull(result2);
        verifyNoAssociationFailure(result2);

        Thread.sleep(1000L);
        cleanupSeedAndLookup(env, seedId);
    }

    // [ nAllocations, nThreads ]
    @DataProvider(name = "entityIdAllocation")
    private Object[][] provideEntityIdAllocationTests() {
        return new Object[][] {
                { 50, 10 },
        };
    }

    private void cleanupSeedAndLookup(CDLMatchEnvironment env, String seedId) {
        CDLRawSeed seed = cdlRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        if (seed == null) {
            return;
        }

        // cleanup seed
        cdlRawSeedService.delete(env, TEST_TENANT, TEST_ENTITY, seedId);
        // cleanup corresponding lookup entries
        seed.getLookupEntries().forEach(entry -> cdlLookupEntryService.delete(env, TEST_TENANT, entry));
    }

    private void cleanup(CDLLookupEntry... entries) {
        Arrays.stream(entries).forEach(entry -> cdlLookupEntryService
                .delete(CDLMatchEnvironment.STAGING, TEST_TENANT, entry));
        Arrays.stream(entries).forEach(entry -> cdlLookupEntryService
                .delete(CDLMatchEnvironment.SERVING, TEST_TENANT, entry));
    }

    private void setupServing(CDLLookupEntry... entries) {
        cdlLookupEntryService.set(
                CDLMatchEnvironment.SERVING, TEST_TENANT,
                Arrays.stream(entries).map(entry -> Pair.of(entry, SEED_ID_FOR_LOOKUP)).collect(Collectors.toList()));
    }

    private void cleanup(List<String> seedIds) {
        seedIds.forEach(id -> cdlRawSeedService.delete(CDLMatchEnvironment.STAGING, TEST_TENANT, TEST_ENTITY, id));
        seedIds.forEach(id -> cdlRawSeedService.delete(CDLMatchEnvironment.SERVING, TEST_TENANT, TEST_ENTITY, id));
    }

    private void setupServing(CDLRawSeed... seeds) {
        Arrays.stream(seeds).forEach(seed -> cdlRawSeedService
                .setIfNotExists(CDLMatchEnvironment.SERVING, TEST_TENANT, seed));
    }

    /*
     * make sure association result has no entries that either fail to update seed or lookup
     */
    private void verifyNoAssociationFailure(
            @NotNull Triple<CDLRawSeed, List<CDLLookupEntry>, List<CDLLookupEntry>> result) {
        Assert.assertNotNull(result.getMiddle());
        Assert.assertTrue(result.getMiddle().isEmpty());
        Assert.assertNotNull(result.getRight());
        Assert.assertTrue(result.getRight().isEmpty());
    }

    /*
     * helper to check seed, not using equals because list of CDLLookupEntry might be in different order
     */
    private void verifyCDLRawSeeds(List<CDLRawSeed> seeds, String... expectedIds) {
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

    /*
     * compare seed ID, entity, and check if seed 1 & 2 have the same set of lookup entries
     */
    private boolean equalsDisregardPriority(CDLRawSeed seed1, CDLRawSeed seed2) {
        if (seed1 == null && seed2 == null) {
            return true;
        } else if (seed1 == null || seed2 == null) {
            return false;
        }

        if (!Objects.equals(seed1.getId(), seed2.getId())
                || !Objects.equals(seed1.getEntity(), seed2.getEntity())) {
            return false;
        }

        Set<CDLLookupEntry> entries1 = new HashSet<>(seed1.getLookupEntries());
        Set<CDLLookupEntry> entries2 = new HashSet<>(seed2.getLookupEntries());
        return entries1.equals(entries2);
    }

    private static CDLRawSeed newSeed(String seedId, String sfdcId, String... domains) {
        List<CDLLookupEntry> entries = new ArrayList<>();
        Map<String, String> attributes = new HashMap<>();
        if (sfdcId != null) {
            entries.add(fromExternalSystem(TEST_ENTITY, EXT_SYSTEM_SFDC, sfdcId));
        }
        if (domains != null) {
            Arrays.stream(domains).forEach(domain ->
                    entries.add(fromDomainCountry(TEST_ENTITY, domain, TEST_COUNTRY)));
        }
        return new CDLRawSeed(seedId, TEST_ENTITY, 0, entries, attributes);
    }

    private static Tenant getTestTenant() {
        Tenant tenant = new Tenant("cdl_match_internal_service_test_tenant_1");
        tenant.setPid(713399053L);
        return tenant;
    }
}
