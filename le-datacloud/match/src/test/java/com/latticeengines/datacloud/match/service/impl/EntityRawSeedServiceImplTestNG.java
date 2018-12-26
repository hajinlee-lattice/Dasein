package com.latticeengines.datacloud.match.service.impl;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_5;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_NETFLIX_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_NETFLIX_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_5;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.Seed.EMPTY;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.equalsDisregardPriority;

public class EntityRawSeedServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_SERVING_TABLE = "CDLMatchServingDev_20181126";
    private static final String TEST_STAGING_TABLE = "CDLMatchDev_20181126";
    private static final Tenant TEST_TENANT = new Tenant("raw_seed_service_test_tenant_1");
    // prevent scan tenant from being affected by other tests
    private static final Tenant TEST_SCAN_TENANT = new Tenant("raw_seed_service_test_tenant_for_scan_1");
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final String EXT_SYSTEM_SFDC = "SFDC";
    private static final String EXT_SYSTEM_MARKETO = "MARKETO";
    private static final String TEST_COUNTRY = "USA";
    private static final String UINS_SEED_ID = "testUpdateIfNotSet";
    private static final String CLR_SEED_ID = "testClear";

    @Inject
    @InjectMocks
    private EntityRawSeedServiceImpl entityRawSeedService;

    @Mock
    private EntityMatchConfigurationServiceImpl entityConfigurationService;

    @BeforeClass(groups = "functional")
    private void setup() {
        MockitoAnnotations.initMocks(this);
        entityConfigurationService.setStagingTableName(TEST_STAGING_TABLE);
        entityConfigurationService.setServingTableName(TEST_SERVING_TABLE);
        Mockito.when(entityConfigurationService.getRetryTemplate(Mockito.any())).thenCallRealMethod();
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment")
    private void testCreateIfNotExists(EntityMatchEnvironment env) {
        String seedId = "testCreateIfNotExists";

        // make sure we don't have this seed
        cleanup(env, seedId);

        // create successfully because no seed at the moment
        Assert.assertTrue(entityRawSeedService.createIfNotExists(env, TEST_TENANT, TEST_ENTITY, seedId));
        // should already exists
        Assert.assertFalse(entityRawSeedService.createIfNotExists(env, TEST_TENANT, TEST_ENTITY, seedId));

        // check the created seed
        EntityRawSeed seed = entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertNotNull(seed);
        Assert.assertEquals(seed.getId(), seedId);
        Assert.assertEquals(seed.getEntity(), TEST_ENTITY);

        // cleanup afterwards
        cleanup(env, seedId);
    }

    @Test(groups = "functional")
    private void testScanAndBatchCreate() throws InterruptedException {
        List<String> seedIds = Arrays.asList("testScan1", "testScan2", "testScan3", "testScan4", "testScan5",
                "testScan6", "testScan7");
        List<EntityRawSeed> scanSeeds = new ArrayList<>();
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING;
        EntityMatchEnvironment destEnv = EntityMatchEnvironment.SERVING;

        // make sure we don't have this seed
        for(String seedId : seedIds) {
            cleanupTenant(env, TEST_SCAN_TENANT, seedId);
        }
        for(String seedId : seedIds) {
            cleanupTenant(destEnv, TEST_SCAN_TENANT, seedId);
        }

        // create successfully because no seed at the moment
        for (String seedId : seedIds) {
            Assert.assertTrue(entityRawSeedService.createIfNotExists(env, TEST_SCAN_TENANT, TEST_ENTITY, seedId));
        }
        // should already exists
        List<String> getSeedIds = new ArrayList<>();
        int loopCount = 0;
        do {
            loopCount++;
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(
                    env, TEST_SCAN_TENANT, TEST_ENTITY, getSeedIds, 2);
            getSeedIds.clear();
            if (MapUtils.isNotEmpty(seeds)) {
                for (Map.Entry<Integer, List<EntityRawSeed>> entry : seeds.entrySet()) {
                    getSeedIds.add(entry.getValue().get(entry.getValue().size() - 1).getId());
                    scanSeeds.addAll(entry.getValue());
                    if (loopCount == 1) {
                        Assert.assertTrue(entry.getValue().size() <= 2);
                    }
                }
            }
        } while (CollectionUtils.isNotEmpty(getSeedIds));

        Assert.assertEquals(loopCount, 3);
        Assert.assertEquals(scanSeeds.size(), seedIds.size());

        //try another batch size
        scanSeeds.clear();
        loopCount = 0;
        do {
            loopCount++;
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(
                    env, TEST_SCAN_TENANT, TEST_ENTITY, getSeedIds, 3);
            getSeedIds.clear();
            if (MapUtils.isNotEmpty(seeds)) {
                for (Map.Entry<Integer, List<EntityRawSeed>> entry : seeds.entrySet()) {
                    getSeedIds.add(entry.getValue().get(entry.getValue().size() - 1).getId());
                    scanSeeds.addAll(entry.getValue());
                    Assert.assertTrue(entry.getValue().size() <= 3);
                }
            }
        } while (CollectionUtils.isNotEmpty(getSeedIds));

        Assert.assertEquals(loopCount, 3);
        Assert.assertEquals(scanSeeds.size(), seedIds.size());

        //try another batch size
        scanSeeds.clear();
        loopCount = 0;
        do {
            loopCount++;
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(
                    env, TEST_SCAN_TENANT, TEST_ENTITY, getSeedIds, 4);
            getSeedIds.clear();
            if (MapUtils.isNotEmpty(seeds)) {
                for (Map.Entry<Integer, List<EntityRawSeed>> entry : seeds.entrySet()) {
                    getSeedIds.add(entry.getValue().get(entry.getValue().size() - 1).getId());
                    scanSeeds.addAll(entry.getValue());
                    Assert.assertTrue(entry.getValue().size() <= 4);
                }
            }
        } while (CollectionUtils.isNotEmpty(getSeedIds));

        Assert.assertEquals(loopCount, 2);
        Assert.assertEquals(scanSeeds.size(), seedIds.size());

        Assert.assertTrue(entityRawSeedService.batchCreate(destEnv, TEST_SCAN_TENANT, scanSeeds));
        Thread.sleep(1000L);
        EntityRawSeed rawSeed = entityRawSeedService.get(destEnv, TEST_SCAN_TENANT, TEST_ENTITY, seedIds.get(0));
        Assert.assertNotNull(rawSeed);
        List<EntityRawSeed> rawSeeds = entityRawSeedService.get(destEnv, TEST_SCAN_TENANT, TEST_ENTITY, seedIds);
        Assert.assertEquals(rawSeeds.size(), seedIds.size());

        for(String seedId : seedIds) {
            cleanupTenant(env, TEST_SCAN_TENANT, seedId);
        }
        for(String seedId : seedIds) {
            cleanupTenant(destEnv, TEST_SCAN_TENANT, seedId);
        }
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment")
    private void testSetIfNotExists(EntityMatchEnvironment env) {
        String seedId = "testSetIfNotExists";

        // make sure we don't have this seed
        cleanup(env, seedId);

        EntityRawSeed seed = newSeed(
                seedId, "sfdc_1", "mkt_1", "9999", "google.com", "facebook.com");

        // create successfully because no seed at the moment
        Assert.assertTrue(entityRawSeedService.setIfNotExists(env, TEST_TENANT, seed));
        // should already exists
        Assert.assertFalse(entityRawSeedService.setIfNotExists(env, TEST_TENANT, seed));

        // check the created seed
        EntityRawSeed updatedSeed = entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertNotNull(updatedSeed);
        Assert.assertTrue(equalsDisregardPriority(updatedSeed, seed));

        // cleanup afterwards
        cleanup(env, seedId);
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment")
    private void testUpdateIfNotSet(EntityMatchEnvironment env) {
        String seedId = "testUpdateIfNotSet";

        // make sure we don't have this seed
        cleanup(env, seedId);

        EntityRawSeed seed1 = newSeed(seedId, "sfdc_1", null, "lattice_1", "domain1.com");
        EntityRawSeed result1 = entityRawSeedService.updateIfNotSet(env, TEST_TENANT, seed1);
        // currently no seed exists, so the returned old seed will be null
        Assert.assertNull(result1);
        // check all attributes are updated correctly
        Assert.assertEquals(entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId), seed1);

        EntityRawSeed seed2 = newSeed(seedId, "sfdc_2", "marketo_1", "lattice_2", "domain2.com");
        EntityRawSeed result2 = entityRawSeedService.updateIfNotSet(env, TEST_TENANT, seed2);
        // seed before update should be seed1
        Assert.assertEquals(result2, seed1);
        EntityRawSeed resultAfterUpdate2 = entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        // 1. SFDC ID & lattice Account ID already exists, not updating
        // 2. Marketo ID updated successfully
        // 3. Domains are merged and no duplicate
        Assert.assertNotNull(resultAfterUpdate2);
        Assert.assertEquals(resultAfterUpdate2.getLookupEntries().size(), 4);
        Assert.assertTrue(equalsDisregardPriority(resultAfterUpdate2,
                newSeed(seedId, "sfdc_1", "marketo_1", "lattice_1", "domain1.com", "domain2.com")));

        // cleanup afterwards
        cleanup(env, seedId);
    }

    /*
     * more general test cases for update if not set
     */
    @Test(groups = "functional", dataProvider = "updateIfNotSet")
    private void testUpdateIfNotSet(
            EntityRawSeed currentState, @NotNull EntityRawSeed seedToUpdate,
            @NotNull EntityRawSeed finalState) throws Exception {
        if (currentState != null) {
            Assert.assertEquals(currentState.getId(), seedToUpdate.getId());
        }
        Assert.assertEquals(seedToUpdate.getId(), finalState.getId());

        String seedId = seedToUpdate.getId();
        String entity = seedToUpdate.getEntity();
        for (EntityMatchEnvironment env : EntityMatchEnvironment.values()) {
            // prepare current state
            cleanup(env, seedId);
            if (currentState != null) {
                boolean currStateSet = entityRawSeedService.setIfNotExists(env, TEST_TENANT, currentState);
                Assert.assertTrue(currStateSet);
            }

            // update
            EntityRawSeed seedBeforeUpdate = entityRawSeedService.updateIfNotSet(env, TEST_TENANT, seedToUpdate);
            // cannot check lookup entry because only attributes that we attempt to update will be in the seed
            if (currentState == null) {
                Assert.assertNull(seedBeforeUpdate);
            } else {
                Assert.assertNotNull(seedBeforeUpdate);
                Assert.assertEquals(seedBeforeUpdate.getId(), seedId);
                Assert.assertEquals(seedBeforeUpdate.getEntity(), entity);
            }
            Thread.sleep(500L);

            // check state after update
            EntityRawSeed seedAfterUpdate = entityRawSeedService.get(
                    env, TEST_TENANT, entity, seedId);
            Assert.assertTrue(equalsDisregardPriority(seedAfterUpdate, finalState));

            cleanup(env, seedId);
        }
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment")
    private void testClear(EntityMatchEnvironment env) throws Exception {
        String seedId = "testClear";

        // make sure we don't have this seed
        cleanup(env, seedId);

        EntityRawSeed seed = newSeed(seedId,
                NC_GOOGLE_1, NC_GOOGLE_2, NC_FACEBOOK_1, NC_NETFLIX_1, NC_GOOGLE_3, DC_GOOGLE_2,
                DC_FACEBOOK_2, DUNS_5, DC_FACEBOOK_1, SFDC_1, MKTO_3, DC_FACEBOOK_4);
        boolean seedSet = entityRawSeedService.setIfNotExists(env, TEST_TENANT, seed);
        Assert.assertTrue(seedSet);
        Thread.sleep(500L);

        // check current state
        EntityRawSeed resultAfterUpdate = entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertTrue(equalsDisregardPriority(resultAfterUpdate, seed));

        EntityRawSeed seedToClear = newSeed(seedId,
                NC_GOOGLE_1, NC_GOOGLE_3, NC_GOOGLE_4, DUNS_5, DC_FACEBOOK_1, DC_FACEBOOK_4);
        EntityRawSeed currSeedBeforeClear = entityRawSeedService.clear(env, TEST_TENANT, seedToClear);
        Assert.assertNotNull(currSeedBeforeClear);
        // in clear, we get back the entire existing seed
        Assert.assertTrue(equalsDisregardPriority(currSeedBeforeClear, resultAfterUpdate));
        Thread.sleep(500L);

        // check state after cleared
        EntityRawSeed resultAfterClear = entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertNotNull(resultAfterClear);
        // check the lookup entries left
        Assert.assertTrue(equalsDisregardPriority(
                resultAfterClear,
                newSeed(seedId, NC_GOOGLE_2, NC_FACEBOOK_1, NC_NETFLIX_1,
                        DC_GOOGLE_2, DC_FACEBOOK_2, SFDC_1, MKTO_3)));

        // cleanup afterwards
        cleanup(env, seedId);
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment")
    private void testClearIfEquals(EntityMatchEnvironment env) {
        String seedId = "testClearIfEquals";

        // make sure we don't have this seed
        cleanup(env, seedId);

        EntityRawSeed seed = newSeed(
                seedId, "sfdc_1", null, "lattice_1",
                "domain1.com", "domain2.com");
        EntityRawSeed result = entityRawSeedService.updateIfNotSet(env, TEST_TENANT, seed);
        // currently no seed exists, so the returned old seed will be null
        Assert.assertNull(result);
        // check all attributes are updated correctly
        EntityRawSeed resultAfterUpdate = entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertEquals(resultAfterUpdate, seed);
        // check version
        Assert.assertEquals(resultAfterUpdate.getVersion(), 1);

        EntityRawSeed seedWithWrongVer = newSeed(
                seedId, "sfdc_1", "marketo_1", null, "domain1.com", "domain4.com");
        // wrong version, optimistic locking failed
        Assert.assertThrows(
                IllegalStateException.class,
                () -> entityRawSeedService.clearIfEquals(env, TEST_TENANT, seedWithWrongVer));

        // right vesion, clear succeeded
        EntityRawSeed seedWithRightVer = copyAndSetVersion(seedWithWrongVer, resultAfterUpdate.getVersion());
        entityRawSeedService.clearIfEquals(env, TEST_TENANT, seedWithRightVer);

        EntityRawSeed resultAfterClear = entityRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertNotNull(resultAfterClear);
        // 1. SFDC ID cleared
        // 2. Marketo ID does not exist, so clearing it has no effect
        // 3. One domain exists and is removed from set, the other one does not and is a no-op
        // 4. lattice account ID is not cleared
        Assert.assertTrue(equalsDisregardPriority(
                resultAfterClear,
                newSeed(seedId, null, null, "lattice_1", "domain2.com")));

        // cleanup afterwards
        cleanup(env, seedId);
    }

    @DataProvider(name = "entityMatchEnvironment")
    private Object[][] provideEntityMatchEnv() {
        return new Object[][] {
                { EntityMatchEnvironment.STAGING },
                { EntityMatchEnvironment.SERVING },
        };
    }

    @DataProvider(name = "updateIfNotSet")
    private Object[][] provideUpdateIfNotSetTestData() {
        return new Object[][] {
                /*
                 * Case #1: Update empty/null seed
                 */
                {
                        null, EMPTY, EMPTY,
                },
                {
                        EMPTY, EMPTY, EMPTY,
                },
                // lookup entries
                {
                        TestEntityMatchUtils.changeId(EMPTY, UINS_SEED_ID),
                        newSeed(UINS_SEED_ID, DC_FACEBOOK_1, DC_FACEBOOK_2, DC_GOOGLE_1),
                        newSeed(UINS_SEED_ID, DC_FACEBOOK_1, DC_FACEBOOK_2, DC_GOOGLE_1),
                },
                {
                        null,
                        newSeed(UINS_SEED_ID, DC_FACEBOOK_1, DC_FACEBOOK_2, DC_GOOGLE_1),
                        newSeed(UINS_SEED_ID, DC_FACEBOOK_1, DC_FACEBOOK_2, DC_GOOGLE_1),
                },
                {
                        TestEntityMatchUtils.changeId(EMPTY, UINS_SEED_ID),
                        newSeed(UINS_SEED_ID, DUNS_1, SFDC_5, NC_FACEBOOK_1, DC_GOOGLE_1),
                        newSeed(UINS_SEED_ID, DUNS_1, SFDC_5, NC_FACEBOOK_1, DC_GOOGLE_1),
                },
                {
                        TestEntityMatchUtils.changeId(EMPTY, UINS_SEED_ID),
                        newSeed(UINS_SEED_ID, SFDC_1, MKTO_1, ELOQUA_1),
                        newSeed(UINS_SEED_ID, SFDC_1, MKTO_1, ELOQUA_1),
                },
                {
                        null,
                        newSeed(UINS_SEED_ID, DUNS_3, MKTO_2, ELOQUA_3),
                        newSeed(UINS_SEED_ID, DUNS_3, MKTO_2, ELOQUA_3),
                },
                {
                        null,
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, MKTO_3, DUNS_5),
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, MKTO_3, DUNS_5),
                },
                // extra attributes
                {
                        null,
                        TestEntityMatchUtils.newSeed(UINS_SEED_ID, "key1", "val1", "key2", "val2"),
                        TestEntityMatchUtils.newSeed(UINS_SEED_ID, "key1", "val1", "key2", "val2"),
                },
                /*
                 * Case #2: no conflict with existing seed
                 */
                // adding domain/country, name/country
                {
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, MKTO_3, DUNS_5),
                        newSeed(UINS_SEED_ID, NC_GOOGLE_2, NC_NETFLIX_2, DC_FACEBOOK_1, DC_FACEBOOK_2),
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, MKTO_3, DUNS_5,
                                NC_GOOGLE_2, NC_NETFLIX_2, DC_FACEBOOK_1, DC_FACEBOOK_2),
                },
                {
                        newSeed(UINS_SEED_ID, DUNS_4, MKTO_1),
                        newSeed(UINS_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_2, DC_GOOGLE_3),
                        newSeed(UINS_SEED_ID, DUNS_4, MKTO_1, DC_GOOGLE_1, DC_GOOGLE_2, DC_GOOGLE_3),
                },
                {
                        newSeed(UINS_SEED_ID, SFDC_1, ELOQUA_3),
                        newSeed(UINS_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_2, DC_GOOGLE_3),
                        newSeed(UINS_SEED_ID, SFDC_1, ELOQUA_3, DC_GOOGLE_1, DC_GOOGLE_2, DC_GOOGLE_3),
                },
                {
                        // some overlap
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_GOOGLE_2, NC_FACEBOOK_2),
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_GOOGLE_2, NC_FACEBOOK_1),
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_GOOGLE_2, NC_FACEBOOK_1, NC_FACEBOOK_2),
                },
                // adding duns
                {
                        // name country in current state
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1),
                        newSeed(UINS_SEED_ID, DUNS_1),
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, DUNS_1),
                },
                {
                        // external system ID in current state
                        newSeed(UINS_SEED_ID, MKTO_1, SFDC_5, ELOQUA_3),
                        newSeed(UINS_SEED_ID, DUNS_4),
                        newSeed(UINS_SEED_ID, MKTO_1, SFDC_5, ELOQUA_3, DUNS_4),
                },
                {
                        // domain country in current state
                        newSeed(UINS_SEED_ID, DC_GOOGLE_1, DC_FACEBOOK_2, DC_GOOGLE_2),
                        newSeed(UINS_SEED_ID, DUNS_2),
                        newSeed(UINS_SEED_ID, DC_GOOGLE_1, DC_FACEBOOK_2, DC_GOOGLE_2, DUNS_2),
                },
                {
                        // all types in current state
                        newSeed(UINS_SEED_ID, MKTO_1, DC_GOOGLE_1, NC_FACEBOOK_1, SFDC_5, DC_GOOGLE_2, ELOQUA_4),
                        newSeed(UINS_SEED_ID, DUNS_5),
                        newSeed(UINS_SEED_ID, MKTO_1, DC_GOOGLE_1, NC_FACEBOOK_1, SFDC_5,
                                DC_GOOGLE_2, ELOQUA_4, DUNS_5),
                },
                // adding external system IDs
                {
                        // name country in current state
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1),
                        newSeed(UINS_SEED_ID, SFDC_1),
                        newSeed(UINS_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, SFDC_1),
                },
                {
                        // external system ID in current state
                        newSeed(UINS_SEED_ID, MKTO_1, SFDC_5),
                        newSeed(UINS_SEED_ID, ELOQUA_1), // can still update other system
                        newSeed(UINS_SEED_ID, MKTO_1, SFDC_5, ELOQUA_1),
                },
                {
                        // DUNS in current state
                        newSeed(UINS_SEED_ID, DUNS_5),
                        newSeed(UINS_SEED_ID, SFDC_1, MKTO_2), // can update multiple systems
                        newSeed(UINS_SEED_ID, SFDC_1, MKTO_2, DUNS_5),
                },
                {
                        // domain country in current state
                        newSeed(UINS_SEED_ID, DC_GOOGLE_1, DC_FACEBOOK_2, DC_GOOGLE_2),
                        newSeed(UINS_SEED_ID, SFDC_5, ELOQUA_3, MKTO_1),
                        newSeed(UINS_SEED_ID, DC_GOOGLE_1, DC_FACEBOOK_2, DC_GOOGLE_2, SFDC_5, ELOQUA_3, MKTO_1),
                },
                {
                        // all types in current state
                        newSeed(UINS_SEED_ID, MKTO_1, DC_GOOGLE_1, NC_FACEBOOK_1, SFDC_5, DC_GOOGLE_2, DUNS_5),
                        newSeed(UINS_SEED_ID, ELOQUA_4),
                        newSeed(UINS_SEED_ID, MKTO_1, DC_GOOGLE_1, NC_FACEBOOK_1, SFDC_5,
                                DC_GOOGLE_2, ELOQUA_4, DUNS_5),
                },
                /*
                 * Case #3: has conflict
                 */
                // conflict in DUNS
                {
                        newSeed(UINS_SEED_ID, DUNS_5),
                        newSeed(UINS_SEED_ID, DUNS_1),
                        newSeed(UINS_SEED_ID, DUNS_5), // DUNS_1 not updated
                },
                {
                        newSeed(UINS_SEED_ID, DUNS_4, DC_GOOGLE_2, NC_FACEBOOK_1), // has other lookup entries
                        newSeed(UINS_SEED_ID, DUNS_2),
                        newSeed(UINS_SEED_ID, DUNS_4, DC_GOOGLE_2, NC_FACEBOOK_1), // DUNS_2 not updated
                },
                {
                        newSeed(UINS_SEED_ID, DUNS_4, NC_GOOGLE_2, MKTO_1),
                        newSeed(UINS_SEED_ID, DUNS_2, NC_GOOGLE_1, SFDC_1),
                        // DUNS_2 not updated, SFDC_1 & NC_GOOGLE_1 updated
                        newSeed(UINS_SEED_ID, DUNS_4, NC_GOOGLE_2, NC_GOOGLE_1, SFDC_1, MKTO_1),
                },
                // conflict in external system
                {
                        newSeed(UINS_SEED_ID, SFDC_1),
                        newSeed(UINS_SEED_ID, SFDC_2),
                        newSeed(UINS_SEED_ID, SFDC_1), // SFDC_2 not updated
                },
                {
                        newSeed(UINS_SEED_ID, SFDC_1, MKTO_1, ELOQUA_4, NC_GOOGLE_1, NC_GOOGLE_2, DC_FACEBOOK_2),
                        newSeed(UINS_SEED_ID, SFDC_2, MKTO_3),
                        // SFDC_2 & MKTO_3 not updated
                        newSeed(UINS_SEED_ID, SFDC_1, MKTO_1, ELOQUA_4, NC_GOOGLE_1, NC_GOOGLE_2, DC_FACEBOOK_2),
                },
                {
                        newSeed(UINS_SEED_ID, SFDC_1, MKTO_1, NC_GOOGLE_1),
                        newSeed(UINS_SEED_ID, SFDC_2, MKTO_3, ELOQUA_1, DUNS_1, NC_GOOGLE_2, DC_GOOGLE_2),
                        // SFDC_2, MKTO_3 not updated, DUNS_1, NC_GOOGLE_2, DC_GOOGLE_2, ELOQUA_1 updated
                        newSeed(UINS_SEED_ID, SFDC_1, MKTO_1, NC_GOOGLE_1, ELOQUA_1, DUNS_1, NC_GOOGLE_2, DC_GOOGLE_2),
                },
                // conflict in DUNS & external system
                {
                        newSeed(UINS_SEED_ID, SFDC_1, DUNS_1, DC_GOOGLE_2, NC_GOOGLE_2),
                        newSeed(UINS_SEED_ID, SFDC_2, DUNS_5, NC_FACEBOOK_1),
                        newSeed(UINS_SEED_ID, SFDC_1, DUNS_1, DC_GOOGLE_2, NC_GOOGLE_2, NC_FACEBOOK_1),
                },
        };
    }

    private void cleanup(EntityMatchEnvironment env, String... seedIds) {
        cleanupTenant(env, TEST_TENANT, seedIds);
    }

    private void cleanupTenant(EntityMatchEnvironment env, Tenant tenant, String... seedIds) {
        Arrays.stream(seedIds).forEach(id -> entityRawSeedService.delete(env, tenant, TEST_ENTITY, id));
    }

    /*
     * Create a test raw seed with two external system (SFDC/Marketo), lattice accountID and domain set
     */
    private EntityRawSeed newSeed(String seedId, String sfdcId, String marketoId, String latticeId, String... domains) {
        List<EntityLookupEntry> entries = new ArrayList<>();
        Map<String, String> attributes = new HashMap<>();
        if (sfdcId != null) {
            entries.add(EntityLookupEntryConverter.fromExternalSystem(TEST_ENTITY, EXT_SYSTEM_SFDC, sfdcId));
        }
        if (marketoId != null) {
            entries.add(EntityLookupEntryConverter.fromExternalSystem(TEST_ENTITY, EXT_SYSTEM_MARKETO, marketoId));
        }
        if (domains != null) {
            Arrays.stream(domains).forEach(domain ->
                    entries.add(EntityLookupEntryConverter.fromDomainCountry(TEST_ENTITY, domain, TEST_COUNTRY)));
        }
        if (latticeId != null) {
            attributes.put(DataCloudConstants.LATTICE_ACCOUNT_ID, latticeId);
        }
        return new EntityRawSeed(seedId, TEST_ENTITY, 0, entries, attributes);
    }

    private EntityRawSeed newSeed(String seedId, EntityLookupEntry... entries) {
        return TestEntityMatchUtils.newSeed(seedId, entries);
    }

    /*
     * copy raw seed and set the version
     */
    private EntityRawSeed copyAndSetVersion(EntityRawSeed seed, int version) {
        return new EntityRawSeed(
                seed.getId(), seed.getEntity(), version, seed.getLookupEntries(), seed.getAttributes());
    }
}
