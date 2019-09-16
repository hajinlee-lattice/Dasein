package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.equalsDisregardPriority;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.getUpdatedAttributes;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.AE_GOOGLE_1_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.AE_GOOGLE_1_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.AE_GOOGLE_1_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ANP_GOOGLE_1_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ANP_GOOGLE_1_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.CAE_GOOGLE_1_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.CAE_GOOGLE_1_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.CANP_GOOGLE_1_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.CANP_GOOGLE_1_2;
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
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.E_GOOGLE_1_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.E_GOOGLE_1_2;
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
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NP_GOOGLE_1_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NP_GOOGLE_1_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_5;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.Seed.EMPTY;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.STAGING;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.City;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Country;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Domain;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LatticeAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Name;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.State;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class EntityRawSeedServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(EntityRawSeedServiceImplTestNG.class);

    private static final int TEST_NUM_STAGING_SHARDS = 2;
    private static final String TEST_ENTITY = Account.name();
    private static final String EXT_SYSTEM_SFDC = "SFDC";
    private static final String EXT_SYSTEM_MARKETO = "MARKETO";
    private static final String TEST_COUNTRY = "USA";
    private static final String TEST_SEED_ID = "seed_9527";
    private static final int TEST_VERSION_1 = 1;
    private static final int TEST_VERSION_2 = 2;

    @Inject
    private EntityRawSeedServiceImpl entityRawSeedService;

    @Value("${datacloud.match.entity.staging.table}")
    private String stagingTableName;

    @Value("${datacloud.match.entity.serving.table}")
    private String servingTableName;

    @BeforeClass(groups = "functional")
    private void setup() throws Exception {
        EntityMatchConfigurationService configService = Mockito.mock(EntityMatchConfigurationService.class);
        Mockito.when(configService.getNumShards(STAGING)).thenReturn(TEST_NUM_STAGING_SHARDS);
        Mockito.when(configService.getRetryTemplate(Mockito.any())).thenReturn(RetryUtils.getRetryTemplate(3));
        Mockito.when(configService.getTableName(STAGING)).thenReturn(stagingTableName);
        Mockito.when(configService.getTableName(SERVING)).thenReturn(servingTableName);
        FieldUtils.writeField(entityRawSeedService, "entityMatchConfigurationService", configService, true);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testVersion() {
        Tenant tenant = newTestTenant();
        EntityMatchEnvironment env = STAGING;
        EntityRawSeed seed = new EntityRawSeed(UUID.randomUUID().toString(), TEST_ENTITY, true);

        boolean created = entityRawSeedService.setIfNotExists(env, tenant, seed, true, TEST_VERSION_1);
        Assert.assertTrue(created);

        // should be able to retrieve with the same version
        EntityRawSeed result = entityRawSeedService.get(env, tenant, seed.getEntity(), seed.getId(), TEST_VERSION_1);
        Assert.assertTrue(TestEntityMatchUtils.equalsDisregardPriority(result, seed));

        result = entityRawSeedService.get(env, tenant, seed.getEntity(), seed.getId(), TEST_VERSION_2);
        Assert.assertNull(result);

        // no should be able to get with another version after set
        entityRawSeedService.batchCreate(env, tenant, Collections.singletonList(seed), true, TEST_VERSION_2);
        result = entityRawSeedService.get(env, tenant, seed.getEntity(), seed.getId(), TEST_VERSION_2);
        Assert.assertTrue(TestEntityMatchUtils.equalsDisregardPriority(result, seed));
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testCreateIfNotExists(EntityMatchEnvironment env) {
        Tenant tenant = newTestTenant();
        String seedId = TEST_SEED_ID;
        int version = TEST_VERSION_1;

        // create successfully because no seed at the moment
        Assert.assertTrue(entityRawSeedService.createIfNotExists(env, tenant, TEST_ENTITY, seedId, true, version));
        // should already exists
        Assert.assertFalse(entityRawSeedService.createIfNotExists(env, tenant, TEST_ENTITY, seedId, true, version));

        // check the created seed
        EntityRawSeed seed = entityRawSeedService.get(env, tenant, TEST_ENTITY, seedId, version);
        Assert.assertNotNull(seed);
        Assert.assertEquals(seed.getId(), seedId);
        Assert.assertEquals(seed.getEntity(), TEST_ENTITY);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testScanAndBatchCreate() throws InterruptedException {
        Tenant tenant = newTestTenant();
        List<String> seedIds = Arrays.asList("testScan1", "testScan2", "testScan3", "testScan4", "testScan5",
                "testScan6", "testScan7");
        List<EntityRawSeed> scanSeeds = new ArrayList<>();
        EntityMatchEnvironment env = STAGING;
        EntityMatchEnvironment destEnv = SERVING;
        int version = TEST_VERSION_1;

        // create successfully because no seed at the moment
        for (String seedId : seedIds) {
            Assert.assertTrue(entityRawSeedService.createIfNotExists(env, tenant, TEST_ENTITY, seedId, true, version));
        }
        // should already exists
        List<String> getSeedIds = new ArrayList<>();
        int loopCount = 0;
        do {
            loopCount++;
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(
                    env, tenant, TEST_ENTITY, getSeedIds, 2, version);
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
                    env, tenant, TEST_ENTITY, getSeedIds, 3, version);
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
                    env, tenant, TEST_ENTITY, getSeedIds, 4, version);
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

        Assert.assertTrue(entityRawSeedService.batchCreate(destEnv, tenant, scanSeeds, true, version));
        Thread.sleep(1000L);
        EntityRawSeed rawSeed = entityRawSeedService.get(destEnv, tenant, TEST_ENTITY, seedIds.get(0), version);
        Assert.assertNotNull(rawSeed);
        List<EntityRawSeed> rawSeeds = entityRawSeedService.get(destEnv, tenant, TEST_ENTITY, seedIds, version);
        Assert.assertEquals(rawSeeds.size(), seedIds.size());
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testSetIfNotExists(EntityMatchEnvironment env) {
        String seedId = TEST_SEED_ID;
        Tenant tenant = newTestTenant();
        int version = TEST_VERSION_1;

        EntityRawSeed seed = newSeed(
                seedId, "sfdc_1", "mkt_1", "9999", "google.com", "facebook.com");

        // create successfully because no seed at the moment
        Assert.assertTrue(entityRawSeedService.setIfNotExists(env, tenant, seed, true, version));
        // should already exists
        Assert.assertFalse(entityRawSeedService.setIfNotExists(env, tenant, seed, true, version));

        // check the created seed
        EntityRawSeed updatedSeed = entityRawSeedService.get(env, tenant, TEST_ENTITY, seedId, version);
        Assert.assertNotNull(updatedSeed);
        Assert.assertTrue(equalsDisregardPriority(updatedSeed, seed));
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testUpdateIfNotSet(EntityMatchEnvironment env) {
        String seedId = TEST_SEED_ID;
        Tenant tenant = newTestTenant();
        int version = TEST_VERSION_1;

        EntityRawSeed seed1 = newSeed(seedId, "sfdc_1", null, "lattice_1", "domain1.com");
        EntityRawSeed result1 = entityRawSeedService.updateIfNotSet(env, tenant, seed1, true, version);
        // currently no seed exists, so the returned old seed will be null
        Assert.assertNull(result1);
        // check all attributes are updated correctly
        Assert.assertEquals(entityRawSeedService.get(env, tenant, TEST_ENTITY, seedId, version), seed1);

        EntityRawSeed seed2 = newSeed(seedId, "sfdc_2", "marketo_1", "lattice_2", "domain2.com");
        EntityRawSeed result2 = entityRawSeedService.updateIfNotSet(env, tenant, seed2, true, version);
        // seed before update should be seed1
        Assert.assertEquals(result2, seed1);
        EntityRawSeed resultAfterUpdate2 = entityRawSeedService.get(env, tenant, TEST_ENTITY, seedId, version);
        // 1. SFDC ID & lattice Account ID already exists, not updating
        // 2. Marketo ID updated successfully
        // 3. Domains are merged and no duplicate
        Assert.assertNotNull(resultAfterUpdate2);
        Assert.assertEquals(resultAfterUpdate2.getLookupEntries().size(), 4);
        Assert.assertTrue(equalsDisregardPriority(resultAfterUpdate2,
                newSeed(seedId, "sfdc_1", "marketo_1", "lattice_1", "domain1.com", "domain2.com")));
    }

    /*
     * more general test cases for update if not set
     */
    @Test(groups = "functional", dataProvider = "updateIfNotSet", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testUpdateIfNotSet(
            EntityRawSeed currentState, @NotNull EntityRawSeed seedToUpdate,
            @NotNull EntityRawSeed finalState) {
        int version = TEST_VERSION_1;
        if (currentState != null) {
            Assert.assertEquals(currentState.getId(), seedToUpdate.getId());
        }
        Assert.assertEquals(seedToUpdate.getId(), finalState.getId());

        String seedId = seedToUpdate.getId();
        String entity = seedToUpdate.getEntity();
        for (EntityMatchEnvironment env : EntityMatchEnvironment.values()) {
            Tenant tenant = newTestTenant();
            if (currentState != null) {
                boolean currStateSet = entityRawSeedService.setIfNotExists(env, tenant, currentState, true, version);
                Assert.assertTrue(currStateSet);
            }

            // update
            EntityRawSeed seedBeforeUpdate = entityRawSeedService.updateIfNotSet(env, tenant, seedToUpdate, true,
                    version);
            // cannot check lookup entry because only attributes that we attempt to update will be in the seed
            if (currentState == null) {
                Assert.assertNull(seedBeforeUpdate);
            } else {
                Assert.assertNotNull(seedBeforeUpdate);
                Assert.assertEquals(seedBeforeUpdate.getId(), seedId);
                Assert.assertEquals(seedBeforeUpdate.getEntity(), entity);
            }
            log.info(String.format(
                    "Waiting for update to take effect for env = %s, seedToUpdate = %s", env, seedToUpdate));

            // check state after update
            EntityRawSeed seedAfterUpdate = entityRawSeedService.get(
                    env, tenant, entity, seedId, version);
            Assert.assertTrue(equalsDisregardPriority(seedAfterUpdate, finalState));
        }
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testClear(EntityMatchEnvironment env) {
        String seedId = TEST_SEED_ID;
        Tenant tenant = newTestTenant();
        int version = TEST_VERSION_1;

        EntityRawSeed seed = newSeed(seedId,
                NC_GOOGLE_1, NC_GOOGLE_2, NC_FACEBOOK_1, NC_NETFLIX_1, NC_GOOGLE_3, DC_GOOGLE_2,
                DC_FACEBOOK_2, DUNS_5, DC_FACEBOOK_1, SFDC_1, MKTO_3, DC_FACEBOOK_4);
        boolean seedSet = entityRawSeedService.setIfNotExists(env, tenant, seed, true, version);
        Assert.assertTrue(seedSet);

        // check current state
        EntityRawSeed resultAfterUpdate = entityRawSeedService.get(env, tenant, TEST_ENTITY, seedId, version);
        Assert.assertTrue(equalsDisregardPriority(resultAfterUpdate, seed));

        EntityRawSeed seedToClear = newSeed(seedId,
                NC_GOOGLE_1, NC_GOOGLE_3, NC_GOOGLE_4, DUNS_5, DC_FACEBOOK_1, DC_FACEBOOK_4);
        EntityRawSeed currSeedBeforeClear = entityRawSeedService.clear(env, tenant, seedToClear, version);
        Assert.assertNotNull(currSeedBeforeClear);
        // in clear, we get back the entire existing seed
        Assert.assertTrue(equalsDisregardPriority(currSeedBeforeClear, resultAfterUpdate));

        // check state after cleared
        EntityRawSeed resultAfterClear = entityRawSeedService.get(env, tenant, TEST_ENTITY, seedId, version);
        Assert.assertNotNull(resultAfterClear);
        // check the lookup entries left
        Assert.assertTrue(equalsDisregardPriority(
                resultAfterClear,
                newSeed(seedId, NC_GOOGLE_2, NC_FACEBOOK_1, NC_NETFLIX_1,
                        DC_GOOGLE_2, DC_FACEBOOK_2, SFDC_1, MKTO_3)));
    }

    @Test(groups = "functional", dataProvider = "entityMatchEnvironment", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testClearIfEquals(EntityMatchEnvironment env) {
        String seedId = TEST_SEED_ID;
        Tenant tenant = newTestTenant();
        int version = TEST_VERSION_1;

        EntityRawSeed seed = newSeed(
                seedId, "sfdc_1", null, "lattice_1",
                "domain1.com", "domain2.com");
        EntityRawSeed result = entityRawSeedService.updateIfNotSet(env, tenant, seed, true, version);
        // currently no seed exists, so the returned old seed will be null
        Assert.assertNull(result);
        // check all attributes are updated correctly
        EntityRawSeed resultAfterUpdate = entityRawSeedService.get(env, tenant, TEST_ENTITY, seedId, version);
        Assert.assertEquals(resultAfterUpdate, seed);
        // check version
        Assert.assertEquals(resultAfterUpdate.getVersion(), 1);

        EntityRawSeed seedWithWrongVer = newSeed(
                seedId, "sfdc_1", "marketo_1", null, "domain1.com", "domain4.com");
        // wrong version, optimistic locking failed
        Assert.assertThrows(
                IllegalStateException.class,
                () -> entityRawSeedService.clearIfEquals(env, tenant, seedWithWrongVer, version));

        // right vesion, clear succeeded
        EntityRawSeed seedWithRightVer = copyAndSetVersion(seedWithWrongVer, resultAfterUpdate.getVersion());
        entityRawSeedService.clearIfEquals(env, tenant, seedWithRightVer, version);

        EntityRawSeed resultAfterClear = entityRawSeedService.get(env, tenant, TEST_ENTITY, seedId, version);
        Assert.assertNotNull(resultAfterClear);
        // 1. SFDC ID cleared
        // 2. Marketo ID does not exist, so clearing it has no effect
        // 3. One domain exists and is removed from set, the other one does not and is a no-op
        // 4. lattice account ID is not cleared
        Assert.assertTrue(equalsDisregardPriority(
                resultAfterClear,
                newSeed(seedId, null, null, "lattice_1", "domain2.com")));
    }

    @Test(groups = "functional", dataProvider = "serdeTestData", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testSerde(EntityRawSeed seed) {
        int version = TEST_VERSION_1;
        for (EntityMatchEnvironment env : EntityMatchEnvironment.values()) {
            Tenant tenant = newTestTenant();
            // set the seed
            boolean setSucceeded = entityRawSeedService.setIfNotExists(env, tenant, seed, true, version);
            Assert.assertTrue(setSucceeded,
                    String.format("Seed(ID=%s) should not exist before the test", seed.getId()));
            EntityRawSeed result = entityRawSeedService.get(env, tenant, TEST_ENTITY, seed.getId(), version);
            Assert.assertNotNull(result, String.format("Seed(ID=%s) should not be null", seed.getId()));
            Assert.assertTrue(equalsDisregardPriority(seed, result), String
                    .format("Retrieved seed=%s does not match expected seed=%s in env=%s", result, seed, env.name()));
        }
    }

    @Test(groups = "functional", dataProvider = "updateAttributes", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testUpdateAttributes(String entity, String[] currAttrs, String[] attrsToUpdate) {
        int version = TEST_VERSION_1;
        String seedId = UUID.randomUUID().toString();
        Tenant tenant = new Tenant(EntityRawSeedServiceImplTestNG.class.getSimpleName() + UUID.randomUUID().toString());
        EntityRawSeed currState = currAttrs == null ? null
                : TestEntityMatchUtils.newSeedFromAttrs(seedId, entity, currAttrs);
        EntityRawSeed seedToUpdate = TestEntityMatchUtils.newSeedFromAttrs(seedId, entity, attrsToUpdate);

        for (EntityMatchEnvironment env : EntityMatchEnvironment.values()) {
            if (currState != null) {
                boolean setSucceeded = entityRawSeedService.setIfNotExists(env, tenant, currState, true, version);
                Assert.assertTrue(setSucceeded, String.format("Seed(ID=%s,entity=%s) should not exist in tenant=%s",
                        seedId, entity, tenant.getId()));
            }

            // update attributes
            entityRawSeedService.updateIfNotSet(env, tenant, seedToUpdate, true, version);

            EntityRawSeed finalState = entityRawSeedService.get(env, tenant, entity, seedId, version);
            Assert.assertNotNull(finalState,
                    String.format("Seed(ID=%s,entity=%s) should exist in tenant=%s", seedId, entity, tenant.getId()));
            Map<String, String> expectedFinalAttributes = getUpdatedAttributes(currState, seedToUpdate);
            Assert.assertEquals(finalState.getAttributes(), expectedFinalAttributes, String.format(
                    "Attributes in the final state does not match the expected result. Seed(ID=%s,entity=%s), tenant=%s",
                    seedId, entity, tenant.getId()));
        }
    }

    @DataProvider(name = "entityMatchEnvironment", parallel = true)
    private Object[][] provideEntityMatchEnv() {
        return new Object[][] {
                { STAGING }, { SERVING },
        };
    }

    @DataProvider(name = "serdeTestData", parallel = true)
    private Object[][] provideSerdeTestData() {
        // TODO add account lookup keys as well, only testing contact lookup keys first
        return new Object[][] { //
                // Account Entity ID + Email
                { newSeed(TEST_SEED_ID, AE_GOOGLE_1_1) }, //
                { newSeed(TEST_SEED_ID, AE_GOOGLE_1_2) }, //
                { newSeed(TEST_SEED_ID, AE_GOOGLE_1_1, AE_GOOGLE_1_2, AE_GOOGLE_1_3) }, //
                // Account Entity ID + Name + Phone Number
                { newSeed(TEST_SEED_ID, ANP_GOOGLE_1_1) }, //
                { newSeed(TEST_SEED_ID, ANP_GOOGLE_1_1, ANP_GOOGLE_1_2) }, //
                // Customer Account ID + Email
                { newSeed(TEST_SEED_ID, CAE_GOOGLE_1_2) }, //
                { newSeed(TEST_SEED_ID, CAE_GOOGLE_1_1, CAE_GOOGLE_1_2) }, //
                // Customer Account ID + Name + Phone Number
                { newSeed(TEST_SEED_ID, CANP_GOOGLE_1_1) }, //
                { newSeed(TEST_SEED_ID, CANP_GOOGLE_1_1, CANP_GOOGLE_1_2) }, //
                // Email
                { newSeed(TEST_SEED_ID, E_GOOGLE_1_1) }, //
                { newSeed(TEST_SEED_ID, E_GOOGLE_1_1, E_GOOGLE_1_2) }, //
                // Name + Phone Number
                { newSeed(TEST_SEED_ID, NP_GOOGLE_1_2) }, //
                { newSeed(TEST_SEED_ID, NP_GOOGLE_1_1, NP_GOOGLE_1_2) }, //
                // Mixing all contact lookup keys
                { newSeed(TEST_SEED_ID, NP_GOOGLE_1_1, AE_GOOGLE_1_2, CANP_GOOGLE_1_1) }, //
                { newSeed(TEST_SEED_ID, CANP_GOOGLE_1_2, AE_GOOGLE_1_2, CANP_GOOGLE_1_1, ANP_GOOGLE_1_1,
                        E_GOOGLE_1_1) }, //
                { newSeed(TEST_SEED_ID, NP_GOOGLE_1_2, AE_GOOGLE_1_2, AE_GOOGLE_1_1, ANP_GOOGLE_1_1, E_GOOGLE_1_2,
                        CAE_GOOGLE_1_2) }, //
                { newSeed(TEST_SEED_ID, E_GOOGLE_1_2, ANP_GOOGLE_1_1, CAE_GOOGLE_1_2) }, //
        }; //
    }

    @DataProvider(name = "updateAttributes", parallel = true)
    private Object[][] updateAttributesTestData() {
        return new Object[][] { //
                // lattice account ID attribute
                { Account.name(), new String[] { LatticeAccountId.name(), "ldc_001" },
                        new String[] { LatticeAccountId.name(), "ldc_002" } }, //
                { Account.name(), new String[] { LatticeAccountId.name(), "ldc_001" },
                        new String[] { LatticeAccountId.name(), "ldc_001" } }, //
                { Account.name(), new String[0], new String[] { LatticeAccountId.name(), "ldc_001" } }, //
                { Contact.name(), new String[] { LatticeAccountId.name(), "ldc_001" },
                        new String[] { LatticeAccountId.name(), "ldc_002" } }, //
                { Contact.name(), new String[] { LatticeAccountId.name(), "ldc_001" },
                        new String[] { LatticeAccountId.name(), "ldc_001" } }, //
                { Contact.name(), new String[0], new String[] { LatticeAccountId.name(), "ldc_001" } }, //
                // account attributes in contact match
                { Contact.name(), new String[] { Name.name(), "Google", State.name(), "CA" },
                        new String[] { Name.name(), null, Domain.name(), "google.com", State.name(), "WA" } }, //
                { Contact.name(), new String[] { Name.name(), "Google", State.name(), "CA" },
                        new String[] { Name.name(), "Facebook", City.name(), "San Mateo" } }, //
                { Contact.name(),
                        new String[] { Name.name(), "Google", State.name(), "CA", Country.name(), "USA", Domain.name(),
                                "google.com" },
                        new String[] { Domain.name(), "fb.com", Name.name(), "Facebook", State.name(), "CA" } }, //
                { Contact.name(), new String[] { CustomerAccountId.name(), "ca123", AccountId.name(), "a11" },
                        new String[] { CustomerAccountId.name(), "ca555", Country.name(), "USA", AccountId.name(),
                                "a11" } }, //
                { Contact.name(), new String[] { CustomerAccountId.name(), "ca123", AccountId.name(), "a11" },
                        new String[] { CustomerAccountId.name(), null, AccountId.name(), null } }, //
        };
    }

    @DataProvider(name = "updateIfNotSet", parallel = true)
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
                        TestEntityMatchUtils.changeId(EMPTY, TEST_SEED_ID),
                        newSeed(TEST_SEED_ID, DC_FACEBOOK_1, DC_FACEBOOK_2, DC_GOOGLE_1),
                        newSeed(TEST_SEED_ID, DC_FACEBOOK_1, DC_FACEBOOK_2, DC_GOOGLE_1),
                },
                {
                        null,
                        newSeed(TEST_SEED_ID, DC_FACEBOOK_1, DC_FACEBOOK_2, DC_GOOGLE_1),
                        newSeed(TEST_SEED_ID, DC_FACEBOOK_1, DC_FACEBOOK_2, DC_GOOGLE_1),
                },
                {
                        TestEntityMatchUtils.changeId(EMPTY, TEST_SEED_ID),
                        newSeed(TEST_SEED_ID, DUNS_1, SFDC_5, NC_FACEBOOK_1, DC_GOOGLE_1),
                        newSeed(TEST_SEED_ID, DUNS_1, SFDC_5, NC_FACEBOOK_1, DC_GOOGLE_1),
                },
                {
                        TestEntityMatchUtils.changeId(EMPTY, TEST_SEED_ID),
                        newSeed(TEST_SEED_ID, SFDC_1, MKTO_1, ELOQUA_1),
                        newSeed(TEST_SEED_ID, SFDC_1, MKTO_1, ELOQUA_1),
                },
                {
                        null,
                        newSeed(TEST_SEED_ID, DUNS_3, MKTO_2, ELOQUA_3),
                        newSeed(TEST_SEED_ID, DUNS_3, MKTO_2, ELOQUA_3),
                },
                {
                        null,
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, MKTO_3, DUNS_5),
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, MKTO_3, DUNS_5),
                },
                // extra attributes
                {
                        null,
                        TestEntityMatchUtils.newSeed(TEST_SEED_ID, "key1", "val1", "key2", "val2"),
                        TestEntityMatchUtils.newSeed(TEST_SEED_ID, "key1", "val1", "key2", "val2"),
                },
                /*
                 * Case #2: no conflict with existing seed
                 */
                // adding domain/country, name/country
                {
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, MKTO_3, DUNS_5),
                        newSeed(TEST_SEED_ID, NC_GOOGLE_2, NC_NETFLIX_2, DC_FACEBOOK_1, DC_FACEBOOK_2),
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, MKTO_3, DUNS_5,
                                NC_GOOGLE_2, NC_NETFLIX_2, DC_FACEBOOK_1, DC_FACEBOOK_2),
                },
                {
                        newSeed(TEST_SEED_ID, DUNS_4, MKTO_1),
                        newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_2, DC_GOOGLE_3),
                        newSeed(TEST_SEED_ID, DUNS_4, MKTO_1, DC_GOOGLE_1, DC_GOOGLE_2, DC_GOOGLE_3),
                },
                {
                        newSeed(TEST_SEED_ID, SFDC_1, ELOQUA_3),
                        newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_2, DC_GOOGLE_3),
                        newSeed(TEST_SEED_ID, SFDC_1, ELOQUA_3, DC_GOOGLE_1, DC_GOOGLE_2, DC_GOOGLE_3),
                },
                {
                        // some overlap
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_GOOGLE_2, NC_FACEBOOK_2),
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_GOOGLE_2, NC_FACEBOOK_1),
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_GOOGLE_2, NC_FACEBOOK_1, NC_FACEBOOK_2),
                },
                // adding duns
                {
                        // name country in current state
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1), newSeed(TEST_SEED_ID, DUNS_1),
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, DUNS_1),
                },
                {
                        // external system ID in current state
                        newSeed(TEST_SEED_ID, MKTO_1, SFDC_5, ELOQUA_3), newSeed(TEST_SEED_ID, DUNS_4),
                        newSeed(TEST_SEED_ID, MKTO_1, SFDC_5, ELOQUA_3, DUNS_4),
                },
                {
                        // domain country in current state
                        newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_FACEBOOK_2, DC_GOOGLE_2), newSeed(TEST_SEED_ID, DUNS_2),
                        newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_FACEBOOK_2, DC_GOOGLE_2, DUNS_2),
                },
                {
                        // all types in current state
                        newSeed(TEST_SEED_ID, MKTO_1, DC_GOOGLE_1, NC_FACEBOOK_1, SFDC_5, DC_GOOGLE_2, ELOQUA_4),
                        newSeed(TEST_SEED_ID, DUNS_5),
                        newSeed(TEST_SEED_ID, MKTO_1, DC_GOOGLE_1, NC_FACEBOOK_1, SFDC_5,
                                DC_GOOGLE_2, ELOQUA_4, DUNS_5),
                },
                // adding external system IDs
                {
                        // name country in current state
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1), newSeed(TEST_SEED_ID, SFDC_1),
                        newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_NETFLIX_1, SFDC_1),
                },
                {
                        // external system ID in current state
                        newSeed(TEST_SEED_ID, MKTO_1, SFDC_5), newSeed(TEST_SEED_ID, ELOQUA_1),
                        // can still update other system
                        newSeed(TEST_SEED_ID, MKTO_1, SFDC_5, ELOQUA_1),
                },
                {
                        // DUNS in current state
                        newSeed(TEST_SEED_ID, DUNS_5), newSeed(TEST_SEED_ID, SFDC_1, MKTO_2),
                        // can update multiple systems
                        newSeed(TEST_SEED_ID, SFDC_1, MKTO_2, DUNS_5),
                },
                {
                        // domain country in current state
                        newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_FACEBOOK_2, DC_GOOGLE_2),
                        newSeed(TEST_SEED_ID, SFDC_5, ELOQUA_3, MKTO_1),
                        newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_FACEBOOK_2, DC_GOOGLE_2, SFDC_5, ELOQUA_3, MKTO_1),
                },
                {
                        // all types in current state
                        newSeed(TEST_SEED_ID, MKTO_1, DC_GOOGLE_1, NC_FACEBOOK_1, SFDC_5, DC_GOOGLE_2, DUNS_5),
                        newSeed(TEST_SEED_ID, ELOQUA_4),
                        newSeed(TEST_SEED_ID, MKTO_1, DC_GOOGLE_1, NC_FACEBOOK_1, SFDC_5,
                                DC_GOOGLE_2, ELOQUA_4, DUNS_5),
                },
                /*
                 * Case #3: has conflict
                 */
                // conflict in DUNS
                {
                        // DUNS_1 not updated
                        newSeed(TEST_SEED_ID, DUNS_5), newSeed(TEST_SEED_ID, DUNS_1), newSeed(TEST_SEED_ID, DUNS_5),
                },
                {
                        newSeed(TEST_SEED_ID, DUNS_4, DC_GOOGLE_2, NC_FACEBOOK_1), // has other lookup entries
                        // DUNS_2 not updated
                        newSeed(TEST_SEED_ID, DUNS_2), newSeed(TEST_SEED_ID, DUNS_4, DC_GOOGLE_2, NC_FACEBOOK_1), //
                },
                {
                        newSeed(TEST_SEED_ID, DUNS_4, NC_GOOGLE_2, MKTO_1),
                        newSeed(TEST_SEED_ID, DUNS_2, NC_GOOGLE_1, SFDC_1),
                        // DUNS_2 not updated, SFDC_1 & NC_GOOGLE_1 updated
                        newSeed(TEST_SEED_ID, DUNS_4, NC_GOOGLE_2, NC_GOOGLE_1, SFDC_1, MKTO_1),
                },
                // conflict in external system
                {
                        // SFDC_2 not updated
                        newSeed(TEST_SEED_ID, SFDC_1), newSeed(TEST_SEED_ID, SFDC_2), newSeed(TEST_SEED_ID, SFDC_1), //
                },
                {
                        newSeed(TEST_SEED_ID, SFDC_1, MKTO_1, ELOQUA_4, NC_GOOGLE_1, NC_GOOGLE_2, DC_FACEBOOK_2),
                        newSeed(TEST_SEED_ID, SFDC_2, MKTO_3),
                        // SFDC_2 & MKTO_3 not updated
                        newSeed(TEST_SEED_ID, SFDC_1, MKTO_1, ELOQUA_4, NC_GOOGLE_1, NC_GOOGLE_2, DC_FACEBOOK_2),
                },
                {
                        newSeed(TEST_SEED_ID, SFDC_1, MKTO_1, NC_GOOGLE_1),
                        newSeed(TEST_SEED_ID, SFDC_2, MKTO_3, ELOQUA_1, DUNS_1, NC_GOOGLE_2, DC_GOOGLE_2),
                        // SFDC_2, MKTO_3 not updated, DUNS_1, NC_GOOGLE_2, DC_GOOGLE_2, ELOQUA_1 updated
                        newSeed(TEST_SEED_ID, SFDC_1, MKTO_1, NC_GOOGLE_1, ELOQUA_1, DUNS_1, NC_GOOGLE_2,
                                DC_GOOGLE_2),
                },
                // conflict in DUNS & external system
                {
                        newSeed(TEST_SEED_ID, SFDC_1, DUNS_1, DC_GOOGLE_2, NC_GOOGLE_2),
                        newSeed(TEST_SEED_ID, SFDC_2, DUNS_5, NC_FACEBOOK_1),
                        newSeed(TEST_SEED_ID, SFDC_1, DUNS_1, DC_GOOGLE_2, NC_GOOGLE_2, NC_FACEBOOK_1),
                },
        };
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

    private Tenant newTestTenant() {
        return new Tenant(EntityRawSeedServiceImplTestNG.class.getSimpleName() + "_" + UUID.randomUUID().toString());
    }
}
