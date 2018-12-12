package com.latticeengines.datacloud.match.service.impl;

import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
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
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
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

/*
 * TODO add more test cases
 * TODO retry on test method to guard against eventual consistency failure, retryAnalyzer is not working for some reason
 */
public class EntityRawSeedServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_SERVING_TABLE = "CDLMatchServingDev_20181126";
    private static final String TEST_STAGING_TABLE = "CDLMatchDev_20181126";
    private static final Tenant TEST_TENANT = getTestTenant();
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final String EXT_SYSTEM_SFDC = "SFDC";
    private static final String EXT_SYSTEM_MARKETO = "MARKETO";
    private static final String TEST_COUNTRY = "USA";

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
    private void testScan() {
        List<String> seedIds = Arrays.asList("testScan1", "testScan2", "testScan3", "testScan4", "testScan5",
                "testScan6", "testScan7");
        List<EntityRawSeed> scanSeeds = new ArrayList<>();
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING;

        // make sure we don't have this seed
        for(String seedId : seedIds) {
            cleanup(env, seedId);
        }

        // create successfully because no seed at the moment
        for (String seedId : seedIds) {
            Assert.assertTrue(entityRawSeedService.createIfNotExists(env, TEST_TENANT, TEST_ENTITY, seedId));
        }
        // should already exists
        List<String> getSeedIds = new ArrayList<>();
        int loopCount = 0;
        do {
            loopCount++;
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(env, TEST_TENANT, TEST_ENTITY, getSeedIds, 2);
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
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(env, TEST_TENANT, TEST_ENTITY, getSeedIds, 3);
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
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(env, TEST_TENANT, TEST_ENTITY, getSeedIds, 4);
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

        for(String seedId : seedIds) {
            cleanup(env, seedId);
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
        Assert.assertTrue(equals(updatedSeed, seed));

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
        Assert.assertTrue(equals(resultAfterUpdate2,
                newSeed(seedId, "sfdc_1", "marketo_1", "lattice_1", "domain1.com", "domain2.com")));

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
        Assert.assertTrue(equals(
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

    private void cleanup(EntityMatchEnvironment env, String... seedIds) {
        Arrays.stream(seedIds).forEach(id -> entityRawSeedService.delete(env, TEST_TENANT, TEST_ENTITY, id));
    }

    private boolean equals(EntityRawSeed seed1, EntityRawSeed seed2) {
        if (seed1 == null && seed2 == null) {
            return true;
        } else if (seed1 == null || seed2 == null) {
            return false;
        }

        Set<EntityLookupEntry> set1 = new HashSet<>(seed1.getLookupEntries());
        Set<EntityLookupEntry> set2 = new HashSet<>(seed1.getLookupEntries());
        return Objects.equals(seed1.getId(), seed2.getId()) && Objects.equals(seed1.getEntity(), seed2.getEntity())
                && Objects.equals(seed1.getAttributes(), seed2.getAttributes())
                && Objects.equals(set1, set2);
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

    /*
     * copy raw seed and set the version
     */
    private EntityRawSeed copyAndSetVersion(EntityRawSeed seed, int version) {
        return new EntityRawSeed(seed.getId(), seed.getEntity(), version, seed.getLookupEntries(), seed.getAttributes());
    }

    private static Tenant getTestTenant() {
        Tenant tenant = new Tenant("raw_seed_service_test_tenant_1");
        tenant.setPid(5798729L);
        return tenant;
    }
}
