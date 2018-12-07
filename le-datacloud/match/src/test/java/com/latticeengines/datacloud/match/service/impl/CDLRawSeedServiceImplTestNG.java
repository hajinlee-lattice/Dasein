package com.latticeengines.datacloud.match.service.impl;

import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
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
public class CDLRawSeedServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_SERVING_TABLE = "CDLMatchServingDev_20181126";
    private static final String TEST_STAGING_TABLE = "CDLMatchDev_20181126";
    private static final Tenant TEST_TENANT = getTestTenant();
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final String EXT_SYSTEM_SFDC = "SFDC";
    private static final String EXT_SYSTEM_MARKETO = "MARKETO";
    private static final String TEST_COUNTRY = "USA";

    @Inject
    @InjectMocks
    private CDLRawSeedServiceImpl cdlRawSeedService;

    @Mock
    private CDLConfigurationServiceImpl cdlConfigurationService;

    @BeforeClass(groups = "functional")
    private void setup() {
        MockitoAnnotations.initMocks(this);
        cdlConfigurationService.setStagingTableName(TEST_STAGING_TABLE);
        cdlConfigurationService.setServingTableName(TEST_SERVING_TABLE);
    }

    @Test(groups = "functional", dataProvider = "cdlMatchEnvironment")
    private void testCreateIfNotExists(CDLMatchEnvironment env) {
        String seedId = "testCreateIfNotExists";

        // make sure we don't have this seed
        cleanup(env, seedId);

        // create successfully because no seed at the moment
        Assert.assertTrue(cdlRawSeedService.createIfNotExists(env, TEST_TENANT, TEST_ENTITY, seedId));
        // should already exists
        Assert.assertFalse(cdlRawSeedService.createIfNotExists(env, TEST_TENANT, TEST_ENTITY, seedId));

        // check the created seed
        CDLRawSeed seed = cdlRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertNotNull(seed);
        Assert.assertEquals(seed.getId(), seedId);
        Assert.assertEquals(seed.getEntity(), TEST_ENTITY);

        // cleanup afterwards
        cleanup(env, seedId);
    }

    @Test(groups = "functional", dataProvider = "cdlMatchEnvironment")
    private void testSetIfNotExists(CDLMatchEnvironment env) {
        String seedId = "testSetIfNotExists";

        // make sure we don't have this seed
        cleanup(env, seedId);

        CDLRawSeed seed = newSeed(
                seedId, "sfdc_1", "mkt_1", "9999", "google.com", "facebook.com");

        // create successfully because no seed at the moment
        Assert.assertTrue(cdlRawSeedService.setIfNotExists(env, TEST_TENANT, seed));
        // should already exists
        Assert.assertFalse(cdlRawSeedService.setIfNotExists(env, TEST_TENANT, seed));

        // check the created seed
        CDLRawSeed updatedSeed = cdlRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertNotNull(updatedSeed);
        Assert.assertTrue(equals(updatedSeed, seed));

        // cleanup afterwards
        cleanup(env, seedId);
    }

    @Test(groups = "functional", dataProvider = "cdlMatchEnvironment")
    private void testUpdateIfNotSet(CDLMatchEnvironment env) {
        String seedId = "testUpdateIfNotSet";

        // make sure we don't have this seed
        cleanup(env, seedId);

        CDLRawSeed seed1 = newSeed(seedId, "sfdc_1", null, "lattice_1", "domain1.com");
        CDLRawSeed result1 = cdlRawSeedService.updateIfNotSet(env, TEST_TENANT, seed1);
        // currently no seed exists, so the returned old seed will be null
        Assert.assertNull(result1);
        // check all attributes are updated correctly
        Assert.assertEquals(cdlRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId), seed1);

        CDLRawSeed seed2 = newSeed(seedId, "sfdc_2", "marketo_1", "lattice_2", "domain2.com");
        CDLRawSeed result2 = cdlRawSeedService.updateIfNotSet(env, TEST_TENANT, seed2);
        // seed before update should be seed1
        Assert.assertEquals(result2, seed1);
        CDLRawSeed resultAfterUpdate2 = cdlRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
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

    @Test(groups = "functional", dataProvider = "cdlMatchEnvironment")
    private void testClearIfEquals(CDLMatchEnvironment env) {
        String seedId = "testClearIfEquals";

        // make sure we don't have this seed
        cleanup(env, seedId);

        CDLRawSeed seed = newSeed(
                seedId, "sfdc_1", null, "lattice_1",
                "domain1.com", "domain2.com");
        CDLRawSeed result = cdlRawSeedService.updateIfNotSet(env, TEST_TENANT, seed);
        // currently no seed exists, so the returned old seed will be null
        Assert.assertNull(result);
        // check all attributes are updated correctly
        CDLRawSeed resultAfterUpdate = cdlRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
        Assert.assertEquals(resultAfterUpdate, seed);
        // check version
        Assert.assertEquals(resultAfterUpdate.getVersion(), 1);

        CDLRawSeed seedWithWrongVer = newSeed(
                seedId, "sfdc_1", "marketo_1", null, "domain1.com", "domain4.com");
        // wrong version, optimistic locking failed
        Assert.assertThrows(
                IllegalStateException.class,
                () -> cdlRawSeedService.clearIfEquals(env, TEST_TENANT, seedWithWrongVer));

        // right vesion, clear succeeded
        CDLRawSeed seedWithRightVer = copyAndSetVersion(seedWithWrongVer, resultAfterUpdate.getVersion());
        cdlRawSeedService.clearIfEquals(env, TEST_TENANT, seedWithRightVer);

        CDLRawSeed resultAfterClear = cdlRawSeedService.get(env, TEST_TENANT, TEST_ENTITY, seedId);
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

    @DataProvider(name = "cdlMatchEnvironment")
    private Object[][] provideCDLMatchEnv() {
        return new Object[][] {
                { CDLMatchEnvironment.STAGING },
                { CDLMatchEnvironment.SERVING },
        };
    }

    private void cleanup(CDLMatchEnvironment env, String... seedIds) {
        Arrays.stream(seedIds).forEach(id -> cdlRawSeedService.delete(env, TEST_TENANT, TEST_ENTITY, id));
    }

    private boolean equals(CDLRawSeed seed1, CDLRawSeed seed2) {
        if (seed1 == null && seed2 == null) {
            return true;
        } else if (seed1 == null || seed2 == null) {
            return false;
        }

        Set<CDLLookupEntry> set1 = new HashSet<>(seed1.getLookupEntries());
        Set<CDLLookupEntry> set2 = new HashSet<>(seed1.getLookupEntries());
        return Objects.equals(seed1.getId(), seed2.getId()) && Objects.equals(seed1.getEntity(), seed2.getEntity())
                && Objects.equals(seed1.getAttributes(), seed2.getAttributes())
                && Objects.equals(set1, set2);
    }

    /*
     * Create a test raw seed with two external system (SFDC/Marketo), lattice accountID and domain set
     */
    private CDLRawSeed newSeed(String seedId, String sfdcId, String marketoId, String latticeId, String... domains) {
        List<CDLLookupEntry> entries = new ArrayList<>();
        Map<String, String> attributes = new HashMap<>();
        if (sfdcId != null) {
            entries.add(CDLLookupEntryConverter.fromExternalSystem(TEST_ENTITY, EXT_SYSTEM_SFDC, sfdcId));
        }
        if (marketoId != null) {
            entries.add(CDLLookupEntryConverter.fromExternalSystem(TEST_ENTITY, EXT_SYSTEM_MARKETO, marketoId));
        }
        if (domains != null) {
            Arrays.stream(domains).forEach(domain ->
                    entries.add(CDLLookupEntryConverter.fromDomainCountry(TEST_ENTITY, domain, TEST_COUNTRY)));
        }
        if (latticeId != null) {
            attributes.put(DataCloudConstants.LATTICE_ACCOUNT_ID, latticeId);
        }
        return new CDLRawSeed(seedId, TEST_ENTITY, 0, entries, attributes);
    }

    /*
     * copy raw seed and set the version
     */
    private CDLRawSeed copyAndSetVersion(CDLRawSeed seed, int version) {
        return new CDLRawSeed(seed.getId(), seed.getEntity(), version, seed.getLookupEntries(), seed.getAttributes());
    }

    private static Tenant getTestTenant() {
        Tenant tenant = new Tenant("raw_seed_service_test_tenant_1");
        tenant.setPid(5798729L);
        return tenant;
    }
}
