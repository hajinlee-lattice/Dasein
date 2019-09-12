package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.newSeed;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_NETFLIX_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_5;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_5;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchMetricService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;

public class EntityMatchInternalServiceImplUnitTestNG {
    private static final String TGEFA_SEED_ID = "testGetLookupEntriesFailedToAssociate";
    private static final String MLES_SEED_ID = "testMapLookupEntriesToSeed";

    @Test(groups = "unit", dataProvider = "getLookupEntriesFailedToAssociate")
    private void testGetLookupEntriesFailedToAssociate(
            @NotNull EntityRawSeed currentState, @NotNull EntityRawSeed seedToAssociate,
            @NotNull Set<EntityLookupEntry> expectedLookupEntries) {
        EntityMatchInternalServiceImpl service = newService();
        Assert.assertNotNull(service);

        Map<Pair<EntityLookupEntry.Type, String>, Set<String>> existingLookupPairs =
                service.getExistingLookupPairs(currentState);
        Assert.assertNotNull(existingLookupPairs);
        Set<EntityLookupEntry> result = service.getLookupEntriesFailedToAssociate(existingLookupPairs, seedToAssociate);
        Assert.assertEquals(result, expectedLookupEntries);
    }

    @Test(groups = "unit", dataProvider = "mapLookupEntriesToSeed")
    private void testMapLookupEntriesToSeed(
            @NotNull EntityRawSeed currentState, @NotNull EntityRawSeed seedToAssociate,
            @NotNull Set<EntityLookupEntry> expectedLookupEntriesToUpdate) {
        // map to store params to EntityLookupEntryService#setIfEquals
        Map<EntityLookupEntry, String> entrySeedIdMap = new HashMap<>();
        // mock entity lookup entry service
        EntityMatchInternalServiceImpl service = newService(entrySeedIdMap);


        Map<Pair<EntityLookupEntry.Type, String>, Set<String>> existingLookupPairs =
                service.getExistingLookupPairs(currentState);
        Assert.assertNotNull(existingLookupPairs);
        List<EntityLookupEntry> result = service.mapLookupEntriesToSeed(
                null, null, existingLookupPairs, seedToAssociate, false, null);
        Assert.assertNotNull(result);
        Map<EntityLookupEntry, String> expectedMap = expectedLookupEntriesToUpdate
                .stream()
                .map(entry -> Pair.of(entry, seedToAssociate.getId()))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        Assert.assertEquals(entrySeedIdMap, expectedMap);
    }

    @DataProvider(name = "getLookupEntriesFailedToAssociate")
    private Object[][] provideGetLookupEntriesFailedToAssociateTestData() {
        return new Object[][] {
                /*
                 * Case #1: no entries failed to associate
                 */
                {
                        newSeed(TGEFA_SEED_ID, new EntityLookupEntry[0]),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1),
                        Sets.newHashSet()
                },
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_2, DUNS_1),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1),
                        Sets.newHashSet()
                },
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_5, MKTO_3, DUNS_1),
                        newSeed(TGEFA_SEED_ID, DC_NETFLIX_2, NC_FACEBOOK_3, NC_FACEBOOK_1, ELOQUA_3),
                        Sets.newHashSet()
                },
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2),
                        newSeed(TGEFA_SEED_ID, DC_NETFLIX_2, NC_FACEBOOK_3, NC_FACEBOOK_1, DUNS_5),
                        Sets.newHashSet()
                },
                {
                        // update entries with one to one mapping with the same value is fine
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_4),
                        Sets.newHashSet()
                },
                {
                        // update entries with x to one mapping with the same value is fine
                        newSeed(TGEFA_SEED_ID, DUNS_4),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_4),
                        Sets.newHashSet()
                },
                /*
                 * Case #2: conflict in DUNS
                 */
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_3),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_4),
                        Sets.newHashSet(DUNS_4)
                },
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_2, DUNS_1),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1, DUNS_3),
                        Sets.newHashSet(DUNS_3)
                },
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_2),
                        newSeed(TGEFA_SEED_ID, DC_NETFLIX_2, NC_FACEBOOK_3, NC_FACEBOOK_1, DUNS_5),
                        Sets.newHashSet(DUNS_5)
                },
                /*
                 * Case #3: conflict in external systems
                 */
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_3),
                        newSeed(TGEFA_SEED_ID, MKTO_1, SFDC_4, ELOQUA_1), // no conflict in SFDC_4
                        Sets.newHashSet(MKTO_1, ELOQUA_1)
                },
                {
                        newSeed(TGEFA_SEED_ID, SFDC_5),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1),
                        Sets.newHashSet(SFDC_1)
                },
                /*
                 * Case #4: conflict in both DUNS & external systems
                 */
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_3),
                        newSeed(TGEFA_SEED_ID, MKTO_1, SFDC_4, ELOQUA_1, DUNS_5), // no conflict in SFDC_4
                        Sets.newHashSet(MKTO_1, ELOQUA_1, DUNS_5)
                },
        };
    }

    /*
     * NOTE the expected set is not exactly the complement of getLookupEntriesFailedToAssociate because
     *      some of the entries already exist in seed and no need to update
     * NOTE no need to set mapping for entries that has conflict or already exist in seed
     */
    @DataProvider(name = "mapLookupEntriesToSeed")
    private Object[][] provideMapLookupEntriesToSeedTestData() {
        return new Object[][] {
                /*
                 * Case #1: no entries failed to associate
                 */
                {
                        newSeed(MLES_SEED_ID, new EntityLookupEntry[0]),
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1),
                        Sets.newHashSet(NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1)
                },
                {
                        // NC_FACEBOOK_1 already exists in current seed, no need to set lookup entry
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_2, DUNS_1),
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1),
                        Sets.newHashSet(NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1)
                },
                {
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, SFDC_5, MKTO_3, DUNS_1),
                        newSeed(MLES_SEED_ID, DC_NETFLIX_2, NC_FACEBOOK_3, NC_FACEBOOK_1, ELOQUA_3),
                        Sets.newHashSet(DC_NETFLIX_2, NC_FACEBOOK_3, ELOQUA_3)
                },
                {
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2),
                        newSeed(MLES_SEED_ID, DC_NETFLIX_2, NC_FACEBOOK_3, NC_FACEBOOK_1, DUNS_5),
                        Sets.newHashSet(DC_NETFLIX_2, NC_FACEBOOK_3, DUNS_5)
                },
                {
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2),
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_4),
                        Sets.newHashSet(DUNS_4)
                },
                {
                        newSeed(MLES_SEED_ID, DUNS_4),
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_4),
                        Sets.newHashSet(NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2)
                },
                /*
                 * Case #2: conflict in DUNS
                 */
                {
                        // no need to set lookup mapping for conflict DUNS
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_3),
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_4),
                        Sets.newHashSet()
                },
                {
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_2, DUNS_1),
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1, DUNS_3),
                        Sets.newHashSet(NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1)
                },
                {
                        // NC_FACEBOOK_1 already exists, DUNS_5 has conflict
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_2),
                        newSeed(MLES_SEED_ID, DC_NETFLIX_2, NC_FACEBOOK_3, NC_FACEBOOK_1, DUNS_5),
                        Sets.newHashSet(DC_NETFLIX_2, NC_FACEBOOK_3)
                },
                /*
                 * Case #3: conflict in external systems
                 */
                {
                        // MKTO_1, ELOQUA_1 has conflict, SFDC_4 already exists
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_3),
                        newSeed(MLES_SEED_ID, MKTO_1, SFDC_4, ELOQUA_1),
                        Sets.newHashSet()
                },
                {
                        // SFDC_1 has conflict
                        newSeed(MLES_SEED_ID, SFDC_5),
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1),
                        Sets.newHashSet(NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, MKTO_1)
                },
                /*
                 * Case #4: conflict in both DUNS & external systems
                 */
                {
                        // MKTO_1, ELOQUA_1, DUNS_5 has conflict, SFDC_4 already exists
                        newSeed(MLES_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_3),
                        newSeed(MLES_SEED_ID, MKTO_1, SFDC_4, ELOQUA_1, DUNS_5),
                        Sets.newHashSet()
                },
        };
    }

    private EntityMatchInternalServiceImpl newService(@NotNull Map<EntityLookupEntry, String> paramMap) {
        EntityLookupEntryService lookupEntryService = Mockito.mock(EntityLookupEntryService.class);
        EntityMatchVersionService entityMatchVersionService = Mockito.mock(EntityMatchVersionService.class);
        Mockito.when(entityMatchVersionService.getCurrentVersion(any(), any())).thenReturn(0);
        Mockito.when(lookupEntryService.setIfEquals(any(), any(), any(), any(), anyBoolean(), anyInt()))
                .thenAnswer(invocation -> {
                    EntityLookupEntry entry = invocation.getArgument(2);
                    String seedId = invocation.getArgument(3);
                    paramMap.put(entry, seedId);
                    // result doesn't matter
                    return true;
                });
        EntityMatchMetricService metricService = Mockito.mock(EntityMatchMetricService.class);
        return new EntityMatchInternalServiceImpl(lookupEntryService, null, null, entityMatchVersionService, null);
    }

    /*
     * all dependent services to null
     */
    private EntityMatchInternalServiceImpl newService() {
        return new EntityMatchInternalServiceImpl(null, null, null, null, null);
    }
}
