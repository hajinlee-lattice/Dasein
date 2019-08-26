package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.google.common.collect.Sets.newHashSet;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.newSeed;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_2;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class EntityAssociateServiceImplUnitTestNG {

    private static final Tenant TEST_TENANT = new Tenant("entity_associate_service_test_tenant_1");
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final String TEST_ENTITY_ID = "associate_service_unit_test_entity_id";
    private static final String TEST_ENTITY_ID2 = "associate_service_unit_test_entity_id2";
    private static final String TEST_REQUEST_ID = "associate_service_unit_test_request_id";

    @Test(groups = "unit", dataProvider = "entityAssociation")
    private void testAssociate(
            EntityAssociationRequest request, EntityRawSeed currentTargetSnapshot, List<EntityRawSeed> expectedParams,
            List<Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>>> results,
            String expectedAssociatedEntityId, Set<EntityLookupEntry> expectedConflictEntries) throws Exception {
        List<EntityRawSeed> params = new ArrayList<>();
        Iterator<Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>>> it = results.iterator();
        EntityAssociateServiceImpl service = mock(params, it);

        EntityAssociationResponse response = service.associate(TEST_REQUEST_ID, request, currentTargetSnapshot);
        Assert.assertNotNull(response);
        Assert.assertEquals(response.getEntity(), request.getEntity());
        Assert.assertNotNull(response.getTenant());
        Assert.assertEquals(response.getTenant().getId(), TEST_TENANT.getId());

        // should make the expected number of calls (iterate through all the results)
        Assert.assertFalse(it.hasNext());
        // expected associated id matched
        Assert.assertEquals(response.getAssociatedEntityId(), expectedAssociatedEntityId);
        if (expectedAssociatedEntityId == null) {
            // if failed to associate, isNewlyAllocated flag should be false
            Assert.assertFalse(response.isNewlyAllocated());
        } else {
            Assert.assertEquals(response.isNewlyAllocated(), currentTargetSnapshot.isNewlyAllocated());
        }
        if (!expectedConflictEntries.isEmpty()) {
            Assert.assertFalse(response.getAssociationErrors().isEmpty(), "Should have association errors");
        } else {
            Assert.assertTrue(response.getAssociationErrors().isEmpty(), "Should not have any association error");
        }

        Assert.assertNotNull(response.getConflictEntries(), "Conflict entry set should not be null");
        Assert.assertEquals(response.getConflictEntries(), expectedConflictEntries,
                "Conflict entries in response should match the expected ones");

        // verify captured params
        Assert.assertEquals(params.size(), expectedParams.size());
        IntStream.range(0, params.size()).forEach(idx -> {
            EntityRawSeed seed = params.get(idx);
            EntityRawSeed expectedSeed = expectedParams.get(idx);
            Assert.assertNotNull(seed);
            // we can use equals of seed here because we need the lookup entries in the same order
            Assert.assertEquals(seed, expectedSeed);
        });
    }

    @Test(groups = "unit", dataProvider = "conflictInHighestPriorityEntry")
    private void testConflictInHighestPriorityEntry(EntityAssociationRequest request, EntityRawSeed target,
            boolean expectedResult) {
        EntityAssociateServiceImpl service = new EntityAssociateServiceImpl();
        Assert.assertEquals(service.conflictInHighestPriorityEntry(request, target), expectedResult);
    }

    @DataProvider(name = "conflictInHighestPriorityEntry")
    private Object[][] provideConflictInHighestPriorityEntryTestData() {
        return new Object[][] {
                // NOTE highest priority entry (that maps to some entity) in the request must
                // map to target entity
                /*
                 * Case #1: highest priority entry map to target entity (no conflict
                 */
                { //
                        newRequest(new Object[][] { { SFDC_1, TEST_ENTITY_ID } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_1), //
                        false //
                }, //
                { //
                        newRequest(new Object[][] { { SFDC_1, TEST_ENTITY_ID } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_2, DUNS_1), // has other entries
                        false //
                }, //
                { //
                        newRequest(new Object[][] { { DUNS_1, TEST_ENTITY_ID }, { DC_FACEBOOK_1, null } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_1, DUNS_1), //
                        false //
                }, //
                { //
                        newRequest(new Object[][] { { NC_GOOGLE_1, TEST_ENTITY_ID }, { DC_FACEBOOK_1, null } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_1, DUNS_1, NC_GOOGLE_1), //
                        false //
                }, //
                /*
                 * Case #2: highest priority entry map to nothing and NOT in target
                 */
                { //
                        newRequest(new Object[][] { { SFDC_1, null }, { MKTO_1, TEST_ENTITY_ID } }), //
                        newSeed(TEST_ENTITY_ID, MKTO_1, DUNS_1), //
                        false //
                }, //
                { //
                        newRequest(new Object[][] { { DUNS_1, null }, { NC_GOOGLE_1, TEST_ENTITY_ID } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_1, NC_GOOGLE_1), //
                        false //
                }, //
                { //
                        newRequest(new Object[][] { { NC_GOOGLE_1, null }, { DC_FACEBOOK_1, TEST_ENTITY_ID } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_1, DUNS_1, DC_FACEBOOK_1), //
                        false //
                }, //
                { //
                        newRequest(new Object[][] { { NC_GOOGLE_1, null }, { DC_FACEBOOK_1, TEST_ENTITY_ID } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_1, DUNS_1, NC_GOOGLE_2, DC_FACEBOOK_1), // can have other n/c entry
                        false //
                }, //
                /*
                 * Case #3: highest priority entry map to nothing and in target (either someone
                 * update entity after lookup is finished for this request or it was there with
                 * different value)
                 */
                { //
                        newRequest(new Object[][] { { SFDC_1, null }, { MKTO_1, TEST_ENTITY_ID } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_1, DUNS_1), // same value, so no conflict
                        false //
                }, //
                { //
                        newRequest(new Object[][] { { SFDC_1, null }, { MKTO_1, TEST_ENTITY_ID } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_2, MKTO_1, DUNS_1), // diff value, has conflict
                        true //
                }, //
                { //
                        newRequest(new Object[][] { { DUNS_1, null }, { NC_GOOGLE_1, TEST_ENTITY_ID } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_1, DUNS_1, NC_GOOGLE_1), //
                        false //
                }, //
                { //
                        newRequest(new Object[][] { { DUNS_1, null }, { NC_GOOGLE_1, TEST_ENTITY_ID } }), //
                        newSeed(TEST_ENTITY_ID, SFDC_1, DUNS_2, NC_GOOGLE_1), //
                        true //
                }, //
        };
    }

    @DataProvider(name = "entityAssociation")
    private Object[][] provideAssociationTestData() {
        return new Object[][] {
                /*
                 * Case #1: empty target seed (newly allocated), one system ID in request
                 */
                {
                        newRequest(new Object[][] {{ SFDC_1, null }}), // only SFDC_1 in request
                        newSeed(TEST_ENTITY_ID, true), // target seed is a newly allocated one
                        // should only call associate once with SFDC_1 (highest priority lookup entry)
                        singletonList(newSeed(TEST_ENTITY_ID, SFDC_1)),
                        // seed before association has no lookup entry (newly allocated)
                        // therefore no association error
                        singletonList(noConflictResult(newSeed(TEST_ENTITY_ID, new EntityLookupEntry[0]))),
                        TEST_ENTITY_ID, emptySet()
                },
                /*
                 * Case #2: empty target seed (newly allocated), more than 1 lookup entries in request
                 */
                {
                        newRequest(new Object[][] {{ SFDC_1, null }, { MKTO_1, null }, { DC_FACEBOOK_1, null }}),
                        newSeed(TEST_ENTITY_ID, true), // target seed is a newly allocated one
                        // should call associate twice
                        // first time with with SFDC_1 (highest priority lookup entry)
                        // second time with the rest of lookup entries (actually we will update all entries
                        // for simplicity, since it will be one update request either way)
                        asList(newSeed(TEST_ENTITY_ID, SFDC_1), newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_1, DC_FACEBOOK_1)),
                        // seed before first association has no lookup entry (newly allocated)
                        // seed before second association only has the highest priority entry
                        // therefore no association error
                        asList(noConflictResult(newSeed(TEST_ENTITY_ID, new EntityLookupEntry[0])),
                                noConflictResult(newSeed(TEST_ENTITY_ID, SFDC_1))),
                        TEST_ENTITY_ID, emptySet()
                },
                /*
                 * Case #3: existing target seed, only one system ID, already mapped to target entity
                 * NOTE: it is not possible this system ID mapped to a entity different than target entity in lookup
                 *       results, otherwise that entity will be chosen as target.
                 */
                {
                        // SFDC_1 already maps to TEST_ENTITY_ID
                        newRequest(new Object[][] {{ SFDC_1, TEST_ENTITY_ID }}),
                        newSeed(TEST_ENTITY_ID, SFDC_1),
                        // no need to associate
                        emptyList(),
                        emptyList(),
                        TEST_ENTITY_ID, emptySet()
                },
                /*
                 * Case #4: existing seed already has some system ID, has association error
                 */
                {
                        newRequest(new Object[][] {
                                { SFDC_1, TEST_ENTITY_ID }, { MKTO_1, null }, { DC_FACEBOOK_1, null }}),
                        newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_2, DC_FACEBOOK_2),
                        // no need to associate highest priority entry, so only one request
                        // no SFDC_1 here because already mapped to other entity
                        // not trying to update MKTO_1 because target seed already has MKTO_2
                        singletonList(newSeed(TEST_ENTITY_ID, DC_FACEBOOK_1)),
                        singletonList(Triple.of(
                                newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_2, DC_FACEBOOK_2),
                                emptyList(),
                                emptyList())),
                        TEST_ENTITY_ID, newHashSet(MKTO_1)
                },
                {
                        // only difference than the previous one is that now there is no DC_FACEBOOK_1 and there
                        // is no need to associate since target entity already has MKTO_2
                        newRequest(new Object[][] {
                                { SFDC_1, TEST_ENTITY_ID }, { MKTO_1, null }}),
                        newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_2),
                        emptyList(),
                        emptyList(),
                        TEST_ENTITY_ID, newHashSet(MKTO_1) // still has error (MKTO_1 with MKTO_2 in target entity)
                },
                /*
                 * Case #5: non-highest priority system ID already associated to other entity
                 */
                {
                        // MKTO_1 already associated to another entity, no need to call underlying service
                        // has association error since MKTO_1 mapped to TEST_ENTITY_ID2
                        newRequest(new Object[][] {
                                { SFDC_1, TEST_ENTITY_ID }, { MKTO_1, TEST_ENTITY_ID2 }}),
                        newSeed(TEST_ENTITY_ID, SFDC_1),
                        emptyList(),
                        emptyList(),
                        TEST_ENTITY_ID, newHashSet(MKTO_1)
                },
                /*
                 * Case #6: non-highest priority lookup entry (many to X) already associate to other entity
                 */
                {
                        // DUNS_1 & DC_FACEBOOK_1 already mapped to another entity
                        // still associate because these entries has many to X mapping
                        newRequest(new Object[][] {
                                { SFDC_1, TEST_ENTITY_ID }, { DUNS_1, TEST_ENTITY_ID2 },
                                { DC_FACEBOOK_1, TEST_ENTITY_ID2 }
                        }),
                        newSeed(TEST_ENTITY_ID, SFDC_1),
                        singletonList(newSeed(TEST_ENTITY_ID, DUNS_1, DC_FACEBOOK_1)),
                        singletonList(Triple.of(
                                newSeed(TEST_ENTITY_ID, SFDC_1),
                                emptyList(),
                                asList(DUNS_1, DC_FACEBOOK_1))),
                        TEST_ENTITY_ID, newHashSet(DUNS_1, DC_FACEBOOK_1)
                },
                /*
                 * Case #7: mixing #5 & #6 & some entries not mapped to any entity
                 */
                {
                        newRequest(new Object[][] {
                                { SFDC_1, TEST_ENTITY_ID }, { MKTO_1, TEST_ENTITY_ID2 },
                                { DUNS_1, TEST_ENTITY_ID2 }, { DC_FACEBOOK_1, TEST_ENTITY_ID2 },
                                { NC_GOOGLE_1, null }, { NC_GOOGLE_2, null }
                        }),
                        newSeed(TEST_ENTITY_ID, SFDC_1),
                        singletonList(newSeed(TEST_ENTITY_ID, DUNS_1, DC_FACEBOOK_1, NC_GOOGLE_1, NC_GOOGLE_2)),
                        singletonList(Triple.of(
                                newSeed(TEST_ENTITY_ID, SFDC_1),
                                emptyList(),
                                asList(MKTO_1, DUNS_1, DC_FACEBOOK_1))),
                        TEST_ENTITY_ID, newHashSet(DUNS_1, MKTO_1, DC_FACEBOOK_1)
                },
                /*
                 * Case #8: two association required
                 */
                {
                        // ELOQUA_3 already mapped to another entity
                        // MKTO_1 has conflict in seed
                        newRequest(new Object[][] {
                                { SFDC_1, null }, { MKTO_1, null }, { ELOQUA_3, TEST_ENTITY_ID2 },
                                { DUNS_1, TEST_ENTITY_ID2 }, { DC_FACEBOOK_1, TEST_ENTITY_ID2 },
                                { NC_GOOGLE_1, null }, { NC_GOOGLE_2, null }
                        }),
                        newSeed(TEST_ENTITY_ID, MKTO_2),
                        asList(newSeed(TEST_ENTITY_ID, SFDC_1), newSeed(TEST_ENTITY_ID, SFDC_1, DUNS_1,
                               DC_FACEBOOK_1, NC_GOOGLE_1, NC_GOOGLE_2)),
                        asList(noConflictResult(newSeed(TEST_ENTITY_ID, MKTO_2)), Triple.of(
                                newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_2),
                                emptyList(),
                                asList(MKTO_1, DUNS_1, DC_FACEBOOK_1))),
                        TEST_ENTITY_ID, newHashSet(DUNS_1, DC_FACEBOOK_1, MKTO_1, ELOQUA_3)
                },
                /*
                 * Case #9: highest priority entry is many to many
                 */
                {
                        newRequest(new Object[][] {
                                { DC_FACEBOOK_1, null }, { NC_GOOGLE_1, null }, { NC_GOOGLE_2, null }
                        }),
                        newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_2, DC_GOOGLE_3),
                        asList(newSeed(TEST_ENTITY_ID, DC_FACEBOOK_1),
                                newSeed(TEST_ENTITY_ID, DC_FACEBOOK_1, NC_GOOGLE_1, NC_GOOGLE_2)),
                        asList(noConflictResult(newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_2, DC_GOOGLE_3)), Triple.of(
                                newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_2, DC_FACEBOOK_1, DC_GOOGLE_3),
                                emptyList(), emptyList())),
                        TEST_ENTITY_ID, emptySet()
                },
                /*
                 * Case #10: conflict caused multiple processes trying to associate at the same time
                 */
                {
                        // conflict during updating seed
                        newRequest(new Object[][] {{ SFDC_1, null }}),
                        newSeed(TEST_ENTITY_ID, new EntityLookupEntry[0]),
                        singletonList(newSeed(TEST_ENTITY_ID, SFDC_1)),
                        // seed before association has no lookup entry (newly allocated)
                        // therefore no association error
                        singletonList(Triple.of(
                                newSeed(TEST_ENTITY_ID, new EntityLookupEntry[0]),
                                singletonList(SFDC_1), emptyList())),
                        // NOTE failed to associate highest priority entry, get null ID in response
                        null, newHashSet(SFDC_1)
                },
                {
                        // conflict during updating lookup mapping
                        newRequest(new Object[][] {{ SFDC_1, null }}),
                        newSeed(TEST_ENTITY_ID, new EntityLookupEntry[0]),
                        singletonList(newSeed(TEST_ENTITY_ID, SFDC_1)),
                        // seed before association has no lookup entry (newly allocated)
                        // therefore no association error
                        singletonList(Triple.of(
                                newSeed(TEST_ENTITY_ID, new EntityLookupEntry[0]),
                                emptyList(), singletonList(SFDC_1))),
                        // NOTE failed to associate highest priority entry, get null ID in response
                        null, newHashSet(SFDC_1)
                },
                /*
                 * Case #11: entry many to one mapping
                 */
                {
                        newRequest(new Object[][] {{ SFDC_1, TEST_ENTITY_ID }, { DUNS_1, null }}),
                        // target seed already has SFDC_1 and DUNS_2 (conflict), no need to make association calls
                        newSeed(TEST_ENTITY_ID, SFDC_1, DUNS_2),
                        emptyList(),
                        emptyList(),
                        // has conflict because DUNS_1 has conflict with DUNS_2 in target seed
                        TEST_ENTITY_ID, newHashSet(DUNS_1)
                },
                {
                        newRequest(new Object[][] {{ SFDC_1, TEST_ENTITY_ID }, { DUNS_1, TEST_ENTITY_ID2 }}),
                        newSeed(TEST_ENTITY_ID, SFDC_1),
                        // still try to update DUNS_1 because the mapping is many to one
                        singletonList(newSeed(TEST_ENTITY_ID, DUNS_1)),
                        singletonList(Triple.of(
                                newSeed(TEST_ENTITY_ID, SFDC_1),
                                emptyList(), emptyList())),
                        // no conflict even if DUNS_1 already mapped to another entity (cuz mapping is many to one)
                        TEST_ENTITY_ID, emptySet()
                },
                /*
                 * Case #12: empty target seed (newly allocated), one system ID in request, and
                 * have conflict in lookup
                 */
                { newRequest(new Object[][] { { SFDC_1, null } }), // only SFDC_1 in request
                        newSeed(TEST_ENTITY_ID, true), // target seed is a newly allocated one
                        // should only call associate once with SFDC_1 (highest priority lookup entry)
                        singletonList(newSeed(TEST_ENTITY_ID, SFDC_1)),
                        // seed before association has no lookup entry (newly allocated)
                        // therefore no conflict in seed
                        // unfortunately, SFDC_1 is mapped to other seed during association
                        singletonList( //
                                Triple.of(newSeed(TEST_ENTITY_ID, false), //
                                        emptyList(), //
                                        singletonList(SFDC_1))), //
                        null, newHashSet(SFDC_1) },
                /*
                 * Case #13: empty target seed (newly allocated), more than 1 lookup entries in
                 * request, conflict in lower priority key's lookup mapping
                 */
                { newRequest(new Object[][] { { SFDC_1, null }, { MKTO_1, null }, { DC_FACEBOOK_1, null } }),
                        newSeed(TEST_ENTITY_ID, true), // target seed is a newly allocated one
                        // should call associate twice
                        // first time with with SFDC_1 (highest priority lookup entry)
                        // second time with the rest of lookup entries (actually we will update all
                        // entries
                        // for simplicity, since it will be one update request either way)
                        asList(newSeed(TEST_ENTITY_ID, SFDC_1), newSeed(TEST_ENTITY_ID, SFDC_1, MKTO_1, DC_FACEBOOK_1)),
                        // seed before first association has no lookup entry (newly allocated)
                        // seed before second association only has the highest priority entry
                        // therefore no association error in seed
                        // however, MKTO_1 is mapped to other seed during association, so conflict in
                        // lookup. Should still associate to the target seed since conflict is not in
                        // highest priority key
                        asList(noConflictResult(newSeed(TEST_ENTITY_ID, new EntityLookupEntry[0])), //
                                Triple.of( //
                                        newSeed(TEST_ENTITY_ID, false, SFDC_1), //
                                        emptyList(), singletonList(MKTO_1))), //
                        TEST_ENTITY_ID, newHashSet(MKTO_1) },
        };
    }

    private Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> noConflictResult(
            EntityRawSeed seedBeforeAssociation) {
        return Triple.of(seedBeforeAssociation, emptyList(), emptyList());
    }

    /*
     * rows format: array of [ EntityLookupEntry, String ]
     */
    private EntityAssociationRequest newRequest(Object[][] rows) {
        List<Pair<MatchKeyTuple, String>> lookupResults = Arrays
                .stream(rows)
                .map(row -> {
                    EntityLookupEntry entry = (EntityLookupEntry) row[0];
                    String seedId = (String) row[1];
                    MatchKeyTuple tuple = EntityLookupEntryConverter.toMatchKeyTuple(entry);
                    return Pair.of(tuple, seedId);
                }).collect(Collectors.toList());
        return new EntityAssociationRequest(TEST_TENANT, TEST_ENTITY, null, lookupResults, null);
    }

    private EntityAssociateServiceImpl mock(
            @NotNull List<EntityRawSeed> params,
            Iterator<Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>>> results)
            throws Exception {
        EntityAssociateServiceImpl service = new EntityAssociateServiceImpl();
        EntityMatchInternalService internalService = Mockito.mock(EntityMatchInternalService.class);
        Mockito.when(internalService.associate(Mockito.any(), Mockito.any(), Mockito.anyBoolean(), Mockito.any()))
                .thenAnswer(invocation -> {
            Tenant inputTenant = invocation.getArgument(0);
            EntityRawSeed seed = invocation.getArgument(1);
            Assert.assertNotNull(inputTenant);
            Assert.assertNotNull(seed);
            Assert.assertEquals(inputTenant.getId(), TEST_TENANT.getId());
            // capture parameter
            params.add(seed);
            return results.next();
        });
        FieldUtils.writeField(service, "entityMatchInternalService", internalService, true);
        return service;
    }
}
