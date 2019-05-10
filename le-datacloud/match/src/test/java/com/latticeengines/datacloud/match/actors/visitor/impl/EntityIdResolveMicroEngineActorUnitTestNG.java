package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.impl.EntityMatchConfigurationServiceImpl;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

public class EntityIdResolveMicroEngineActorUnitTestNG {

    private static final String TEST_ROOT_OPERATION_UID = "entity_id_resolve_actor_unit_test";
    // tuple does not matter in resolve function, just use a dummy one for testing
    private static final MatchKeyTuple TEST_TUPLE = new MatchKeyTuple();

    private static final EntityMatchConfigurationService configurationService = mockConfigService();

    @Test(groups = "unit", dataProvider = "resolveActorAccept")
    private void testAcceptTraveler(@NotNull MatchTraveler traveler, boolean shouldAccept) throws Exception {
        // mock actor
        EntityIdResolveMicroEngineActor actor = Mockito.mock(EntityIdResolveMicroEngineActor.class);
        Mockito.doCallRealMethod().when(actor).accept(Mockito.any());
        FieldUtils.writeField(actor, "entityMatchConfigurationService", configurationService, true);

        // test accept
        Assert.assertEquals(actor.accept(traveler), shouldAccept);
    }

    @Test(groups = "unit", dataProvider = "resolveEntityId")
    private void testResolveEntityIdFromLookupResults(@NotNull MatchTraveler traveler, String expectedEntityId) {
        // mock actor
        EntityIdResolveMicroEngineActor actor = Mockito.mock(EntityIdResolveMicroEngineActor.class);
        Mockito.doCallRealMethod().when(actor).execute(Mockito.any());
        // resolve entity ID
        actor.execute(traveler);

        if (expectedEntityId == null) {
            // no match
            Assert.assertFalse(traveler.isMatched());
            Assert.assertNull(traveler.getResult());
        } else {
            Assert.assertTrue(traveler.isMatched());
            Assert.assertEquals(traveler.getResult(), expectedEntityId);
        }
    }

    /*
     * Row: [ MatchTraveler, boolean (should accept this traveler) ]
     */
    @DataProvider(name = "resolveActorAccept")
    private Object[][] provideAcceptTestData() {
        // all test data for resolve entity ID test should be accepted
        Object[][] resolveTestData = provideResolveEntityIdTestData();
        // should not be accepted
        Object[] rejectTravelers = new Object[] {
                newTraveler(null), newTraveler(new String[0][])
        };

        // merge the test data
        Object[][] res = new Object[resolveTestData.length + rejectTravelers.length][2];
        int idx = 0;
        for (Object[] row : resolveTestData) {
            res[idx][0] = row[0];
            res[idx][1] = true;
            idx++;
        }
        for (Object traveler : rejectTravelers) {
            res[idx][0] = traveler;
            res[idx][1] = false;
            idx++;
        }
        return res;
    }

    /*
     * Row: [ MatchTraveler, String (entityId) ]
     */
    @DataProvider(name = "resolveEntityId")
    private Object[][] provideResolveEntityIdTestData() {
        return new Object[][] {
                // NOTE lookup results will not be empty/null since actor will reject these
                // NOTE empty entity id should also not happen since lookup actors should only return null or non-empty
                //      entity id
                {
                        // one lookup result with empty entity ID list
                        newTraveler(new String[][] {
                                {}
                        }), null
                },
                {
                        // no entity found
                        newTraveler(new String[][] {
                                { null, null },
                                { null },
                                { null, null, null, null },
                        }), null
                },
                {
                        // entity found at the end
                        newTraveler(new String[][] {
                                { null, null, null, null },
                                { null, null },
                                { null, "123" }
                        }), "123"
                },
                {
                        // entity found in the middle
                        newTraveler(new String[][] {
                                { null, "123", "456", null },
                                { "333", "555" },
                                { "111", null, null }
                        }), "123"
                },
                {
                        // entity found at the start (all lookup found entity)
                        newTraveler(new String[][] {
                                { "123", "123", "123", "456" },
                                { "333", "333" },
                                { "333" }
                        }), "123"
                },
                {
                        // entity found at the start (some lookup didn't find entity)
                        newTraveler(new String[][] {
                                { "123", null, null, null, "456" },
                                { "333", null, "123", null },
                                { "555" },
                                { null },
                                { "555" },
                        }), "123"
                },
        };
    }

    private static EntityMatchConfigurationService mockConfigService() {
        EntityMatchConfigurationServiceImpl service = new EntityMatchConfigurationServiceImpl();
        // set to lookup mode to test resolve actor
        service.setIsAllocateMode(false);
        return service;
    }

    /*
     * lookupResults => array of rows (each row is another array of entity IDs)
     */
    private MatchTraveler newTraveler(String[][] lookupResults) {
        MatchTraveler traveler = new MatchTraveler(TEST_ROOT_OPERATION_UID, TEST_TUPLE);
        if (lookupResults != null) {
            List<Pair<MatchKeyTuple, List<String>>> results = Arrays
                    .stream(lookupResults)
                    .map(Arrays::asList)
                    .map(list -> Pair.of(TEST_TUPLE, list))
                    .collect(Collectors.toList());
            traveler.setMatchLookupResults(results);
        }
        return traveler;
    }
}
