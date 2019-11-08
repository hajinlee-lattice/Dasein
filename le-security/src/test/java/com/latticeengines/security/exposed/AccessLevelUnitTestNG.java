package com.latticeengines.security.exposed;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;

public class AccessLevelUnitTestNG {

    @Test(groups = "unit")
    public void testRandome() {

        Map<String, DataUnit> map = new HashMap<>();
        map.put("0", new HdfsDataUnit());
        map.put("1", null);

        Map<String, DataUnit> inputUnits = new ConcurrentHashMap<>();
        inputUnits.put("0", new HdfsDataUnit());
        inputUnits.put("1", null);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void parseString() {
        AccessLevel.valueOf(GrantedRight.VIEW_PLS_MODELS.getAuthority());
    }

    @Test(groups = "unit")
    public void cardinalityOfAccessLevels() {
        AccessLevel[] levelsInOrder = new AccessLevel[] { AccessLevel.SUPER_ADMIN, //
                AccessLevel.INTERNAL_ADMIN, //
                AccessLevel.INTERNAL_USER, //
                AccessLevel.EXTERNAL_ADMIN, //
                AccessLevel.EXTERNAL_USER, //
                AccessLevel.THIRD_PARTY_USER //
        };
        for (int i = 0; i < levelsInOrder.length - 1; i++) {
            assertTrue(levelsInOrder[i].compareTo(levelsInOrder[i + 1]) > 0);
            assertTrue(levelsInOrder[i + 1].compareTo(levelsInOrder[i]) < 0);
        }
    }

    @Test(groups = "unit")
    public void maxAccessLevel() {
        List<GrantedRight> rights = new ArrayList<>();
        testMaxAccessLevel(rights, null);

        for (AccessLevel level : AccessLevel.values()) {
            testMaxAccessLevel(level.getGrantedRights(), level);
        }

        rights = Collections.singletonList(GrantedRight.VIEW_PLS_REPORTING);
        testMaxAccessLevel(rights, null);

        rights = Arrays.asList(GrantedRight.VIEW_PLS_USERS, //
                GrantedRight.EDIT_PLS_USERS, //
                GrantedRight.VIEW_PLS_CONFIGURATIONS, //
                GrantedRight.EDIT_PLS_CONFIGURATIONS, //
                GrantedRight.EDIT_PLS_MODELS);
        testMaxAccessLevel(rights, null);

    }

    private void testMaxAccessLevel(List<GrantedRight> rights, AccessLevel expectedLevel) {
        AccessLevel maxLevel = AccessLevel.maxAccessLevel(rights);
        assertEquals(maxLevel, expectedLevel);
    }
}
