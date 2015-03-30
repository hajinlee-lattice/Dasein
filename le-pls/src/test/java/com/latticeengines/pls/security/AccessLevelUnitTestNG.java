package com.latticeengines.pls.security;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

public class AccessLevelUnitTestNG {

    @Test(groups = "unit")
    public void maxAccessLevel() {
        List<GrantedRight> rights = new ArrayList<>();
        testMaxAccessLevel(rights, null);

        for(AccessLevel level: AccessLevel.values()) {
            testMaxAccessLevel(level.getGrantedRights(), level);
        }

        rights = Collections.singletonList(GrantedRight.VIEW_PLS_REPORTING);
        testMaxAccessLevel(rights, null);

        rights = Arrays.asList(
            GrantedRight.VIEW_PLS_USERS,
            GrantedRight.EDIT_PLS_USERS,
            GrantedRight.VIEW_PLS_CONFIGURATION,
            GrantedRight.EDIT_PLS_CONFIGURATION,
            GrantedRight.VIEW_PLS_REPORTING,
            GrantedRight.EDIT_PLS_MODELS
        );
        testMaxAccessLevel(rights, null);

    }

    private void testMaxAccessLevel(List<GrantedRight> rights, AccessLevel expectedLevel) {
        AccessLevel maxLevel = AccessLevel.maxAccessLevel(rights);
        assertEquals(maxLevel, expectedLevel);
    }
}
