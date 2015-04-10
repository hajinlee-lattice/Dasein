package com.latticeengines.security.exposed;


import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum AccessLevel {

    EXTERNAL_USER(
        Collections.singletonList(
            GrantedRight.VIEW_PLS_MODELS
        )
    ),
    EXTERNAL_ADMIN(
        Arrays.asList(
            GrantedRight.VIEW_PLS_USERS,
            GrantedRight.EDIT_PLS_USERS,
            GrantedRight.VIEW_PLS_MODELS
        )
    ),
    INTERNAL_USER(
        Arrays.asList(
            GrantedRight.VIEW_PLS_REPORTING,
            GrantedRight.VIEW_PLS_MODELS
        )
    ),
    INTERNAL_ADMIN(
        Arrays.asList(
            GrantedRight.VIEW_PLS_USERS,
            GrantedRight.EDIT_PLS_USERS,
            GrantedRight.VIEW_PLS_CONFIGURATION,
            GrantedRight.EDIT_PLS_CONFIGURATION,
            GrantedRight.VIEW_PLS_REPORTING,
            GrantedRight.VIEW_PLS_MODELS,
            GrantedRight.EDIT_PLS_MODELS
        )
    ),
    SUPER_ADMIN(
        Arrays.asList(
            GrantedRight.VIEW_PLS_USERS,
            GrantedRight.EDIT_PLS_USERS,
            GrantedRight.VIEW_PLS_CONFIGURATION,
            GrantedRight.EDIT_PLS_CONFIGURATION,
            GrantedRight.VIEW_PLS_REPORTING,
            GrantedRight.VIEW_PLS_MODELS,
            GrantedRight.EDIT_PLS_MODELS,
            GrantedRight.CREATE_PLS_MODELS
        )
    );

    private List<GrantedRight> grantedRights;

    AccessLevel(List<GrantedRight> grantedRights) {
        this.grantedRights = grantedRights;
    }

    public List<GrantedRight> getGrantedRights() {
        return this.grantedRights;
    }

    public static AccessLevel maxAccessLevel(List<GrantedRight> hadRights) {
        AccessLevel maxLevel = null;
        for (AccessLevel accessLevel: AccessLevel.values()) {
            if (hadRights.containsAll(accessLevel.getGrantedRights())) {
                maxLevel = accessLevel;
            }
        }
        return maxLevel;
    }
}
