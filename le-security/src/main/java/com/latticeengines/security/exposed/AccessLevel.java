package com.latticeengines.security.exposed;

import java.util.Arrays;
import java.util.List;

public enum AccessLevel {

    EXTERNAL_USER(
        Arrays.asList(
            GrantedRight.VIEW_PLS_MODELS,
            GrantedRight.VIEW_PLS_TARGETMARKETS,
            GrantedRight.VIEW_PLS_QUOTAS
        )
    ),
    EXTERNAL_ADMIN(
        Arrays.asList(
            GrantedRight.VIEW_PLS_USERS,
            GrantedRight.EDIT_PLS_USERS,
            GrantedRight.VIEW_PLS_MODELS,
            GrantedRight.VIEW_PLS_TARGETMARKETS,
            GrantedRight.VIEW_PLS_QUOTAS
        )
    ),
    INTERNAL_USER(
        Arrays.asList(
            GrantedRight.VIEW_PLS_REPORTING,
            GrantedRight.VIEW_PLS_MODELS,
            GrantedRight.VIEW_PLS_TARGETMARKETS,
            GrantedRight.VIEW_PLS_QUOTAS
        )
    ),
    INTERNAL_ADMIN(
        Arrays .asList(
            GrantedRight.VIEW_PLS_USERS,
            GrantedRight.EDIT_PLS_USERS,
            GrantedRight.VIEW_PLS_CONFIGURATION,
            GrantedRight.EDIT_PLS_CONFIGURATION,
            GrantedRight.VIEW_PLS_REPORTING,
            GrantedRight.VIEW_PLS_MODELS,
            GrantedRight.EDIT_PLS_MODELS,
            GrantedRight.VIEW_PLS_TARGETMARKETS,
            GrantedRight.EDIT_PLS_TARGETMARKETS,
            GrantedRight.VIEW_PLS_QUOTAS,
            GrantedRight.EDIT_PLS_QUOTAS
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
            GrantedRight.CREATE_PLS_MODELS,
            GrantedRight.VIEW_PLS_TARGETMARKETS,
            GrantedRight.EDIT_PLS_TARGETMARKETS,
            GrantedRight.CREATE_PLS_TARGETMARKETS,
            GrantedRight.VIEW_PLS_QUOTAS,
            GrantedRight.EDIT_PLS_QUOTAS,
            GrantedRight.CREATE_PLS_QUOTAS
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
        for (AccessLevel accessLevel : AccessLevel.values()) {
            if (hadRights.containsAll(accessLevel.getGrantedRights())) {
                maxLevel = accessLevel;
            }
        }
        return maxLevel;
    }

    public static AccessLevel findAccessLevel(List<String> rights) {
        AccessLevel maxLevel = null;
        for (String right : rights) {
            try {
                AccessLevel level = AccessLevel.valueOf(right);
                if (maxLevel == null || level.compareTo(maxLevel) > 0) {
                    maxLevel = level;
                }
            } catch (IllegalArgumentException e) {
                // ignore
            }
        }
        return maxLevel;
    }
}
