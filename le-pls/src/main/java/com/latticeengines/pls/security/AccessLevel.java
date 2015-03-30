package com.latticeengines.pls.security;


import java.util.Arrays;
import java.util.List;

public enum AccessLevel {

    EXTERNAL_USER(
        Arrays.asList(
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

}
