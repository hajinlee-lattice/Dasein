package com.latticeengines.pls.security;



import org.springframework.security.core.GrantedAuthority;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum GrantedRight implements GrantedAuthority {

    VIEW_PLS_CONFIGURATION {
        @Override
        public String getAuthority() {
            return "View_PLS_Configuration";
        }
    }, //
    EDIT_PLS_CONFIGURATION {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Configuration";
        }
    }, //
    VIEW_PLS_REPORTING {
        @Override
        public String getAuthority() {
            return "View_PLS_Reporting";
        }
    }, //
    VIEW_PLS_MODELS {
        @Override
        public String getAuthority() {
            return "View_PLS_Models";
        }
    }, //
    CREATE_PLS_MODELS {
        @Override
        public String getAuthority() {
            return "Create_PLS_Models";
        }
    }, //
    EDIT_PLS_MODELS {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Models";
        }
    }, //
    VIEW_PLS_USERS {
        @Override
        public String getAuthority() {
            return "View_PLS_Users";
        }
    }, //
    EDIT_PLS_USERS {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Users";
        }
    };

    private static Map<String, GrantedRight> grantedRightsMap = new HashMap<>();

    static {
        for (GrantedRight grantedRight : GrantedRight.values()) {
            grantedRightsMap.put(grantedRight.getAuthority(), grantedRight);
        }
    }

    public static GrantedRight getGrantedRight(String value) {
        return grantedRightsMap.get(value);
    }

    public static List<GrantedRight> getDefaultRights() {
        return Arrays.asList(
            GrantedRight.VIEW_PLS_MODELS,
            GrantedRight.VIEW_PLS_REPORTING,
            GrantedRight.VIEW_PLS_CONFIGURATION
        );
    }

    public static List<GrantedRight> getAdminRights() {
        return Arrays.asList(
            GrantedRight.VIEW_PLS_MODELS,
            GrantedRight.EDIT_PLS_MODELS,
            GrantedRight.CREATE_PLS_MODELS,
            GrantedRight.VIEW_PLS_REPORTING,
            GrantedRight.VIEW_PLS_CONFIGURATION,
            GrantedRight.EDIT_PLS_CONFIGURATION,
            GrantedRight.VIEW_PLS_USERS,
            GrantedRight.EDIT_PLS_USERS
        );
    }

}
