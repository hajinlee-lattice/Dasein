package com.latticeengines.security.exposed;


import java.util.HashMap;
import java.util.Map;

import org.springframework.security.core.GrantedAuthority;

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
    }, //
    EDIT_LATTICE_CONFIGURATION {
        @Override
        public String getAuthority() { return "Edit_LATTICE_Configuration"; }
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

}
