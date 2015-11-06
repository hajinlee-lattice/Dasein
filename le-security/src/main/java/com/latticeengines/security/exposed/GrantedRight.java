package com.latticeengines.security.exposed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.security.core.GrantedAuthority;

public enum GrantedRight implements GrantedAuthority {

    VIEW_PLS_CONFIGURATIONS {
        @Override
        public String getAuthority() {
            return "View_PLS_Configurations";
        }
    }, //
    EDIT_PLS_CONFIGURATIONS {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Configurations";
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
    VIEW_PLS_TARGETMARKETS {
        @Override
        public String getAuthority() {
            return "View_PLS_TargetMarkets";
        }
    }, //
    CREATE_PLS_TARGETMARKETS {
        @Override
        public String getAuthority() {
            return "Create_PLS_TargetMarkets";
        }
    }, //
    EDIT_PLS_TARGETMARKETS {
        @Override
        public String getAuthority() {
            return "Edit_PLS_TargetMarkets";
        }
    },
    VIEW_PLS_QUOTAS {
        @Override
        public String getAuthority() {
            return "View_PLS_Quotas";
        }
    }, //
    CREATE_PLS_QUOTAS {
        @Override
        public String getAuthority() {
            return "Create_PLS_Quotas";
        }
    }, //
    EDIT_PLS_QUOTAS {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Quotas";
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

    public static List<GrantedRight> getGrantedRights(List<String> values) {
        List<GrantedRight> result = new ArrayList<>();
        for (String value : values) {
            GrantedRight right = GrantedRight.getGrantedRight(value);
            if (right != null) {
                result.add(right);
            }
        }
        return result;
    }

    public static List<String> getAuthorities(List<GrantedRight> rights) {
        List<String> result = new ArrayList<>();
        for (GrantedRight right : rights) {
            result.add(right.getAuthority());
        }
        return result;
    }

}
