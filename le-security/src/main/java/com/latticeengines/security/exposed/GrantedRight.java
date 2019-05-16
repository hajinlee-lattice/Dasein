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
    },
    VIEW_PLS_REPORTS {
        @Override
        public String getAuthority() {
            return "View_PLS_Reports";
        }
    }, //
    EDIT_PLS_REPORTS {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Reports";
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
    VIEW_PLS_RATING_ENGINES {
        @Override
        public String getAuthority() {
            return "View_PLS_RatingEngines";
        }
    }, //
    CREATE_PLS_RATING_ENGINES {
        @Override
        public String getAuthority() {
            return "Create_PLS_RatingEngines";
        }
    }, //
    EDIT_PLS_RATING_ENGINES {
        @Override
        public String getAuthority() {
            return "Edit_PLS_RatingEngines";
        }
    }, //
    VIEW_PLS_PLAYS {
        @Override
        public String getAuthority() {
            return "View_PLS_Plays";
        }
    }, //
    CREATE_PLS_PLAYS {
        @Override
        public String getAuthority() {
            return "Create_PLS_Plays";
        }
    }, //
    EDIT_PLS_PLAYS {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Plays";
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
    VIEW_PLS_JOBS {
        @Override
        public String getAuthority() {
            return "View_PLS_Jobs";
        }
    }, //
    EDIT_PLS_JOBS {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Jobs";
        }
    }, //
    VIEW_PLS_DATA {
        @Override
        public String getAuthority() {
            return "View_PLS_Data";
        }
    }, //
    EDIT_PLS_DATA {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Data";
        }
    }, //
    VIEW_PLS_TARGETMARKETS {
        @Override
        public String getAuthority() {
            return "View_PLS_TargetMarkets";
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
    }, //
    CREATE_PLS_OAUTH2_TOKEN {
        @Override
        public String getAuthority() {
            return "Create_PLS_Oauth2Token";
        }
    }, //
    CREATE_PLS_OAUTH2_TOKEN_EXTERNAL {
        @Override
        public String getAuthority() {
            return "Create_PLS_Oauth2Token_External";
        }
    }, //
    VIEW_PLS_CRMCREDENTIAL {
        @Override
        public String getAuthority() {
            return "View_PLS_CrmCredential";
        }
    }, //
    EDIT_PLS_CRMCREDENTIAL {
        @Override
        public String getAuthority() {
            return "Edit_PLS_CrmCredential";
        }
    }, //
    VIEW_PLS_MARKETO_CREDENTIALS {
        @Override
        public String getAuthority() {
            return "View_PLS_MarketoCredentials";
        }
    }, //
    EDIT_PLS_MARKETO_CREDENTIALS {
        @Override
        public String getAuthority() {
            return "Edit_PLS_MarketoCredentials";
        }
    }, //
    VIEW_PLS_MARKETO_CREDENTIALS_SIMPLIFIED {
        @Override
        public String getAuthority() {
            return "View_PLS_MarketoCredentials_Simplified";
        }
    }, //
    VIEW_PLS_REFINE_CLONE {
        @Override
        public String getAuthority() {
            return "View_PLS_Refine_Clone";
        }
    }, //
    EDIT_PLS_REFINE_CLONE {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Refine_Clone";
        }
    }, //
    VIEW_PLS_REMODEL {
        @Override
        public String getAuthority() {
            return "View_PLS_Remodel";
        }
    }, //
    VIEW_PLS_SAMPLE_LEADS {
        @Override
        public String getAuthority() {
            return "View_PLS_Sample_Leads";
        }
    }, //
    VIEW_PLS_CAMPAIGNS {
        @Override
        public String getAuthority() {
            return "View_PLS_Campaigns";
        }
    }, //
    EDIT_PLS_CAMPAIGNS {
        @Override
        public String getAuthority() {
            return "Edit_PLS_Campaigns";
        }
    },
    VIEW_PLS_CDL_DATA {
        @Override
        public String getAuthority() {
            return "View_PLS_CDL_Data";
        }
    }, //
    EDIT_PLS_CDL_DATA {
        @Override
        public String getAuthority() {
            return "Edit_PLS_CDL_Data";
        }
    }, //
    GENERATE_S3_CREDENTIAL {
        @Override
        public String getAuthority() {
            return "Generate_S3_Credential";
        }
    }, //
    VIEW_S3_CREDENTIAL {
        @Override
        public String getAuthority() {
            return "View_S3_Credential";
        }
    }, //
    EDIT_PLS_SSO_CONFIG {
        @Override
        public String getAuthority() {
            return "Edit_PLS_SSO_Config";
        }
    }, //
    VIEW_PLS_SSO_CONFIG {
        @Override
        public String getAuthority() {
            return "View_PLS_SSO_Config";
        }
    }; //

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
