package com.latticeengines.security.exposed;

import java.util.Arrays;
import java.util.List;

import org.springframework.security.core.GrantedAuthority;

public enum AccessLevel implements GrantedAuthority {
    THIRD_PARTY_USER(Arrays.asList(GrantedRight.CREATE_PLS_OAUTH2_TOKEN, //
            GrantedRight.VIEW_PLS_CRMCREDENTIAL, //
            GrantedRight.EDIT_PLS_CRMCREDENTIAL, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS_SIMPLIFIED //
    ) //
    ), //
    EXTERNAL_USER(Arrays.asList(GrantedRight.VIEW_PLS_REPORTS, //
            GrantedRight.VIEW_PLS_CAMPAIGNS, //
            GrantedRight.EDIT_PLS_CAMPAIGNS, //
            GrantedRight.VIEW_PLS_DATA, //
            GrantedRight.EDIT_PLS_DATA, //
            GrantedRight.VIEW_PLS_MODELS, //
            GrantedRight.VIEW_PLS_CONFIGURATIONS, //
            GrantedRight.EDIT_PLS_CONFIGURATIONS, //
            GrantedRight.VIEW_PLS_JOBS, //
            GrantedRight.EDIT_PLS_JOBS, //
            GrantedRight.VIEW_PLS_TARGETMARKETS, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS_SIMPLIFIED, //
            GrantedRight.EDIT_PLS_MARKETO_CREDENTIALS, //
            GrantedRight.VIEW_PLS_SAMPLE_LEADS, //
            GrantedRight.EDIT_PLS_REFINE_CLONE, //
            GrantedRight.VIEW_PLS_QUOTAS //
    ) //
    ), //
    EXTERNAL_ADMIN(Arrays.asList(GrantedRight.VIEW_PLS_REPORTS, //
            GrantedRight.EDIT_PLS_REPORTS, //
            GrantedRight.VIEW_PLS_CAMPAIGNS, //
            GrantedRight.EDIT_PLS_CAMPAIGNS, //
            GrantedRight.VIEW_PLS_DATA, //
            GrantedRight.EDIT_PLS_DATA, //
            GrantedRight.VIEW_PLS_USERS, //
            GrantedRight.EDIT_PLS_USERS, //
            GrantedRight.VIEW_PLS_CONFIGURATIONS, //
            GrantedRight.EDIT_PLS_CONFIGURATIONS, //
            GrantedRight.VIEW_PLS_CRMCREDENTIAL, //
            GrantedRight.EDIT_PLS_CRMCREDENTIAL, //
            GrantedRight.VIEW_PLS_MODELS, //
            GrantedRight.EDIT_PLS_MODELS, //
            GrantedRight.VIEW_PLS_JOBS, //
            GrantedRight.EDIT_PLS_JOBS, //
            GrantedRight.VIEW_PLS_TARGETMARKETS, //
            GrantedRight.EDIT_PLS_TARGETMARKETS, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS_SIMPLIFIED, //
            GrantedRight.EDIT_PLS_MARKETO_CREDENTIALS, //
            GrantedRight.VIEW_PLS_QUOTAS, //
            GrantedRight.VIEW_PLS_SAMPLE_LEADS, //
            GrantedRight.CREATE_PLS_OAUTH2_TOKEN_EXTERNAL //
    ) //
    ), //
    INTERNAL_USER(Arrays.asList(GrantedRight.VIEW_PLS_REPORTS, //
            GrantedRight.EDIT_PLS_REPORTS, //
            GrantedRight.VIEW_PLS_REPORTING, //
            GrantedRight.VIEW_PLS_CAMPAIGNS, //
            GrantedRight.EDIT_PLS_CAMPAIGNS, //
            GrantedRight.VIEW_PLS_DATA, //
            GrantedRight.EDIT_PLS_DATA, //
            GrantedRight.VIEW_PLS_MODELS, //
            GrantedRight.EDIT_PLS_MODELS, //
            GrantedRight.VIEW_PLS_CONFIGURATIONS, //
            GrantedRight.EDIT_PLS_CONFIGURATIONS, //
            GrantedRight.VIEW_PLS_JOBS, //
            GrantedRight.EDIT_PLS_JOBS, //
            GrantedRight.VIEW_PLS_TARGETMARKETS, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS_SIMPLIFIED, //
            GrantedRight.EDIT_PLS_MARKETO_CREDENTIALS, //
            GrantedRight.VIEW_PLS_SAMPLE_LEADS, //
            GrantedRight.VIEW_PLS_REFINE_CLONE, //
            GrantedRight.EDIT_PLS_REFINE_CLONE, //
            GrantedRight.VIEW_PLS_QUOTAS //
    ) //
    ), //
    INTERNAL_ADMIN(Arrays.asList(GrantedRight.VIEW_PLS_REPORTS, //
            GrantedRight.EDIT_PLS_REPORTS, //
            GrantedRight.VIEW_PLS_CAMPAIGNS, //
            GrantedRight.EDIT_PLS_CAMPAIGNS, //
            GrantedRight.VIEW_PLS_DATA, //
            GrantedRight.EDIT_PLS_DATA, //
            GrantedRight.VIEW_PLS_USERS, //
            GrantedRight.EDIT_PLS_USERS, //
            GrantedRight.VIEW_PLS_CONFIGURATIONS, //
            GrantedRight.EDIT_PLS_CONFIGURATIONS, //
            GrantedRight.VIEW_PLS_CRMCREDENTIAL, //
            GrantedRight.EDIT_PLS_CRMCREDENTIAL, //
            GrantedRight.VIEW_PLS_REPORTING, //
            GrantedRight.VIEW_PLS_MODELS, //
            GrantedRight.EDIT_PLS_MODELS, //
            GrantedRight.VIEW_PLS_JOBS, //
            GrantedRight.EDIT_PLS_JOBS, //
            GrantedRight.VIEW_PLS_TARGETMARKETS, //
            GrantedRight.EDIT_PLS_TARGETMARKETS, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS_SIMPLIFIED, //
            GrantedRight.EDIT_PLS_MARKETO_CREDENTIALS, //
            GrantedRight.VIEW_PLS_QUOTAS, //
            GrantedRight.EDIT_PLS_QUOTAS, //
            GrantedRight.VIEW_PLS_SAMPLE_LEADS, //
            GrantedRight.VIEW_PLS_REFINE_CLONE, //
            GrantedRight.EDIT_PLS_REFINE_CLONE, //
            GrantedRight.CREATE_PLS_OAUTH2_TOKEN, //
            GrantedRight.CREATE_PLS_OAUTH2_TOKEN_EXTERNAL //
    )), //
    SUPER_ADMIN(Arrays.asList(GrantedRight.VIEW_PLS_REPORTS, //
            GrantedRight.EDIT_PLS_REPORTS, //
            GrantedRight.VIEW_PLS_CAMPAIGNS, //
            GrantedRight.EDIT_PLS_CAMPAIGNS, //
            GrantedRight.VIEW_PLS_DATA, //
            GrantedRight.EDIT_PLS_DATA, //
            GrantedRight.VIEW_PLS_USERS, //
            GrantedRight.EDIT_PLS_USERS, //
            GrantedRight.VIEW_PLS_CONFIGURATIONS, //
            GrantedRight.EDIT_PLS_CONFIGURATIONS, //
            GrantedRight.VIEW_PLS_CRMCREDENTIAL, //
            GrantedRight.EDIT_PLS_CRMCREDENTIAL, //
            GrantedRight.VIEW_PLS_REPORTING, //
            GrantedRight.VIEW_PLS_MODELS, //
            GrantedRight.EDIT_PLS_MODELS, //
            GrantedRight.CREATE_PLS_MODELS, //
            GrantedRight.VIEW_PLS_JOBS, //
            GrantedRight.EDIT_PLS_JOBS, //
            GrantedRight.VIEW_PLS_TARGETMARKETS, //
            GrantedRight.EDIT_PLS_TARGETMARKETS, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS, //
            GrantedRight.VIEW_PLS_MARKETO_CREDENTIALS_SIMPLIFIED, //
            GrantedRight.EDIT_PLS_MARKETO_CREDENTIALS, //
            GrantedRight.VIEW_PLS_QUOTAS, //
            GrantedRight.EDIT_PLS_QUOTAS, //
            GrantedRight.CREATE_PLS_QUOTAS, //
            GrantedRight.VIEW_PLS_SAMPLE_LEADS, //
            GrantedRight.VIEW_PLS_REFINE_CLONE, //
            GrantedRight.EDIT_PLS_REFINE_CLONE, //
            GrantedRight.CREATE_PLS_OAUTH2_TOKEN, //
            GrantedRight.CREATE_PLS_OAUTH2_TOKEN_EXTERNAL //
    )); //

    private List<GrantedRight> grantedRights;

    AccessLevel(List<GrantedRight> grantedRights) {
        this.grantedRights = grantedRights;
    }

    public List<GrantedRight> getGrantedRights() {
        return this.grantedRights;
    }

    @Override
    public String getAuthority() {
        return name();
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
