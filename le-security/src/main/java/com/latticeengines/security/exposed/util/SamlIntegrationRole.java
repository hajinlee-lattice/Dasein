package com.latticeengines.security.exposed.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.latticeengines.security.exposed.AccessLevel;

public enum SamlIntegrationRole {

    LATTICE_USER (AccessLevel.EXTERNAL_USER),
    LATTICE_ADMIN (AccessLevel.EXTERNAL_ADMIN);

    private AccessLevel accessLevel;

    SamlIntegrationRole(AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    public AccessLevel getAccessLevel() {
        return accessLevel;
    }

    public static List<String> getLatticeRoles(List<String> integrationRoles) {
        if (integrationRoles == null) {
            return Collections.emptyList();
        }
        List<String> latticeRoles =  new ArrayList<>();
        for(String extRoleStr : integrationRoles) {
            try {
                SamlIntegrationRole integrationRole = SamlIntegrationRole.valueOf(extRoleStr);
                latticeRoles.add(integrationRole.getAccessLevel().toString());
            } catch (IllegalArgumentException ignore) {
                // extRoleStr is not a SamlIntegrationRole
            }
        }
        return latticeRoles;
    }

}
