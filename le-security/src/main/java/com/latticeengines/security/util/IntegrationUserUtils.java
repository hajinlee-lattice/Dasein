package com.latticeengines.security.util;

import java.util.List;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.util.SamlIntegrationRole;

public final class IntegrationUserUtils {

    protected IntegrationUserUtils() {
        throw new UnsupportedOperationException();
    }

    public static User buildUserFrom(LoginValidationResponse samlResponse) {
        User user = new User();
        user.setLastName(samlResponse.getLastName());
        user.setFirstName(samlResponse.getFirstName());
        user.setEmail(samlResponse.getUserId());
        user.setUsername(samlResponse.getUserId());
        AccessLevel accessLevel = AccessLevel.findAccessLevel(getLatticeRoles(samlResponse.getUserRoles()));
        if (accessLevel != null)
            user.setAccessLevel(accessLevel.name());
        return user;
    }
    
    public static List<String> getLatticeRoles(List<String> integrationRoles) {
        return SamlIntegrationRole.getLatticeRoles(integrationRoles);
    }
    
    public static boolean isIntegrationUserMatchesWithGlobalAuthUser(GlobalAuthUser globalAuthUser, User integrationUser) throws Exception {
        if (globalAuthUser == null || integrationUser == null) {
            throw new Exception("External User or GlobalAuth User is Empty. Cannot match their details");
        }
        return globalAuthUser.getFirstName().equals(integrationUser.getFirstName())
                && globalAuthUser.getLastName().equals(integrationUser.getLastName());
    }
}
