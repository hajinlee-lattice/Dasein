package com.latticeengines.security.exposed.service;

import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;

public interface SessionService {
    Ticket authenticate(Credentials credentials);

    Session attach(Ticket ticket);

    void validateSamlLoginResponse(LoginValidationResponse samlLoginResp);

    Session attachSamlUserToTenant(String userName, String tenantDeploymentId);

    Session retrieve(Ticket ticket);

    void logout(Ticket ticket);

    boolean clearCacheIfNecessary(String tenantId, String token);
}
