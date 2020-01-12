package com.latticeengines.security.exposed.service;

import com.latticeengines.domain.exposed.auth.GlobalAuthExternalSession;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;

public interface SessionService {
    Ticket authenticate(Credentials credentials);

    Ticket authenticateSamlUser(String username, GlobalAuthExternalSession externalSession);

    Session attach(Ticket ticket);

    void validateSamlLoginResponse(LoginValidationResponse samlLoginResp);

    Session attachSamlUserToTenant(String userName, String tenantDeploymentId);

    Session retrieve(Ticket ticket);

    GlobalAuthExternalSession retrieveExternalSession(Ticket ticket);

    void logout(Ticket ticket);

    void logoutByIdp(String userName, String issuer);

    boolean clearCacheIfNecessary(String tenantId, String token);
}
