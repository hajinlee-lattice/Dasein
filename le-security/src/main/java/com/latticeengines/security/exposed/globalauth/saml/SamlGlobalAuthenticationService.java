package com.latticeengines.security.exposed.globalauth.saml;

import com.latticeengines.domain.exposed.security.Ticket;

public interface SamlGlobalAuthenticationService {

    Ticket externallyAuthenticated(String emailAddress, String tenantDeploymentId);

    boolean discard(Ticket ticket);
}
