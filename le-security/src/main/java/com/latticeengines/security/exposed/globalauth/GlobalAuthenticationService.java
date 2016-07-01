package com.latticeengines.security.exposed.globalauth;

import com.latticeengines.domain.exposed.security.Ticket;

public interface GlobalAuthenticationService {

    Ticket authenticateUser(String user, String password);

    Ticket externallyAuthenticated(String emailAddress);

    boolean discard(Ticket ticket);
}
