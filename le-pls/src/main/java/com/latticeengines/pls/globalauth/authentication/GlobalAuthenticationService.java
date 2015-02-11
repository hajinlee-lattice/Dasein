package com.latticeengines.pls.globalauth.authentication;

import com.latticeengines.domain.exposed.security.Ticket;

public interface GlobalAuthenticationService {

    Ticket authenticateUser(String user, String password);

    boolean discard(Ticket ticket);
}
