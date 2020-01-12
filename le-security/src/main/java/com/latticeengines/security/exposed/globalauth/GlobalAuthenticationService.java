package com.latticeengines.security.exposed.globalauth;

import java.util.List;

import com.latticeengines.domain.exposed.auth.GlobalAuthExternalSession;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;

public interface GlobalAuthenticationService {

    Ticket authenticateUser(String user, String password);

    Ticket externallyAuthenticated(String emailAddress, GlobalAuthExternalSession externalSession);

    List<Tenant> getValidTenants(GlobalAuthUser user);

    boolean discard(Ticket ticket);
}
