package com.latticeengines.security.exposed.service;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;

public interface SessionService {
    Ticket authenticate(Credentials credentials);

    Session attach(Ticket ticket);

    Session retrieve(Ticket ticket);

    void logout(Ticket ticket);
}
