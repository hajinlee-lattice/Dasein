package com.latticeengines.security.exposed.service;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;

public interface SessionService {
    Session attach(Ticket ticket);

    Session retrieve(Ticket ticket);
}
