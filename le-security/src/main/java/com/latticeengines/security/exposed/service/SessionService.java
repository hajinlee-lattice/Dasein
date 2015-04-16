package com.latticeengines.security.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AccessLevel;

public interface SessionService {
    Session attach(Ticket ticket);

    Session retrieve(Ticket ticket);

    AccessLevel upgradeFromDeprecatedBARD(String tenantId, String username, String email, List<String> rights);
}
