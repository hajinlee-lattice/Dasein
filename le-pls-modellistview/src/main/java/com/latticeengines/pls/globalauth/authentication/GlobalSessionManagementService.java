package com.latticeengines.pls.globalauth.authentication;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;

public interface GlobalSessionManagementService {
    Session retrieve(Ticket ticket);
    
    Session attach(Ticket ticket);
}
