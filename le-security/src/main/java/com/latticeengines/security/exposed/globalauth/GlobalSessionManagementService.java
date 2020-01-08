package com.latticeengines.security.exposed.globalauth;

import java.util.List;

import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;

public interface GlobalSessionManagementService {
    Session retrieve(Ticket ticket);
    
    Session attach(Ticket ticket);

    List<GlobalAuthTicket> findTicketsByUserIdAndTenant(Long userId, GlobalAuthTenant tenant);
}
