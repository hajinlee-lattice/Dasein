package com.latticeengines.security.exposed.globalauth;

import java.util.List;

import com.latticeengines.domain.exposed.auth.GlobalAuthExternalSession;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;

public interface GlobalSessionManagementService {
    Session retrieve(Ticket ticket);

    Session attach(Ticket ticket);

    List<GlobalAuthTicket> findTicketsByUserIdsAndTenant(List<Long> userIds, GlobalAuthTenant tenant);

    List<GlobalAuthTicket> findTicketsByEmailAndExternalIssuer(String email, String issuer);

    GlobalAuthExternalSession retrieveExternalSession(Ticket ticket);

    List<GlobalAuthTicket> findByUserIdAndTenantIdAndNotInTicket(Long tenantId, Long userId, String ticket);

    boolean discardSession(boolean expireSession, Ticket ticket, Long tenantId, Long ticketId, Long userId);
}
