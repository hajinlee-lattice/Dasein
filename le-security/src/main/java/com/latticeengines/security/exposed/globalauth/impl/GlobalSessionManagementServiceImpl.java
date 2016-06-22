package com.latticeengines.security.exposed.globalauth.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.auth.GlobalAuthSession;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.entitymanager.GlobalAuthSessionEntityMgr;
import com.latticeengines.security.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.security.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.security.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.security.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;

@Component("globalSessionManagementService")
public class GlobalSessionManagementServiceImpl
        extends GlobalAuthenticationServiceBaseImpl
        implements GlobalSessionManagementService {

    private static final Log LOGGER = LogFactory.getLog(GlobalSessionManagementServiceImpl.class);

    private static final int TicketInactivityTimeoutInMinute = 1440;

    @Autowired
    private GlobalAuthTicketEntityMgr gaTicketEntityMgr;

    @Autowired
    private GlobalAuthUserEntityMgr gaUserEntityMgr;

    @Autowired
    private GlobalAuthSessionEntityMgr gaSessionEntityMgr;

    @Autowired
    private GlobalAuthTenantEntityMgr gaTenantEntityMgr;

    @Autowired
    private GlobalAuthUserTenantRightEntityMgr gaUserTenantRightEntityMgr;

    @Override
    public synchronized Session retrieve(Ticket ticket) {
        if (ticket == null) {
            throw new NullPointerException("Ticket cannot be null.");
        }
        if (ticket.getRandomness() == null) {
            throw new NullPointerException("Ticket.getRandomness() cannot be null.");
        }
        if (ticket.getUniqueness() == null) {
            throw new NullPointerException("Ticket.getUniqueness() cannot be null.");
        }
        try {
            LOGGER.info(String.format("Retrieving session from ticket %s against Global Auth.",
                    ticket.toString()));
            Session s = retrieveSession(ticket);
            s.setTicket(ticket);
            return s;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18002, e, new String[] { ticket.getData() });
        }
    }

    private Session retrieveSession(Ticket ticket) throws Exception {
        GlobalAuthTicket ticketData = gaTicketEntityMgr.findByTicket(ticket.getData());

        if (ticketData == null) {
            throw new Exception("Unable to find the ticket requested.");
        }

        Date now = new Date(System.currentTimeMillis());
        Long timeElapsed = now.getTime() - ticketData.getLastAccessDate().getTime();
        if ((int) (timeElapsed / (1000 * 60)) > TicketInactivityTimeoutInMinute) {
            throw new Exception("The requested ticket has expired.");
        }

        GlobalAuthUser userData = gaUserEntityMgr.findByUserId(ticketData.getUserId());

        if (userData == null) {
            throw new Exception("Unable to find the user requested.");
        }

        if (!userData.getIsActive()) {
            throw new Exception("The user is inactive!");
        }

        GlobalAuthSession sessionData = gaSessionEntityMgr.findByTicketId(ticketData.getPid());

        if (sessionData == null) {
            throw new Exception("Unable to find the session requested.");
        }

        GlobalAuthTenant tenantData = gaTenantEntityMgr.findById(sessionData.getTenantId());

        if (tenantData == null) {
            throw new Exception("Unable to find the tenant requested.");
        }

        List<GlobalAuthUserTenantRight> userTenantRightData = gaUserTenantRightEntityMgr
                .findByUserIdAndTenantId(userData.getPid(), sessionData.getTenantId());

        if (userTenantRightData == null || userTenantRightData.size() == 0) {
            throw new Exception("Unable to find the rights for the user-tenant requested.");
        }

        ticketData.setLastAccessDate(now);
        gaTicketEntityMgr.update(ticketData);

        return new SessionBuilder().build(userData, sessionData, tenantData, userTenantRightData);
    }

    @Override
    public synchronized Session attach(Ticket ticket) {
        if (ticket == null) {
            throw new NullPointerException("Ticket cannot be null.");
        }
        if (ticket.getRandomness() == null) {
            throw new NullPointerException("Ticket.getRandomness() cannot be null.");
        }
        if (ticket.getUniqueness() == null) {
            throw new NullPointerException("Ticket.getUniqueness() cannot be null.");
        }
        if (ticket.getTenants().size() == 0) {
            throw new RuntimeException("There must be at least one tenant in the ticket.");
        }
        try {
            LOGGER.info(String.format("Attaching ticket %s against Global Auth.", ticket.toString()));
            Session s = attachSession(ticket, ticket.getTenants().get(0));
            s.setTicket(ticket);
            return s;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18001, e, new String[] { ticket.toString() });
        }
    }

    private Session attachSession(Ticket ticket, Tenant tenant) throws Exception {
        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenant.getId());
        if (tenantData == null) {
            throw new Exception("Unable to find the tenant requested.");
        }

        GlobalAuthTicket ticketData = gaTicketEntityMgr.findByTicket(ticket.getData());
        if (ticketData == null) {
            throw new Exception("Unable to find the ticket requested.");
        }

        Date now = new Date(System.currentTimeMillis());
        Long timeElapsed = now.getTime() - ticketData.getLastAccessDate().getTime();
        if ((int) (timeElapsed / (1000 * 60)) > TicketInactivityTimeoutInMinute) {
            throw new Exception("The requested ticket has expired.");
        }

        GlobalAuthUser userData = gaUserEntityMgr.findByUserId(ticketData.getUserId());
        if (userData == null) {
            throw new Exception("Unable to find the user requested.");
        }

        if (!userData.getIsActive()) {
            throw new Exception("The user is inactive!");
        }

        List<GlobalAuthUserTenantRight> userTenantRightData = gaUserTenantRightEntityMgr
                .findByUserIdAndTenantId(userData.getPid(), tenantData.getPid());
        if (userTenantRightData == null || userTenantRightData.size() == 0) {
            throw new Exception("Unable to find the rights for the user-tenant requested.");
        }

        GlobalAuthSession sessionData = gaSessionEntityMgr.findByTicketId(ticketData.getPid());
        ticketData.setLastAccessDate(now);
        gaTicketEntityMgr.update(ticketData);

        if (sessionData != null) {
            if (sessionData.getTenantId() == tenantData.getPid()) {
                return new SessionBuilder().build(userData, sessionData, tenantData,
                        userTenantRightData);
            } else {
                gaSessionEntityMgr.delete(sessionData);
            }
        }

        sessionData = new GlobalAuthSession();
        sessionData.setTenantId(tenantData.getPid());
        sessionData.setUserId(ticketData.getUserId());
        sessionData.setTicketId(ticketData.getPid());
        gaSessionEntityMgr.create(sessionData);
        return new SessionBuilder().build(userData, sessionData, tenantData, userTenantRightData);
    }

    static class SessionBuilder {

        public Session build(GlobalAuthUser userData, GlobalAuthSession sessionData, GlobalAuthTenant tenantData,
                List<GlobalAuthUserTenantRight> userTenantRightData) {

            Tenant tenant = new Tenant();
            tenant.setId(tenantData.getId());
            tenant.setName(tenantData.getName());

            List<String> rights = new ArrayList<String>();
            for (GlobalAuthUserTenantRight userTenantRight : userTenantRightData) {
                rights.add(userTenantRight.getOperationName());
            }

            Session session = new Session();
            session.setIdentifier(userData.getPid().toString());
            session.setDisplayName(userData.getFirstName() + " " + userData.getLastName());
            session.setEmailAddress(userData.getEmail());
            session.setLocale("en-US");
            session.setRights(rights);
            session.setTitle(userData.getTitle());
            session.setTenant(new TenantBuilder().build(tenantData));

            return session;
        }
    }

    static class TenantBuilder {
        public Tenant build(GlobalAuthTenant tenantData) {
            Tenant tenant = new Tenant();
            tenant.setId(tenantData.getId());
            tenant.setName(tenantData.getName());
            return tenant;
        }
    }
}
