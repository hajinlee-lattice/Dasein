package com.latticeengines.security.exposed.globalauth.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthSessionEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthSession;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;

@Component("globalSessionManagementService")
@CacheConfig(cacheNames = CacheName.Constants.SessionCacheName)
public class GlobalSessionManagementServiceImpl extends GlobalAuthenticationServiceBaseImpl
        implements GlobalSessionManagementService {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalSessionManagementServiceImpl.class);

    public static final int TicketInactivityTimeoutInMinute = 1440;

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
    @Cacheable(key = "#ticket.data")
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
            LOGGER.info(String.format("Retrieving session from ticket %s against Global Auth.", ticket.toString()));
            Session s = retrieveSession(ticket);
            s.setTicket(ticket);
            return s;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19016, e, new String[] {});
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
    @CachePut(key = "#ticket.data", unless = "#result == null")
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
        Session s = null;
        try {
            LOGGER.info(String.format("Attaching ticket %s against Global Auth.", ticket.toString()));
            s = attachSession(ticket, ticket.getTenants().get(0));
            if (s != null) {
                s.setTicket(ticket);
            }
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
            LOGGER.warn("Unable to find the ticket requested.");
            return null;
        }

        Date now = new Date(System.currentTimeMillis());
        Long timeElapsed = now.getTime() - ticketData.getLastAccessDate().getTime();
        if ((int) (timeElapsed / (1000 * 60)) > TicketInactivityTimeoutInMinute) {
            LOGGER.warn("The requested ticket has expired.");
            return null;
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
                return new SessionBuilder().build(userData, sessionData, tenantData, userTenantRightData);
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

        @SuppressWarnings("unused")
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

            if (session == null) {
                throw new RuntimeException("Failed to attach ticket against GA.");
            }
            interpretGARights(session);

            return session;
        }

        private void interpretGARights(Session session) {
            List<String> GARights = session.getRights();
            try {
                AccessLevel level = AccessLevel.findAccessLevel(GARights);
                session.setRights(GrantedRight.getAuthorities(level.getGrantedRights()));
                session.setAccessLevel(level.name());
            } catch (NullPointerException e) {
                if (!GARights.isEmpty()) {
                    AccessLevel level = isInternalEmail(session.getEmailAddress()) ? AccessLevel.INTERNAL_USER
                            : AccessLevel.EXTERNAL_USER;
                    session.setRights(GrantedRight.getAuthorities(level.getGrantedRights()));
                    session.setAccessLevel(level.name());
                }
                LOGGER.warn(String.format("Failed to interpret GA rights: %s for user %s in tenant %s. Use %s instead",
                        GARights.toString(), session.getEmailAddress(), session.getTenant().getId(),
                        session.getAccessLevel()));
            }
        }

        private boolean isInternalEmail(String email) {
            return email.toLowerCase().endsWith("lattice-engines.com");
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
