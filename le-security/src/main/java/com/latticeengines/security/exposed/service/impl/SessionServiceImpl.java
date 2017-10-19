package com.latticeengines.security.exposed.service.impl;

import java.util.List;
import java.util.Random;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.util.GASessionCache;
import com.latticeengines.security.util.GASessionCacheHolder;

@Component("sessionService")
public class SessionServiceImpl implements SessionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionServiceImpl.class);
    private static final Integer MAX_RETRY = 3;
    private static final Long RETRY_INTERVAL_MSEC = 200L;
    private static Random random = new Random(System.currentTimeMillis());

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    private GASessionCache sessionCache;

    @PostConstruct
    private void initializeSessionCache() {
        sessionCache = GASessionCacheHolder.getCache(globalSessionManagementService);
    }

    @Override
    public Ticket authenticate(Credentials credentials) {
        return globalAuthenticationService.authenticateUser(credentials.getUsername().toLowerCase(),
                credentials.getPassword());
    }

    @Override
    public Session attach(Ticket ticket) {
        Long retryInterval = RETRY_INTERVAL_MSEC;
        Integer retries = 0;
        Session session = null;
        if (ticket.getTenants() == null || ticket.getTenants().size() == 0) {
            LOGGER.error("The ticket" + ticket.getData() + "'s tenant is null");
            throw new RuntimeException("The ticket" + ticket.getData() + "'s tenant is null");
        }
        while (++retries <= MAX_RETRY) {
            try {
                session = globalSessionManagementService.attach(ticket);
                if (session != null) {
                    break;
                }
            } catch (Exception e) {
                LOGGER.error("Failed to attach tenent " + ticket.getTenants().get(0) + " session " + ticket.getData()
                        + " from GA - retried " + retries + " out of " + MAX_RETRY + " times", e);
            }
            try {
                retryInterval = new Double(retryInterval * (1 + 1.0 * random.nextInt(1000) / 1000)).longValue();
                Thread.sleep(retryInterval);
            } catch (Exception e) {
                // ignore
            }
        }
        if (session != null) {
            interpretGARights(session);
        } else {
            LOGGER.error(String.format("Failed to attach ticket %s against GA", ticket.getData()));
            throw new RuntimeException("Failed to attach ticket against GA.");
        }
        // refresh cache upon attach, because the session object changed
        sessionCache.removeToken(ticket.getData());
        sessionCache.put(ticket.getData(), session);
        return session;
    }

    @Override
    public Session retrieve(Ticket ticket) {
        return sessionCache.retrieve(ticket.getData());
    }

    @Override
    public void logout(Ticket ticket) {
        globalAuthenticationService.discard(ticket);
        sessionCache.removeToken(ticket.getData());
    }

    @Override
    public void clearCacheIfNecessary(String tenantId, String token) {
        Session session = sessionCache.retrieve(token);
        Tenant tenant = session.getTenant();
        if (tenant == null || !tenant.getId().equals(tenantId)) {
            LOGGER.info(String.format(
                    "Clearing cache for ticket %s because client thinks that tenant is %s and our cache has %s", token,
                    tenantId, tenant != null ? tenant.getId() : ""));
            sessionCache.removeToken(token);
        }
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
