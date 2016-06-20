package com.latticeengines.security.exposed.service.impl;

import java.util.List;
import java.util.Random;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.util.GASessionCache;

@Component("sessionService")
public class SessionServiceImpl implements SessionService {

    private static final Log LOGGER = LogFactory.getLog(SessionServiceImpl.class);
    private static final int CACHE_TIMEOUT_IN_SEC = 60; // compare to the session timeout in GA, 1 min is negligible
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
        sessionCache = new GASessionCache(globalSessionManagementService, CACHE_TIMEOUT_IN_SEC);
    }

    @Override
    public Ticket authenticate(Credentials credentials) {
        return globalAuthenticationService.authenticateUser(credentials.getUsername().toLowerCase(),
                credentials.getPassword());
    }

    @Override
    public Session attach(Ticket ticket){
        Long retryInterval = RETRY_INTERVAL_MSEC;
        Integer retries = 0;
        Session session = null;
        while (++retries <= MAX_RETRY) {
            try {
                session = globalSessionManagementService.attach(ticket);
            } catch (Exception e) {
                LOGGER.warn("Failed to attach tenent " + ticket.getTenants().get(0) + " session " + ticket.getData()
                        + " from GA - retried " + retries + " out of " + MAX_RETRY + " times", e);
            } finally {
                try {
                    retryInterval = new Double(retryInterval * (1 + 1.0 * random.nextInt(1000) / 1000)).longValue();
                    Thread.sleep(retryInterval);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        if (session != null) {
            interpretGARights(session);
        } else {
            throw new RuntimeException("Failed to attach ticket against GA.");
        }
        // refresh cache upon attach, because the session object changed
        sessionCache.removeToken(ticket.getData());
        sessionCache.put(ticket.getData(), session);
        return session;
    }

    @Override
    public Session retrieve(Ticket ticket){ return sessionCache.retrieve(ticket.getData()); }

    @Override
    public void logout(Ticket ticket) {
        globalAuthenticationService.discard(ticket);
        sessionCache.removeToken(ticket.getData());
    }

    private void interpretGARights(Session session) {
        List<String> GARights = session.getRights();
        try {
            AccessLevel level = AccessLevel.findAccessLevel(GARights);
            session.setRights(GrantedRight.getAuthorities(level.getGrantedRights()));
            session.setAccessLevel(level.name());
        } catch (NullPointerException e) {
            if (!GARights.isEmpty()) {
                AccessLevel level = isInternalEmail(session.getEmailAddress()) ?
                        AccessLevel.INTERNAL_USER : AccessLevel.EXTERNAL_USER;
                session.setRights(GrantedRight.getAuthorities(level.getGrantedRights()));
                session.setAccessLevel(level.name());
            }
            LOGGER.warn(String.format("Failed to interpret GA rights: %s for user %s in tenant %s. Use %s instead",
                    GARights.toString(), session.getEmailAddress(), session.getTenant().getId(),
                    session.getAccessLevel()), e);
        }
    }

    private boolean isInternalEmail(String email) {
        return email.toLowerCase().endsWith("lattice-engines.com");
    }
}
