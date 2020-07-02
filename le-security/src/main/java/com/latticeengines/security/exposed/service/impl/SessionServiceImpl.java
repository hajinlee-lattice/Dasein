package com.latticeengines.security.exposed.service.impl;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.util.SessionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthExternalSession;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.exposed.globalauth.saml.SamlGlobalAuthenticationService;
import com.latticeengines.security.exposed.service.SessionService;

@Component("sessionService")
@CacheConfig(cacheNames = CacheName.Constants.SessionCacheName)
public class SessionServiceImpl implements SessionService {

    private static final Logger log = LoggerFactory.getLogger(SessionServiceImpl.class);

    private static final Integer MAX_RETRY = 3;
    private static final Long RETRY_INTERVAL_MSEC = 200L;
    private static Random random = new Random(System.currentTimeMillis());
    private static Long retryIntervalMsec = 200L;
    private static final String AUTH_ROUTE_SSO = "SSO";
    private static final String AUTH_ROUTE_GA = "GA";

    @Inject
    private GlobalSessionManagementService globalSessionManagementService;

    @Inject
    private GlobalAuthenticationService globalAuthenticationService;

    @Inject
    private SamlGlobalAuthenticationService samlGlobalAuthenticationService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public Ticket authenticate(Credentials credentials) {
        Ticket ticket = globalAuthenticationService.authenticateUser(credentials.getUsername().toLowerCase(),
                credentials.getPassword());
        if (ticket != null) {
            ticket.setAuthenticationRoute(AUTH_ROUTE_GA);
        }
        return ticket;
    }

    @Override
    public Ticket authenticateSamlUser(String username, GlobalAuthExternalSession externalSession) {
        if (username == null || StringUtils.isBlank(username)) {
            log.info("Saml Login response is missing required information. " + username);
            throw new LedpException(LedpCode.LEDP_19005);
        }
        Ticket ticket = globalAuthenticationService.externallyAuthenticated(username, externalSession);
        if (ticket != null) {
            ticket.setAuthenticationRoute(AUTH_ROUTE_GA);
        }
        return ticket;
    }

    @Override
    public void validateSamlLoginResponse(LoginValidationResponse samlLoginResp) {
        if (samlLoginResp == null || StringUtils.isBlank(samlLoginResp.getUserId())) {
            log.info("Saml Login response is missing required information. " + samlLoginResp);
            throw new LedpException(LedpCode.LEDP_19005);
        }
        // PLS-17389. Remove isInternalUser check from PLS-6543 to allow internal emails.
        // if (EmailUtils.isInternalUser(samlLoginResp.getUserId())) {
        // throw new LedpException(LedpCode.LEDP_19004);
        // }
    }

    @Override
    public Session attachSamlUserToTenant(String userName, String tenantDeploymentId) {
        Ticket ticket = samlGlobalAuthenticationService.externallyAuthenticated(userName, tenantDeploymentId);
        ticket.setAuthenticationRoute(AUTH_ROUTE_SSO);

        // Update Tenant info into Ticket
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantDeploymentId);
        if (tenant == null) {
            throw new RuntimeException("Could not find tenant: " + tenantDeploymentId);
        }
        ticket.setTenants(Collections.singletonList(tenant));

        Session session = attach(ticket);
        session.setAuthenticationRoute(AUTH_ROUTE_SSO);
        return setTenantPid(session);
    }

    @Override
    public Session attach(Ticket ticket) {
        Long retryInterval = RETRY_INTERVAL_MSEC;
        Integer retries = 0;
        Session session = null;
        if (ticket.getTenants() == null || ticket.getTenants().size() == 0) {
            log.error("The ticket" + ticket.getData() + "'s tenant is null");
            throw new RuntimeException("The ticket" + ticket.getData() + "'s tenant is null");
        }
        while (++retries <= MAX_RETRY) {
            try {
                session = globalSessionManagementService.attach(ticket);
                if (session != null) {
                    break;
                }
            } catch (Exception e) {
                log.error("Failed to attach tenent " + ticket.getTenants().get(0) + " session " + ticket.getData()
                        + " from GA - retried " + retries + " out of " + MAX_RETRY + " times", e);
            }
            retryInterval = Double.valueOf(retryInterval * (1 + 1.0 * random.nextInt(1000) / 1000)).longValue();
            SleepUtils.sleep(retryInterval);
        }
        if (session != null) {
            validateSession(session);
            session.setAuthenticationRoute(AUTH_ROUTE_GA);
        }
        return setTenantPid(session);
    }

    @Override
    public Session retrieve(Ticket ticket) {
        return retrieve(ticket.getData());
    }

    @Override
    public GlobalAuthExternalSession retrieveExternalSession(Ticket ticket) {
        return globalSessionManagementService.retrieveExternalSession(ticket);
    }

    @Override
    public void logout(Ticket ticket) {
        globalAuthenticationService.discard(ticket);
    }

    @Override
    public void logoutByIdp(String userName, String issuer) {
        List<GlobalAuthTicket> gaTickets = //
                globalSessionManagementService.findTicketsByEmailAndExternalIssuer(userName, issuer);
        log.info("Found {} tickets for {} issued by {}", CollectionUtils.size(gaTickets), userName, issuer);
        for (GlobalAuthTicket gaTicket : gaTickets) {
            logout(new Ticket(gaTicket.getTicket()));
        }
    }

    @Override
    @CacheEvict(key = "#token", condition = "#result == true")
    public boolean clearCacheIfNecessary(String tenantId, String token) {
        Session session = retrieve(token);
        Tenant tenant = null;
        if (session != null) {
            tenant = session.getTenant();
        }
        if (tenant == null || !tenant.getId().equals(tenantId)) {
            log.info(String.format(
                    "Clearing cache for ticket %s because client thinks that tenant is %s and our cache has %s", token,
                    tenantId, tenant != null ? tenant.getId() : ""));
            return true;
        }
        return false;
    }

    private void validateSession(Session session) {
        Date now = new Date(System.currentTimeMillis());
        Long timeElapsed = now.getTime() - session.getTicketCreationTime();
        if ((int) (timeElapsed / (1000 * 60)) > SessionUtils.TicketTimeoutInMinute) {
            throw new LedpException(LedpCode.LEDP_19016);
        }
    }

    private Session retrieve(String token) {
        Ticket ticket = new Ticket(token);
        Long retryInterval = retryIntervalMsec;
        Integer retries = 0;
        Session session = null;
        while (++retries <= MAX_RETRY) {
            try {
                session = globalSessionManagementService.retrieve(ticket);
                if (session != null) {
                    break;
                }
            } catch (Exception e) {
                log.warn("Failed to retrieve session {} from GA - retried {} out of {} times. Cause: {}", token,
                        retries, MAX_RETRY, e.getMessage());
            }
            retryInterval = Double.valueOf(retryInterval * (1 + 1.0 * random.nextInt(1000) / 1000)).longValue();
            SleepUtils.sleep(retryInterval);
        }
        if (session != null) {
            validateSession(session);
        }
        return setTenantPid(session);
    }

    private Session setTenantPid(Session session) {
        if (session != null) {
            Tenant tenant = session.getTenant();
            if (tenant != null && tenant.getPid() == null) {
                Tenant tenantWithPid = tenantEntityMgr.findByTenantId(tenant.getId());
                if (tenantWithPid == null) {
                    throw new IllegalStateException("Cannot find pid for in tenant :" + JsonUtils.serialize(tenant));
                }
                session.setTenant(tenantWithPid);
            }
        }
        return session;
    }

}
