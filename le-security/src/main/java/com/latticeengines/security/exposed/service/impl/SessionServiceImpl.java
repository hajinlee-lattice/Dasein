package com.latticeengines.security.exposed.service.impl;

import java.util.Collections;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.cache.CacheName;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionServiceImpl.class);

    private static final Integer MAX_RETRY = 3;
    private static final Long RETRY_INTERVAL_MSEC = 200L;
    private static Random random = new Random(System.currentTimeMillis());
    private static Long retryIntervalMsec = 200L;
    private static final String AUTH_ROUTE_SSO = "SSO";
    private static final String AUTH_ROUTE_GA = "GA";

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private SamlGlobalAuthenticationService samlGlobalAuthenticationService;

    @Autowired
    private GlobalAuthTenantEntityMgr gaTenantEntityMgr;

    @Override
    public Ticket authenticate(Credentials credentials) {
        Ticket ticket = globalAuthenticationService.authenticateUser(credentials.getUsername().toLowerCase(),
                credentials.getPassword());
        ticket.setAuthenticationRoute(AUTH_ROUTE_GA);
        return ticket;
    }

    @Override
    public Session attchSamlUserToTenant(String userName, String tenantDeploymentId) {
        Ticket ticket = samlGlobalAuthenticationService.externallyAuthenticated(userName, tenantDeploymentId);
        ticket.setAuthenticationRoute(AUTH_ROUTE_SSO);
        GlobalAuthTenant gaTenant = gaTenantEntityMgr.findByTenantId(tenantDeploymentId);

        // Update Tenant info into Ticket
        Tenant tenant = new Tenant();
        tenant.setId(gaTenant.getId());
        tenant.setName(gaTenant.getName());
        // TODO - jaya to fetch tenant from pls multitenant for correct uiVersion
        // as it is needed by UI
        tenant.setUiVersion("3.0");

        ticket.setTenants(Collections.singletonList(tenant));

        Session session = attach(ticket);
        session.setAuthenticationRoute(AUTH_ROUTE_SSO);
        return session;
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
            session.setAuthenticationRoute(AUTH_ROUTE_GA);
        }
        return session;
    }

    @Override
    public Session retrieve(Ticket ticket) {
        return retrieve(ticket.getData());
    }

    @Override
    public void logout(Ticket ticket) {
        globalAuthenticationService.discard(ticket);
    }

    @Override
    @CacheEvict(key = "#token", condition = "#result == true")
    public boolean clearCacheIfNecessary(String tenantId, String token) {
        Session session = retrieve(token);
        Tenant tenant = session.getTenant();
        if (tenant == null || !tenant.getId().equals(tenantId)) {
            LOGGER.info(String.format(
                    "Clearing cache for ticket %s because client thinks that tenant is %s and our cache has %s", token,
                    tenantId, tenant != null ? tenant.getId() : ""));
            return true;
        }
        return false;
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
                LOGGER.warn("Failed to retrieve session " + token + " from GA - retried " + retries + " out of "
                        + MAX_RETRY + " times");
            }
            try {
                retryInterval = new Double(retryInterval * (1 + 1.0 * random.nextInt(1000) / 1000)).longValue();
                Thread.sleep(retryInterval);
            } catch (Exception e) {
                // ignore
            }
        }

        return session;
    }

}
