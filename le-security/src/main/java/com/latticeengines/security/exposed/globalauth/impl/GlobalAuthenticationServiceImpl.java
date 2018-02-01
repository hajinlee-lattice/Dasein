package com.latticeengines.security.exposed.globalauth.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthAuthenticationEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantConfigEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserConfigSummary;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.util.GlobalAuthPasswordUtils;

@Component("globalAuthenticationService")
@CacheConfig(cacheNames = CacheName.Constants.SessionCacheName)
public class GlobalAuthenticationServiceImpl extends GlobalAuthenticationServiceBaseImpl
        implements GlobalAuthenticationService {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthenticationServiceImpl.class);

    @Autowired
    private GlobalAuthAuthenticationEntityMgr gaAuthenticationEntityMgr;

    @Autowired
    protected GlobalAuthUserEntityMgr gaUserEntityMgr;

    @Autowired
    protected GlobalAuthUserTenantConfigEntityMgr gaUserTenantConfigEntityMgr;

    @Autowired
    protected GlobalAuthTicketEntityMgr gaTicketEntityMgr;

    @Override
    public synchronized Ticket authenticateUser(String user, String password) {
        try {
            log.info(String.format("Authenticating user %s against Global Auth.", user));
            return globalAuthAuthenticateUser(user, password);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18001, e, new String[] { user });
        }

    }

    private Ticket globalAuthAuthenticateUser(String username, String password) throws Exception {
        GlobalAuthAuthentication latticeAuthenticationData = gaAuthenticationEntityMgr.findByUsername(username);
        if (latticeAuthenticationData == null) {
            throw new Exception("The specified user doesn't exists");
        }

        boolean isActive = latticeAuthenticationData.getGlobalAuthUser().getIsActive();
        if (!isActive) {
            throw new Exception("The user is inactive!");
        }
        Ticket ticket = authenticate(username, password);
        if (ticket != null) {
            return ticket;
        }

        log.error("The credentials provided for login are incorrect.");
        return null;
    }

    private Ticket authenticate(String username, String password) {
        GlobalAuthAuthentication latticeAuthenticationData = gaAuthenticationEntityMgr.findByUsername(username);
        if (latticeAuthenticationData == null) {
            return null;
        }
        if (!latticeAuthenticationData.getPassword().equals(GlobalAuthPasswordUtils.encryptPassword(password))) {
            return null;
        }

        Ticket ticket = constructTicket(latticeAuthenticationData.getGlobalAuthUser());
        ticket.setMustChangePassword(latticeAuthenticationData.getMustChangePassword());
        ticket.setPasswordLastModified(latticeAuthenticationData.getLastModificationDate().getTime());
        return ticket;
    }

    protected Ticket constructTicket(GlobalAuthUser user) {
        Ticket ticket = new Ticket();
        ticket.setUniqueness(UUID.randomUUID().toString());
        ticket.setRandomness(GlobalAuthPasswordUtils.getSecureRandomString(16));
        GlobalAuthTicket ticketData = new GlobalAuthTicket();
        ticketData.setUserId(user.getPid());
        ticketData.setTicket(ticket.getData());
        ticketData.setLastAccessDate(new Date(System.currentTimeMillis()));
        
        attachValidTenantsToTicket(user, ticket);
        
        gaTicketEntityMgr.create(ticketData);
        
        return ticket;
    }

    protected void attachValidTenantsToTicket(GlobalAuthUser user, Ticket ticket) {
        GlobalAuthUser userData = gaUserEntityMgr.findByUserIdWithTenantRightsAndAuthentications(user.getPid());
        if (userData.getUserTenantRights() != null && userData.getUserTenantRights().size() > 0) {
            Map<String, GlobalAuthTenant> distinctTenants = new HashMap<String, GlobalAuthTenant>();
            for (GlobalAuthUserTenantRight rightData : userData.getUserTenantRights()) {
                if (rightData.getGlobalAuthTenant() != null) {
                    if (!distinctTenants.containsKey(rightData.getGlobalAuthTenant().getId())) {
                        distinctTenants.put(rightData.getGlobalAuthTenant().getId(), rightData.getGlobalAuthTenant());
                    }
                }
            }
            
            // Collect all Tenants where SSO is Enabled and User is forced SSO Login only.
            List<GlobalAuthUserConfigSummary> userConfigSummaryList = gaUserTenantConfigEntityMgr
                    .findUserConfigSummaryByUserId(user.getPid());
            Map<String, GlobalAuthUserConfigSummary> ssoForcedTenantMap = new HashMap<>();
            userConfigSummaryList.forEach(userConfigSummary -> {
                if (userConfigSummary.getSsoEnabled() && userConfigSummary.getForceSsoLogin()) {
                    ssoForcedTenantMap.put(userConfigSummary.getTenantDeploymentId(), userConfigSummary);
                }
            });

            List<Tenant> tenants = new ArrayList<Tenant>();
            for (Entry<String, GlobalAuthTenant> tenantData : distinctTenants.entrySet()) {
                // Exclude SSO forced tenants from Display List
                if (ssoForcedTenantMap.containsKey(tenantData.getKey())) {
                    continue;
                }
                Tenant tenant = new Tenant();
                tenant.setId(tenantData.getKey());
                tenant.setName(tenantData.getValue().getName());
                tenants.add(tenant);
            }
            ticket.setTenants(tenants);
        }
    }

    
    @Override
    public synchronized Ticket externallyAuthenticated(String emailAddress) {
        try {
            log.info(String.format("Retrieving ticket for already authenticated user %s", emailAddress));
            return globalAuthExternallyAuthenticated(emailAddress);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18001, new String[] { emailAddress });
        }
    }

    private Ticket globalAuthExternallyAuthenticated(String emailAddress) throws Exception {
        GlobalAuthUser user = gaUserEntityMgr.findByEmailJoinAuthentication(emailAddress);

        validateUserForTicketCreation(user);

        Ticket ticket = constructTicket(user);
        if (ticket != null) {
            return ticket;
        }

        throw new Exception("The credentials provided for login are incorrect.");
    }

    protected void validateUserForTicketCreation(GlobalAuthUser user) throws Exception {
        if (user == null) {
            throw new Exception("The specified user doesn't exists");
        }

        boolean isActive = user.getIsActive();
        if (!isActive) {
            throw new Exception("The user is inactive!");
        }
    }

    @Override
    @CacheEvict(key = "#ticket.data")
    public synchronized boolean discard(Ticket ticket) {
        try {
            log.info("Discarding ticket " + ticket + " against Global Auth.");

            return globalDiscard(ticket);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18009, e, new String[] { ticket.toString() });
        }

    }

    private boolean globalDiscard(Ticket ticket) {
        GlobalAuthTicket ticketData = gaTicketEntityMgr.findByTicket(ticket.getData());
        if (ticketData != null) {
            gaTicketEntityMgr.delete(ticketData);
        } else {
            return false;
        }
        return true;
    }

}
