package com.latticeengines.security.exposed.globalauth.saml.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserConfigSummary;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.globalauth.impl.GlobalAuthenticationServiceImpl;
import com.latticeengines.security.exposed.globalauth.saml.SamlGlobalAuthenticationService;

@Component("samlGlobalAuthenticationService")
public class SamlGlobalAuthenticationServiceImpl extends GlobalAuthenticationServiceImpl
        implements SamlGlobalAuthenticationService {

    @Autowired
    private GlobalAuthTenantEntityMgr gaTenantEntityMgr;

    @Override
    public Ticket externallyAuthenticated(String emailAddress, String tenantDeploymentId) {
        try {
            GlobalAuthTenant tenant = gaTenantEntityMgr.findByTenantId(tenantDeploymentId);
            if (tenant == null) {
                throw new Exception("The specified Tenant doesn't exists: " + tenantDeploymentId);
            }

            GlobalAuthUser user = gaUserEntityMgr.findByEmail(emailAddress);
            return validateUserAndCreateTicket(user, tenant);

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18171, e, new String[] { emailAddress });
        }

    }

    protected Ticket validateUserAndCreateTicket(GlobalAuthUser user, GlobalAuthTenant gaTenant) throws Exception {
        GlobalAuthUserConfigSummary userConfig = gaUserTenantConfigEntityMgr
                .findUserConfigSummaryByUserIdTenantId(user.getPid(), gaTenant.getPid());

        validateUserForTicketCreation(user, userConfig);

        Ticket ticket = constructTicket(user);

        return ticket;
    }

    @Override
    public boolean discard(Ticket ticket) {
        return super.discard(ticket);
    }

    protected void validateUserForTicketCreation(GlobalAuthUser user, GlobalAuthUserConfigSummary userConfig)
            throws Exception {
        super.validateUserForTicketCreation(user);
        if (userConfig == null || Boolean.FALSE.equals(userConfig.getSsoEnabled())) {
            throw new Exception("SSO login not enabled for user at tenant: " + user.getEmail());
        }
    }

    @Override
    protected void attachValidTenantsToTicket(GlobalAuthUser user, Ticket ticket) {
        // None for SAML Users
        // Tenant Will be attached after Ticket creation
    }

}
