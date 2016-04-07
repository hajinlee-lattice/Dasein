package com.latticeengines.propdata.match.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.propdata.match.service.PropDataTenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

@Component("propDataTenantService")
public class PropDataTenantServiceImpl implements PropDataTenantService {

    private static final Log log = LogFactory.getLog(PropDataTenantServiceImpl.class);

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    private String tenantId = MatchConstants.SERVICE_CUSTOMERSPACE;

    @Value("${propdata.ga.username}")
    private String username;

    @Value("${propdata.ga.password.hash}")
    private String passwordHash;

    @Value("${propdata.ga.password.encrypted}")
    private String password;

    public void bootstrapServiceTenant() {
        if (tenantService.findByTenantId(tenantId) == null) {
            log.info("Could not find propdata service tenant " + tenantId + ". Register it now.");
            Tenant tenant = new Tenant();
            tenant.setId(tenantId);
            tenant.setName("PropData Service Tenant");
            tenant.setUiVersion("2.0");
            tenantService.registerTenant(tenant);
        }

        if (userService.findByUsername(username) == null) {
            log.info("Could not find propdata service user " + username + ". Register it now.");

            UserRegistration userReg = new UserRegistration();
            userReg.setValidation(false);

            User user = new User();
            user.setFirstName("Propdata");
            user.setFirstName("Service");
            user.setUsername(username);
            user.setEmail(username);
            user.setActive(true);
            userReg.setUser(user);

            Credentials creds = new Credentials();
            creds.setUsername(username);
            creds.setPassword(passwordHash);
            userReg.setCredentials(creds);

            userService.createUser(userReg);
        }

        if (!AccessLevel.SUPER_ADMIN.equals(userService.getAccessLevel(tenantId, username))) {
            userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, tenantId, username);
        }
    }

}
