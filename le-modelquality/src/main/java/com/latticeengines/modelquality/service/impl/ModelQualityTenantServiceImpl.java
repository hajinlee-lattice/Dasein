package com.latticeengines.modelquality.service.impl;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.modelquality.service.ModelQualityTenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

@Component("modelQualityTenantService")
public class ModelQualityTenantServiceImpl implements ModelQualityTenantService {

    private static final Logger log = LoggerFactory.getLogger(ModelQualityTenantServiceImpl.class);

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    @Value("${modelquality.pls.login.tenant}")
    private String tenant;

    @Value("${modelquality.pls.login.username}")
    private String username;

    @Value("${modelquality.pls.login.password}")
    private String password;

    @Value("${modelquality.pls.login.password.hash}")
    private String passwordHash;

    @PostConstruct
    public void bootstrapServiceTenant() {
        try {
            String tenantId = CustomerSpace.parse(tenant).toString();

            if (tenantService.findByTenantId(tenantId) == null) {
                log.info("Could not find ModelQuality service tenant " + tenantId + ". Registering it now.");
                Tenant tenant = new Tenant();
                tenant.setId(tenantId);
                tenant.setName(CustomerSpace.parse(this.tenant).getTenantId());
                tenant.setUiVersion("3.0");
                tenantService.registerTenant(tenant);
                log.info("Successfully created ModelQuality service tenant: " + tenantId + ".");
            }

            if (userService.findByUsername(username) == null) {
                log.info("Could not find modelQuality service user " + username + ". Registering it now.");

                UserRegistration userReg = new UserRegistration();
                userReg.setValidation(false);

                User user = new User();
                user.setFirstName("ModelQuality");
                user.setLastName("Test");
                user.setUsername(username);
                user.setEmail(username);
                user.setActive(true);
                userReg.setUser(user);

                Credentials creds = new Credentials();
                creds.setUsername(username);
                creds.setPassword(passwordHash);
                userReg.setCredentials(creds);

                userService.createUser(null, userReg);
                log.info("Successfully created ModelQuality service user: " + username + ".");
            }

            if (!AccessLevel.SUPER_ADMIN.equals(userService.getAccessLevel(tenantId, username))) {
                userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, tenantId, username);
                log.info("Assigned Super Admin access to " + username + //
                        " for ModelQuality service tenant: " + tenantId + ".");
            }
        } catch (Exception e) {
            // log but ignore so that the startup process does't fail
            log.error(String.format("Failed to create the model quality service tenant/user with exception: \n%s",
                    e.getMessage()));
            log.error("Model quality model runs may fail");
        }
    }

}
