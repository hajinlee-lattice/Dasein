package com.latticeengines.pls.provisioning;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

@Component
public class PLSComponentManager {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    public void provisionTenant(Tenant tenant, List<String> adminEmails) {

        incrementTenantName(tenant);

        if (tenantService.hasTenantId(tenant.getId())) {
            tenantService.updateTenant(tenant);
        } else {
            tenantService.registerTenant(tenant);
        }

        for (String email : adminEmails) {
            User user = userService.findByEmail(email);
            if (user == null) {
                UserRegistration uReg = createAdminUserRegistration(email);
                userService.createUser(uReg);
                user = userService.findByEmail(email);
            }
            userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant.getId(), user.getUsername());
        }

    }

    public void discardTenant(Tenant tenant) {
        List<User> users = userService.getUsers(tenant.getId());

        for (User user: users) {
            userService.deleteUser(tenant.getId(), user.getUsername());
        }

        if (tenantService.hasTenantId(tenant.getId())) {
            tenantService.discardTenant(tenant);
        }

    }

    private UserRegistration createAdminUserRegistration(String username) {
        // construct User
        User adminUser = new User();
        adminUser.setUsername(username);
        adminUser.setFirstName("Super");
        adminUser.setLastName("Admin");
        adminUser.setAccessLevel(AccessLevel.SUPER_ADMIN.name());
        adminUser.setActive(true);
        adminUser.setTitle("Lattice PLO");
        adminUser.setEmail(username);

        // construct credential
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword("EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=");

        // construct user registration
        UserRegistration uReg = new UserRegistration();
        uReg.setUser(adminUser);
        uReg.setCredentials(creds);

        return uReg;
    }

    private synchronized void incrementTenantName(Tenant tenant) {
        int duplicateOrdinal = 0;
        String name = tenant.getName();
        Tenant oldTenant = tenantService.findByTenantName(name);
        while (oldTenant != null && !oldTenant.getId().equals(tenant.getId())) {
            duplicateOrdinal++;
            name = tenant.getName() + String.format(" (%03d)", duplicateOrdinal);
            oldTenant = tenantService.findByTenantName(name);
        }
        tenant.setName(name);
    }

}
