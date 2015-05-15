package com.latticeengines.pls.provisioning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

@Component
public class PLSComponentManager {

    private static final Log LOGGER = LogFactory.getLog(PLSComponentManager.class);

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    @Autowired
    private TenantConfigService tenantConfigService;

    public void provisionTenant(Tenant tenant, List<String> superAdminEmails, List<String> internalAdminEmails) {

        try {
            if (tenantService.hasTenantId(tenant.getId())) {
                Tenant oldTenant = tenantService.findByTenantId(tenant.getId());
                if (!oldTenant.getName().equals(tenant.getName())) {
                    LOGGER.info(String.format("Update instead of register during the provision of %s .", tenant.getId()));
                    tenantService.updateTenant(tenant);
                }
            } else {
                tenantService.registerTenant(tenant);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18028, "Registering tenant " + tenant.getId() + " error.", e);
        }

        for (String email : superAdminEmails) {
            User user;
            try {
                user = userService.findByEmail(email);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028, "Finding users with email " + email + " error.", e);
            }
            if (user == null) {
                UserRegistration uReg = createAdminUserRegistration(email, AccessLevel.SUPER_ADMIN);
                try {
                    userService.createUser(uReg);
                } catch (Exception e) {
                    throw new LedpException(LedpCode.LEDP_18028, "Adding new user " + email + " error.", e);
                }
            }
            try {
                userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant.getId(), email);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028, "Assigning SuperAdmin role to " + email + " error.", e);
            }
        }

        for (String email : internalAdminEmails) {
            User user;
            try {
                user = userService.findByEmail(email);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028, "Finding users with email " + email + " error.", e);
            }
            if (user == null) {
                UserRegistration uReg = createAdminUserRegistration(email, AccessLevel.INTERNAL_ADMIN);
                try {
                    userService.createUser(uReg);
                } catch (Exception e) {
                    throw new LedpException(LedpCode.LEDP_18028, "Adding new user " + email + " error.", e);
                }
            }
            try {
                userService.assignAccessLevel(AccessLevel.INTERNAL_ADMIN, tenant.getId(), email);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028, "Assigning LatticeAdmin role to " + email + " error.", e);
            }
        }

    }

    public void provisionTenant(CustomerSpace space, DocumentDirectory configDir) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        String camilleContractId = space.getContractId();
        String camilleSpaceId = space.getSpaceId();

        String PLSTenantId = String.format("%s.%s.%s", camilleContractId, camilleTenantId, camilleSpaceId);
        LOGGER.info(String.format("Provisioning tenant %s", PLSTenantId));

        TenantDocument tenantDocument = tenantConfigService.getTenantDocument(PLSTenantId);
        String tenantName = tenantDocument.getTenantInfo().properties.displayName;

        String emailListInJson;
        try {
            emailListInJson = configDir.get("/SuperAdminEmails").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse input configuration", e);
        }
        List<String> superAdminEmails = parseEmails(emailListInJson);

        try {
            emailListInJson = configDir.get("/LatticeAdminEmails").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse input configuration", e);
        }
        List<String> internalAdminEmails = parseEmails(emailListInJson);

        Tenant tenant = new Tenant();
        tenant.setId(PLSTenantId);
        tenant.setName(tenantName);

        provisionTenant(tenant, superAdminEmails, internalAdminEmails);
    }

    private static List<String> parseEmails(String emailsInJson) {
        List<String> adminEmails = new ArrayList<>();

        try {
            ObjectMapper mapper = new ObjectMapper();
            String unescaped = StringEscapeUtils.unescapeJava(emailsInJson);
            JsonNode aNode = mapper.readTree(unescaped);
            if (!aNode.isArray()) {
                throw new IOException("AdminEmails suppose to be a list of strings");
            }
            for (JsonNode node : aNode) {
                adminEmails.add(node.asText());
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18028,
                    "Cannot parse AdminEmails to a list of valid emails: " + emailsInJson, e);
        }

        return adminEmails;
    }

    public void discardTenant(Tenant tenant) {
        List<User> users = userService.getUsers(tenant.getId());
        if (users != null) {
            for (User user : users) {
                userService.deleteUser(tenant.getId(), user.getUsername());
            }
        }

        if (tenantService.hasTenantId(tenant.getId())) {
            tenantService.discardTenant(tenant);
        }

    }

    private UserRegistration createAdminUserRegistration(String username, AccessLevel accessLevel) {
        // construct User
        User adminUser = new User();
        adminUser.setUsername(username);
        adminUser.setFirstName("Super");
        adminUser.setLastName("Admin");
        adminUser.setAccessLevel(accessLevel.name());
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

}
