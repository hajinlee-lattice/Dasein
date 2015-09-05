package com.latticeengines.pls.provisioning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
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
        if (tenantService.hasTenantId(tenant.getId())) {
            LOGGER.info(String.format("Update instead of register during the provision of %s .", tenant.getId()));
            try {
                tenantService.updateTenant(tenant);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028, String.format("Updating tenant %s error. " +
                        "Tenant name possibly already exists.", tenant.getId()), e);
            }
        } else {
            try {
                tenantService.registerTenant(tenant);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028, String.format("Registrating tenant %s error. " +
                        "Tenant name possibly already exists.", tenant.getId()), e);
            }
        }

        assignAccessLevelByEmails(internalAdminEmails, AccessLevel.INTERNAL_ADMIN, tenant.getId());
        assignAccessLevelByEmails(superAdminEmails, AccessLevel.SUPER_ADMIN, tenant.getId());
    }

    public void provisionTenant(CustomerSpace space, DocumentDirectory configDir) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        String camilleContractId = space.getContractId();
        String camilleSpaceId = space.getSpaceId();

        String PLSTenantId = String.format("%s.%s.%s", camilleContractId, camilleTenantId, camilleSpaceId);
        LOGGER.info(String.format("Provisioning tenant %s", PLSTenantId));

        TenantDocument tenantDocument;
        try {
            tenantDocument = tenantConfigService.getTenantDocument(PLSTenantId);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18028,
                    String.format("Getting tenant document error."), e);
        }
        String tenantName = tenantDocument.getTenantInfo().properties.displayName;

        String emailListInJson;
        try {
            emailListInJson = configDir.get("/SuperAdminEmails").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028,
                    String.format("Cannot parse input configuration"), e);
        }
        List<String> superAdminEmails = parseEmails(emailListInJson);

        try {
            emailListInJson = configDir.get("/LatticeAdminEmails").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028,
                    String.format("Cannot parse input configuration"), e);
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
            throw new LedpException(LedpCode.LEDP_18028, String.format(
                    "Cannot parse AdminEmails to a list of valid emails: %s", emailsInJson), e);
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

    private void assignAccessLevelByEmails(Collection<String> emails, AccessLevel accessLevel, String tenantId) {
        for (String email : emails) {
            User user;
            try {
                user = userService.findByEmail(email);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028,
                        String.format("Finding user with email %s error.", email), e);
            }
            if (user == null) {
                UserRegistration uReg = createAdminUserRegistration(email, accessLevel);
                try {
                    userService.createUser(uReg);
                } catch (Exception e) {
                    throw new LedpException(LedpCode.LEDP_18028,
                            String.format("Adding new user %s error.", email), e);
                }
            }
            try {
                userService.assignAccessLevel(accessLevel, tenantId, email);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028,
                        String.format("Assigning SuperAdmin role to %s error.", email), e);
            }
        }
    }

}
