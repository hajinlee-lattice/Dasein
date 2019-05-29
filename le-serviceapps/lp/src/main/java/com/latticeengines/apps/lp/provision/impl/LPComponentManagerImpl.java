package com.latticeengines.apps.lp.provision.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.provision.LPComponentManager;
import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.component.InstallDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

@Component("lpComponentManager")
public class LPComponentManagerImpl implements LPComponentManager {

    private static final Logger log = LoggerFactory.getLogger(LPComponentManagerImpl.class);

    private static final int MIN_PREMIUM_ENRICHMENT_ATTRIBUTES = 0;

    private static final String DEFAUTL_PASSWORD = "admin";

    private static final String DEFAULT_PASSWORD_HASH = "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=";

    @Inject
    private TenantService tenantService;

    @Inject
    private UserService userService;

    @Override
    public void provisionTenant(CustomerSpace space, InstallDocument installDocument) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        String camilleContractId = space.getContractId();

        String PLSTenantId = space.toString();
        log.info(String.format("Provisioning tenant %s", PLSTenantId));

        String tenantDisplayName = installDocument.getProperty(ComponentConstants.Install.TENANT_DISPLAY_NAME);
        String userName = installDocument.getProperty(ComponentConstants.Install.USER_NAME);

        String maxPremiumEnrichAttributesStr = installDocument.getProperty(ComponentConstants.Install.EA_MAX);

        log.info("maxPremiumEnrichAttributesStr is " + maxPremiumEnrichAttributesStr);
        validateEnrichAttributes(maxPremiumEnrichAttributesStr);

        String emailListInJson = installDocument.getProperty(ComponentConstants.Install.SUPER_ADMIN);
        List<String> superAdminEmails = StringUtils.isEmpty(emailListInJson) ? new ArrayList<>() : EmailUtils.parseEmails(emailListInJson);

        emailListInJson = installDocument.getProperty(ComponentConstants.Install.LATTICE_ADMIN);
        List<String> internalAdminEmails = StringUtils.isEmpty(emailListInJson) ? new ArrayList<>() : EmailUtils.parseEmails(emailListInJson);

        emailListInJson = installDocument.getProperty(ComponentConstants.Install.EXTERNAL_ADMIN);
        List<String> externalAdminEmails = StringUtils.isEmpty(emailListInJson) ? new ArrayList<>() : EmailUtils.parseEmails(emailListInJson);

        emailListInJson = installDocument.getProperty(ComponentConstants.Install.THIRD_PARTY_USER);
        List<String> thirdPartyEmails = StringUtils.isEmpty(emailListInJson) ? new ArrayList<>() : EmailUtils.parseEmails(emailListInJson);

        Tenant tenant;
        if (tenantService.hasTenantId(PLSTenantId)) {
            tenant = tenantService.findByTenantId(PLSTenantId);
        } else {
            tenant = new Tenant();
        }
        tenant.setId(PLSTenantId);
        tenant.setName(tenantDisplayName);

        String productsJson = installDocument.getProperty(ComponentConstants.Install.PRODUCTS);
        List<String> productList = JsonUtils.convertList(JsonUtils.deserialize(productsJson, List.class), String.class);

        List<LatticeProduct> products = productList.stream().map(LatticeProduct::fromName).collect(Collectors.toList());
        if (products.contains(LatticeProduct.LPA3) && products.contains(LatticeProduct.CG)) {
            tenant.setUiVersion("4.0");
        } else if (products.contains(LatticeProduct.LPA3) || products.contains(LatticeProduct.PD)) {
            tenant.setUiVersion("3.0");
        } else if (products.contains(LatticeProduct.LPA)) {
            tenant.setUiVersion("2.0");
        }

        String tenantStatus = installDocument.getProperty(ComponentConstants.Install.TENANT_STATUS);
        String tenantType = installDocument.getProperty(ComponentConstants.Install.TENANT_TYPE);
        String contract = installDocument.getProperty(ComponentConstants.Install.CONTRACT);
        if (StringUtils.isNotBlank(tenantStatus)) {
            tenant.setStatus(TenantStatus.valueOf(tenantStatus));
        }
        if (StringUtils.isNotBlank(tenantType)) {
            tenant.setTenantType(TenantType.valueOf(tenantType));
        }
        tenant.setContract(contract);
        log.info("registered tenant's status is " + String.valueOf(tenant.getStatus()) + ", tenant type is "
                + String.valueOf(tenant.getTenantType()));

        provisionTenant(tenant, superAdminEmails, internalAdminEmails, externalAdminEmails, thirdPartyEmails, userName);
    }

    private void provisionTenant(Tenant tenant, List<String> superAdminEmails, List<String> internalAdminEmails,
                                 List<String> externalAdminEmails, List<String> thirdPartyEmails, String userName) {
        if (tenantService.hasTenantId(tenant.getId())) {
            log.info(String.format("Update instead of register during the provision of %s .", tenant.getId()));
            try {
                tenantService.updateTenant(tenant);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028, String.format("Updating tenant %s error. "
                        + "Tenant name possibly already exists.", tenant.getId()), e);
            }
        } else {
            try {
                tenantService.registerTenant(tenant, userName);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028, String.format("Registering tenant %s error. "
                        + "Tenant name possibly already exists.", tenant.getId()), e);
            }
        }

        try {
            Thread.sleep(500); // wait for replication lag
        } catch (Exception e) {
            // ignore
        }

        assignAccessLevelByEmails(userName, internalAdminEmails, AccessLevel.INTERNAL_ADMIN, tenant.getId());
        assignAccessLevelByEmails(userName, superAdminEmails, AccessLevel.SUPER_ADMIN, tenant.getId());
        assignAccessLevelByEmails(userName, externalAdminEmails, AccessLevel.EXTERNAL_ADMIN, tenant.getId());
        assignAccessLevelByEmails(userName, thirdPartyEmails, AccessLevel.THIRD_PARTY_USER, tenant.getId());
    }

    private void assignAccessLevelByEmails(String userName, Collection<String> emails, AccessLevel accessLevel, String tenantId) {
        for (String email : emails) {
            User user;
            try {
                user = userService.findByEmail(email);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028, String.format("Finding user with email %s error.", email),
                        e);
            }
            if (user == null) {
                UserRegistration uReg = createAdminUserRegistration(email, accessLevel);
                try {
                    userService.createUser(userName, uReg);
                    Thread.sleep(500);
                    user = userService.findByEmail(email);
                    if (user == null) {
                        throw new RuntimeException(String.format("Cannot find the new user %s.", email));
                    }
                } catch (Exception e) {
                    throw new LedpException(LedpCode.LEDP_18028, String.format("Adding new user %s error.", email), e);
                }
                updatePasswordBasedOnUsername(user);
            }
            try {
                userService.assignAccessLevel(accessLevel, tenantId, email, userName, null, false);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028,
                        String.format("Assigning Access level to %s error.", email), e);
            }
        }
    }

    private UserRegistration createAdminUserRegistration(String username, AccessLevel accessLevel) {
        // construct User
        User adminUser = new User();
        adminUser.setUsername(username);
        adminUser.setFirstName("Lattice");
        adminUser.setLastName("User");
        adminUser.setAccessLevel(accessLevel.name());
        adminUser.setActive(true);
        adminUser.setTitle("Lattice User");
        adminUser.setEmail(username);

        // construct credential
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DEFAULT_PASSWORD_HASH);

        // construct user registration
        UserRegistration uReg = new UserRegistration();
        uReg.setUser(adminUser);
        uReg.setCredentials(creds);

        return uReg;
    }

    private void updatePasswordBasedOnUsername(User user) {
        // update the default password "admin" to SHA256(CipherUtils(username))
        if (user == null) {
            log.error("User cannot be found.");
            throw new RuntimeException("User %s cannot be found.");
        }
        UserUpdateData userUpdateData = new UserUpdateData();
        userUpdateData.setAccessLevel(user.getAccessLevel());
        userUpdateData.setOldPassword(DigestUtils.sha256Hex(DEFAUTL_PASSWORD));
        // le-pls and le-admin uses the same encoding schema to be in synch
        log.info("The username is " + user.getUsername());
        String newPassword = Base64Utils.encodeBase64WithDefaultTrim(user.getUsername());
        userUpdateData.setNewPassword(DigestUtils.sha256Hex(newPassword));
        userService.updateCredentials(user, userUpdateData);
    }

    private void validateEnrichAttributes(String maxPremiumEnrichAttributesStr) {
        maxPremiumEnrichAttributesStr = maxPremiumEnrichAttributesStr.replaceAll("\"", "");
        int premiumEnrichAttributes = Integer.parseInt(maxPremiumEnrichAttributesStr);
        if (premiumEnrichAttributes < MIN_PREMIUM_ENRICHMENT_ATTRIBUTES) {
            throw new RuntimeException(String.format("PremiumEnrichAttributes: %d is less than %d.",
                    premiumEnrichAttributes, MIN_PREMIUM_ENRICHMENT_ATTRIBUTES));
        }
    }

    @Override
    public void discardTenant(String tenantId) {
        Tenant tenant = tenantService.findByTenantId(tenantId);
        if (tenant != null) {
            discardTenant(tenant);
        }
    }

    private void discardTenant(Tenant tenant) {
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
}
