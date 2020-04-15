package com.latticeengines.apps.lp.provision.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.lp.provision.PLSComponentManager;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.component.InstallDocument;
import com.latticeengines.domain.exposed.dcp.idaas.ProductRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.util.ValidateEnrichAttributesUtils;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.service.IDaaSService;
import com.latticeengines.security.service.impl.IDaaSServiceImpl;
import com.latticeengines.security.service.impl.IDaaSUser;

@Component("lpComponentManager")
public class PLSComponentManagerImpl implements PLSComponentManager {

    private static final Logger log = LoggerFactory.getLogger(PLSComponentManagerImpl.class);

    private static final String DEFAULT_PASSWORD = "admin";

    private static final String DEFAULT_PASSWORD_HASH = "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=";

    @Inject
    private TenantService tenantService;

    @Inject
    private UserService userService;

    @Inject
    private BatonService batonService;

    @Inject
    private IDaaSService iDaaSService;

    @Override
    public void provisionTenant(CustomerSpace space, InstallDocument installDocument) {
        // get tenant information
        String PLSTenantId = space.toString();
        log.info(String.format("Provisioning tenant %s", PLSTenantId));

        String tenantDisplayName = installDocument.getProperty(ComponentConstants.Install.TENANT_DISPLAY_NAME);
        String userName = installDocument.getProperty(ComponentConstants.Install.USER_NAME);

        String maxPremiumEnrichAttributesStr = installDocument.getProperty(ComponentConstants.Install.EA_MAX);

        log.info("maxPremiumEnrichAttributesStr is " + maxPremiumEnrichAttributesStr);
        ValidateEnrichAttributesUtils.validateEnrichAttributes(maxPremiumEnrichAttributesStr);

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
        setTenantInfo(tenant, products);
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
        log.info("registered tenant's status is " + tenant.getStatus() + ", tenant type is " + tenant.getTenantType());

        provisionTenant(tenant, superAdminEmails, internalAdminEmails, externalAdminEmails, thirdPartyEmails, userName);
    }

    private void setTenantInfo(Tenant tenant, List<LatticeProduct> products) {
        if (products.contains(LatticeProduct.LPA3) && products.contains(LatticeProduct.CG)) {
            tenant.setUiVersion("4.0");
        } else if (products.contains(LatticeProduct.LPA3) && products.contains(LatticeProduct.DCP)) {
            tenant.setUiVersion("4.0");
        } else if (products.contains(LatticeProduct.LPA3) || products.contains(LatticeProduct.PD)) {
            tenant.setUiVersion("3.0");
        } else if (products.contains(LatticeProduct.LPA)) {
            tenant.setUiVersion("2.0");
        }

        if (products.contains(LatticeProduct.CG) && products.contains(LatticeProduct.DCP)) {
            tenant.setEntitledApps("Lattice,DnB");
        } else if (products.contains(LatticeProduct.DCP)) {
            tenant.setEntitledApps("DnB");
        } else {
            tenant.setEntitledApps("Lattice");
        }
    }

    @VisibleForTesting
    void provisionTenant(Tenant tenant, List<String> superAdminEmails, List<String> internalAdminEmails,
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
        // wait for replication lag
        SleepUtils.sleep(500);
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
        userUpdateData.setOldPassword(DigestUtils.sha256Hex(DEFAULT_PASSWORD));
        // le-pls and le-admin uses the same encoding schema to be in synch
        log.info("The username is " + user.getUsername());
        String newPassword = Base64Utils.encodeBase64WithDefaultTrim(user.getUsername());
        userUpdateData.setNewPassword(DigestUtils.sha256Hex(newPassword));
        userService.updateCredentials(user, userUpdateData);
    }

    @Override
    public void discardTenant(String tenantId) {
        Tenant tenant = tenantService.findByTenantId(tenantId);
        if (tenant != null) {
            discardTenant(tenant);
        }
    }

    private List<LatticeProduct> getProducts(CustomerSpace customerSpace) {
        try {
            TenantDocument tenantDocument = batonService.getTenant(customerSpace.getContractId(), customerSpace.getTenantId());
            SpaceConfiguration spaceConfiguration = tenantDocument.getSpaceConfig();
            return spaceConfiguration.getProducts();
        } catch (Exception e) {
            log.error("Failed to get product list of tenant " + customerSpace.toString(), e);
            return new ArrayList<>();
        }
    }

    private TenantDocument getTenantDocument(CustomerSpace customerSpace) {
        try {
            return batonService.getTenant(customerSpace.getContractId(), customerSpace.getTenantId());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18034, e);
        }
    }

    @Override
    public void provisionTenant(CustomerSpace space, DocumentDirectory configDir) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        String camilleContractId = space.getContractId();
        String camilleSpaceId = space.getSpaceId();
        String PLSTenantId = String.format("%s.%s.%s", camilleContractId, camilleTenantId, camilleSpaceId);
        log.info(String.format("Provisioning tenant %s", PLSTenantId));

        TenantDocument tenantDocument;
        try {
            tenantDocument = getTenantDocument(space);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18028, String.format("Getting tenant document error."), e);
        }
        String tenantName = tenantDocument.getTenantInfo().properties.displayName;
        String userName = tenantDocument.getTenantInfo().properties.userName;

        String maxPremiumEnrichAttributesStr;
        try {
            maxPremiumEnrichAttributesStr = configDir.get("/EnrichAttributesMaxNumber").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse input configuration", e);
        }
        log.info("maxPremiumEnrichAttributesStr is " + maxPremiumEnrichAttributesStr);
        ValidateEnrichAttributesUtils.validateEnrichAttributes(maxPremiumEnrichAttributesStr);

        String emailListInJson;
        try {
            emailListInJson = configDir.get("/SuperAdminEmails").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse input configuration", e);
        }
        List<String> superAdminEmails = EmailUtils.parseEmails(emailListInJson);

        try {
            emailListInJson = configDir.get("/LatticeAdminEmails").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse input configuration", e);
        }
        List<String> internalAdminEmails = EmailUtils.parseEmails(emailListInJson);
        // add get external Admin Emails
        try {
            emailListInJson = configDir.get("/ExternalAdminEmails").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse input configuration", e);
        }
        List<String> externalAdminEmails = EmailUtils.parseEmails(emailListInJson);

        try {
            emailListInJson = configDir.get("/ThirdPartyUserEmails").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse input configuration", e);
        }
        List<String> thirdPartyEmails = EmailUtils.parseEmails(emailListInJson);

        String usersInJson ;
        try {
            usersInJson = configDir.get("/IDaaSUsers").getDocument().getData();
        } catch (Exception e) {
            usersInJson = "[]";
        }
        log.info("IDaaS users are: " + usersInJson);
        List<IDaaSUser> iDaaSUsers = JsonUtils.convertList(JsonUtils.deserialize(usersInJson, List.class),
                IDaaSUser.class);
        OperateIDaaSUsers(iDaaSUsers, superAdminEmails, externalAdminEmails);

        Tenant tenant;
        if (tenantService.hasTenantId(PLSTenantId)) {
            tenant = tenantService.findByTenantId(PLSTenantId);
        } else {
            tenant = new Tenant();
        }
        tenant.setId(PLSTenantId);
        tenant.setName(tenantName);
        List<LatticeProduct> products = getProducts(space);
        setTenantInfo(tenant, products);
        try {
            TenantInfo info = TenantLifecycleManager.getInfo(camilleContractId, camilleTenantId);
            if (StringUtils.isNotBlank(info.properties.status)) {
                // change to inactive to avoid user visit tenant eagerly
                tenant.setStatus(TenantStatus.INACTIVE);
            }
            if (StringUtils.isNotBlank(info.properties.tenantType)) {
                tenant.setTenantType(TenantType.valueOf(info.properties.tenantType));
            }
            tenant.setContract(info.properties.contract);
            log.info("registered tenant's status is " + tenant.getStatus() + ", tenant type is "
                    + tenant.getTenantType());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18028, "Failed to retrieve tenants properties", e);
        }
        provisionTenant(tenant, superAdminEmails, internalAdminEmails, externalAdminEmails, thirdPartyEmails, userName);
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

    private void OperateIDaaSUsers(List<IDaaSUser> iDaaSUsers, List<String> superAdminEmails,
                                   List<String> externalAdminEmails) {
        for (IDaaSUser user : iDaaSUsers) {
            String email = user.getEmailAddress();
            if (iDaaSService.getIDaaSUser(email) == null) {
                iDaaSService.createIDaaSUser(user);
            } else {
                // add product access and default role to user when user already exists in IDaaS
                iDaaSService.addProductAccessToUser(constructProductRequest(user.getEmailAddress()));
            }
            if (EmailUtils.isInternalUser(email)) {
                superAdminEmails.add(email.toLowerCase());
            } else {
                externalAdminEmails.add(email.toLowerCase());
            }
        }
    }

    private ProductRequest constructProductRequest(String email) {
        ProductRequest request = new ProductRequest();
        request.setEmailAddress(email);
        request.setRequestor(IDaaSServiceImpl.DCP_PRODUCT);
        request.setProducts(Collections.singletonList(IDaaSServiceImpl.DCP_PRODUCT));
        return request;
    }

}
