package com.latticeengines.apps.lp.provision.impl;

import java.io.InputStream;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.lp.provision.PLSComponentManager;
import com.latticeengines.auth.exposed.service.GlobalAuthSubscriptionService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.component.InstallDocument;
import com.latticeengines.domain.exposed.dcp.idaas.IDaaSUser;
import com.latticeengines.domain.exposed.dcp.idaas.SubscriberDetails;
import com.latticeengines.domain.exposed.dcp.vbo.VboUserSeatUsageEvent;
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
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.monitor.tracing.TracingTags;
import com.latticeengines.monitor.util.TracingUtils;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TeamService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.service.IDaaSService;
import com.latticeengines.security.service.VboService;

import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

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

    @Inject
    private VboService vboService;

    @Inject
    private TeamService teamService;

    @Inject
    private S3Service s3Service;

    @Inject
    private GlobalAuthSubscriptionService subscriptionService;

    @Inject
    private EmailService emailService;

    @Value("${aws.customer.s3.bucket}")
    private String customersBucket;

    @Value("${common.dcp.public.url}")
    private String dcpPublicUrl;

    @Value("${aws.s3.bucket}")
    private String tenantConfigBucket;

    @Value("${aws.tenant.configuration.s3.folder}")
    private String tenantConfigFolder;

    @Value("${aws.tenant.configuration.dcp.admin.email}")
    private String dcpAdminEmailsConfig;

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
        List<IDaaSUser> iDaaSUsers = new ArrayList<>();
        if (products.contains(LatticeProduct.DCP)) {
            List<String> dcpAdminEmails = getDCPSuperAdminEmails();
            if (CollectionUtils.isNotEmpty(dcpAdminEmails)) {
                for (String dcpAdminEmail: dcpAdminEmails) {
                    IDaaSUser iDaaSUser = iDaaSService.getIDaaSUser(dcpAdminEmail);
                    if (iDaaSUser != null) {
                        iDaaSUsers.add(iDaaSUser);
                    } else {
                        log.warn("Cannot find IDaaS user: " + dcpAdminEmail);
                    }
                }
            }
        }
        List<IDaaSUser> retrievedUsers = OperateIDaaSUsers(iDaaSUsers, superAdminEmails, externalAdminEmails, tenantDisplayName);
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

        provisionTenant(tenant, superAdminEmails, internalAdminEmails, externalAdminEmails, thirdPartyEmails,
                retrievedUsers, userName);
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
                         List<String> externalAdminEmails, List<String> thirdPartyEmails, List<IDaaSUser> iDaaSUsers,
                         String userName) {
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

        SubscriberDetails iDaaSDetails = null;
        if (tenant.getSubscriberNumber() != null)
            iDaaSDetails = iDaaSService.getSubscriberDetails(tenant.getSubscriberNumber());

        // wait for replication lag
        SleepUtils.sleep(500);
        assignAccessLevelByEmails(userName, internalAdminEmails, AccessLevel.INTERNAL_ADMIN, tenant.getId(), null, null);
        assignAccessLevelByEmails(userName, superAdminEmails, AccessLevel.SUPER_ADMIN, tenant.getId(), iDaaSUsers, null);
        assignAccessLevelByEmails(userName, externalAdminEmails, AccessLevel.EXTERNAL_ADMIN, tenant.getId(), iDaaSUsers, iDaaSDetails);
        assignAccessLevelByEmails(userName, thirdPartyEmails, AccessLevel.THIRD_PARTY_USER, tenant.getId(), null, iDaaSDetails);
        MultiTenantContext.setTenant(tenant);
        teamService.createDefaultTeam();
    }

    private void assignAccessLevelByEmails(String userName, Collection<String> emails, AccessLevel accessLevel,
                                           String tenantId, List<IDaaSUser> iDaaSUsers, SubscriberDetails details) {
        Map<String, IDaaSUser> emailToIDaaSUser = iDaaSUsers == null ? new HashMap<>() :
                iDaaSUsers.stream().collect(Collectors.toMap(IDaaSUser::getEmailAddress, Function.identity()));
        for (String email : emails) {
            User user;
            try {
                user = userService.findByEmail(email);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18028, String.format("Finding user with email %s error.", email),
                        e);
            }
            if (user == null) {
                IDaaSUser iDaaSUser = emailToIDaaSUser.get(email);
                UserRegistration uReg = createAdminUserRegistration(email, accessLevel, iDaaSUser);
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

            if (accessLevel == AccessLevel.EXTERNAL_ADMIN || accessLevel == AccessLevel.THIRD_PARTY_USER) {
                VboUserSeatUsageEvent usageEvent = new VboUserSeatUsageEvent();

                Tracer tracer = GlobalTracer.get();
                Span span = tracer.activeSpan();
                if (span != null)
                    usageEvent.setPOAEID(span.context().toTraceId());
                usageEvent.setTimeStamp(ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
                usageEvent.setFeatureURI(VboUserSeatUsageEvent.FeatureURI.STCT);

                if (details != null) {
                    usageEvent.setSubscriberID(details.getSubscriberNumber());
                    usageEvent.setSubjectCountry(details.getAddress().getCountryCode());
                    usageEvent.setSubscriberCountry(details.getAddress().getCountryCode());
                    usageEvent.setContractTermStartDate(details.getEffectiveDate());
                    usageEvent.setContractTermEndDate(details.getExpirationDate());
                }

                try {
                    vboService.sendUserUsageEvent(usageEvent);
                } catch (Exception e) {
                    log.error("Could not send usage report: ", e);
                    log.error("Failed usage report stack trace: " + ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    private UserRegistration createAdminUserRegistration(String username, AccessLevel accessLevel, IDaaSUser iDaaSUser) {
        // construct User
        User adminUser = new User();
        if (iDaaSUser != null) {
            log.info("create ga user {} using IDaaS info", username);
            adminUser.setFirstName(iDaaSUser.getFirstName());
            adminUser.setLastName(iDaaSUser.getLastName());
        } else {
            adminUser.setFirstName("Lattice");
            adminUser.setLastName("User");
        }
        adminUser.setUsername(username);
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
            String id = CustomerSpace.parse(tenant.getId()).getTenantId();
            log.info("Cleanup s3 directory for tenant {}", id);
            s3Service.cleanupDirectory(customersBucket, id);
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
            throw new LedpException(LedpCode.LEDP_18028, "Getting tenant document error.", e);
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

        String usersInJson;
        boolean hasNode = false;
        try {
            usersInJson = configDir.get("/IDaaSUsers").getDocument().getData();
            hasNode = true;
        } catch (Exception e) {
            usersInJson = "[]";
        }
        log.info("IDaaS users are: " + usersInJson);
        List<IDaaSUser> iDaaSUsers = JsonUtils.convertList(JsonUtils.deserialize(usersInJson, List.class),
                IDaaSUser.class);

        List<LatticeProduct> products = getProducts(space);
        if (products.contains(LatticeProduct.DCP)) {
            List<String> dcpAdminEmails = getDCPSuperAdminEmails();
            if (CollectionUtils.isNotEmpty(dcpAdminEmails)) {
                for (String dcpAdminEmail: dcpAdminEmails) {
                    IDaaSUser iDaaSUser = iDaaSService.getIDaaSUser(dcpAdminEmail);
                    if (iDaaSUser != null) {
                        iDaaSUsers.add(iDaaSUser);
                    } else {
                        log.warn("Cannot find IDaaS user: " + dcpAdminEmail);
                    }
                }
            }
        }

        Map<String, String> parentCtxMap = null;
        try {
            String contextStr = configDir.get("/TracingContext").getDocument().getData();
            Map<?, ?> rawMap = JsonUtils.deserialize(contextStr, Map.class);
            parentCtxMap = JsonUtils.convertMap(rawMap, String.class, String.class);
        } catch (Exception e) {
            log.warn("no tracing context node exist {}.", e.getMessage());
        }
        Tracer tracer = GlobalTracer.get();
        SpanContext parentCtx = TracingUtils.getSpanContext(parentCtxMap);

        List<IDaaSUser> retrievedUsers = OperateIDaaSUsers(iDaaSUsers, superAdminEmails, externalAdminEmails, tenantName);

        // Update IDaaS users node with email sent times; to be stored in Camille
        if (hasNode) {
            String usersWithEmailTime = JsonUtils.serialize(retrievedUsers);
            configDir.get("/IDaaSUsers").getDocument().setData(usersWithEmailTime);
        }

        Tenant tenant;
        if (tenantService.hasTenantId(PLSTenantId)) {
            tenant = tenantService.findByTenantId(PLSTenantId);
        } else {
            tenant = new Tenant();
        }
        tenant.setId(PLSTenantId);
        tenant.setName(tenantName);
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

        String subscriberNumber;
        try {
            subscriberNumber = configDir.get("/SubscriberNumber").getDocument().getData();
            tenant.setSubscriberNumber(subscriberNumber);
        } catch (Exception e) {
            log.info("no node exist {}.", e.getMessage());
        }
        long start = System.currentTimeMillis() * 1000;
        Span provisionSpan = null;
        try (Scope scope = startProvisionSpan(parentCtx, PLSTenantId, start)) {
            provisionSpan = tracer.activeSpan();
            provisionSpan.log("Provisioning lp component for tenant " + PLSTenantId);
            provisionSpan.log(new ImmutableMap.Builder<String, String>()
                    .put("superAdmin", JsonUtils.serialize(superAdminEmails))
                    .put("internalAdmin", JsonUtils.serialize(internalAdminEmails))
                    .put("externalAdmin", JsonUtils.serialize(externalAdminEmails))
                    .put("thirdParty", JsonUtils.serialize(thirdPartyEmails))
                    .put("iDaaSUsers", JsonUtils.serialize(retrievedUsers))
                    .put("userName", StringUtils.isBlank(userName) ? "Empty userName" : userName)
                    .build());
            provisionTenant(tenant, superAdminEmails, internalAdminEmails, externalAdminEmails, thirdPartyEmails,
                    retrievedUsers, userName);
        } finally {
            TracingUtils.finish(provisionSpan);
        }
        try {
            emailListInJson = configDir.get("/SubscriptionEmails").getDocument().getData();
        } catch (Exception e) {
            log.info("no SubscriptionEmails node exist {}.", e.getMessage());
        }
        List<String> subscriptionEmails = EmailUtils.parseEmails(emailListInJson);
        subscriptionService.createByEmailsAndTenantId(new HashSet<>(subscriptionEmails), tenant.getId());
    }

    private Scope startProvisionSpan(SpanContext parentContext, String tenantId, long startTimeStamp) {
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("PLSComponent Bootstrap") //
                .addReference(References.FOLLOWS_FROM, parentContext) //
                .withTag(TracingTags.TENANT_ID, tenantId) //
                .withStartTimestamp(startTimeStamp) //
                .start();
        return tracer.activateSpan(span);
    }


    private void discardTenant(Tenant tenant) {
        List<User> users = userService.getUsers(tenant.getId());
        if (users != null) {
            for (User user : users) {
                userService.deleteUser(tenant.getId(), user.getUsername(), false);
            }
        }
        if (tenantService.hasTenantId(tenant.getId())) {
            tenantService.discardTenant(tenant);
        }
    }

    private List<String> getDCPSuperAdminEmails() {
        String dcpEmailS3File = tenantConfigFolder + "/" + dcpAdminEmailsConfig;
        if (s3Service.objectExist(tenantConfigBucket, dcpEmailS3File)) {
            log.info(String.format("Start getting DCP admin email from %s : %s", tenantConfigBucket, dcpEmailS3File));
            try {
                InputStream dcpEmailStream = s3Service.readObjectAsStream(tenantConfigBucket, dcpEmailS3File);
                return JsonUtils.convertList(JsonUtils.deserialize(dcpEmailStream, List.class), String.class);
            } catch (Exception e) {
                log.error("Cannot parse DCP default admin email file. Exception: {}", e.getMessage());
                return Collections.emptyList();
            }
        } else {
            log.warn("DCP default admin email file is missing.");
        }
        return Collections.emptyList();
    }

    private List<IDaaSUser> OperateIDaaSUsers(List<IDaaSUser> iDaaSUsers, List<String> superAdminEmails,
                                   List<String> externalAdminEmails, String tenantName) {
        log.info("Operating IDaaS users");
        List<IDaaSUser> createdUsers = new ArrayList<>();
        for (IDaaSUser user : iDaaSUsers) {
            String email = user.getEmailAddress();

            User createUserData = new User();
            createUserData.setEmail(user.getEmailAddress().toLowerCase());
            createUserData.setFirstName(user.getFirstName());
            createUserData.setLastName(user.getLastName());
            createUserData.setUsername(user.getUserName());
            createUserData.setPhoneNumber(user.getPhoneNumber());
            IDaaSUser createdUser = userService.createIDaaSUser(createUserData, user.getSubscriberNumber());

            if (EmailUtils.isInternalUser(email)) {
                superAdminEmails.add(email.toLowerCase());
            } else {
                externalAdminEmails.add(email.toLowerCase());
            }

            if (createdUser != null) {
                String welcomeUrl = dcpPublicUrl;
                if (createdUser.getInvitationLink() != null) {
                    welcomeUrl = createdUser.getInvitationLink();
                }

                // Add info needed for VBO callback
                createdUser.setInvitationSentTime(emailService.sendDCPWelcomeEmail(createdUser, tenantName, welcomeUrl));
                createdUser.setSubscriberNumber(user.getSubscriberNumber());

                createdUsers.add(createdUser);
            } else {
                user.setEmailAddress(email.toLowerCase());
                createdUsers.add(user);
            }
        }
        return createdUsers;
    }

}
