package com.latticeengines.security.service.impl;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.auth.IDaaSExternalSession;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.dcp.idaas.IDaaSResponse;
import com.latticeengines.domain.exposed.dcp.idaas.IDaaSUser;
import com.latticeengines.domain.exposed.dcp.idaas.InvitationLinkResponse;
import com.latticeengines.domain.exposed.dcp.idaas.ProductRequest;
import com.latticeengines.domain.exposed.dcp.idaas.RoleRequest;
import com.latticeengines.domain.exposed.dcp.idaas.SubscriberDetails;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.LoginTenant;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.service.AuthorizationServiceBase;
import com.latticeengines.security.service.IDaaSService;
import com.latticeengines.security.util.LoginUtils;

@Service("iDaasService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class IDaaSServiceImpl extends AuthorizationServiceBase implements IDaaSService {

    private static final Logger log = LoggerFactory.getLogger(IDaaSServiceImpl.class);

    public static final String DCP_PRODUCT = "DnB Connect";
    public static final String DCP_ROLE = "DNB_CONNECT_ACCESS";
    private static final int MAX_RETRIES = 3;

    @Inject
    private IDaaSServiceImpl _self;

    @Inject
    private SessionService sessionService;

    @Inject
    private GlobalAuthTicketEntityMgr gaTicketEntityMgr;

    @Inject
    private GlobalAuthUserEntityMgr gaUserEntityMgr;

    @Inject
    private TenantService tenantService;

    @Value("${remote.idaas.api.url}")
    private String apiUrl;

    @Override
    protected String refreshOAuthTokens(String cacheKey) {
        return _self.getTokenFromIDaaS(clientId);
    }

    @Override
    public LoginDocument login(Credentials credentials) {
        refreshToken();
        LoginDocument doc = new LoginDocument();
        doc.setErrors(Collections.singletonList("Failed to authenticate the user."));
        boolean authenticated = authenticate(credentials);
        if (authenticated) {
            IDaaSUser iDaaSUser = getIDaaSUser(credentials.getUsername());
            boolean hasAccess = hasAccessToApp(iDaaSUser);
            if (hasAccess) {
                String email = iDaaSUser.getEmailAddress().toLowerCase();
                if (StringUtils.isBlank(email)) {
                    email = credentials.getUsername().toLowerCase();
                }
                IDaaSExternalSession externalSession = new IDaaSExternalSession();
                externalSession.setIssuer("IDaaS");
                Ticket ticket = null;
                try {
                    ticket = sessionService.authenticateSamlUser(email, externalSession);
                } catch (Exception e) {
                    log.warn("Failed to generate ticket for external session.", e);
                }
                doc = LoginUtils.generateLoginDoc(ticket, gaUserEntityMgr, gaTicketEntityMgr, tenantService);
                doc = addSubscriberDetails(doc);
            } else {
                doc.setErrors(Collections.singletonList("The user does not have access to this application."));
            }
        }
        return doc;
    }

    /**
     * In each Tenant in the LoginDocument, add SubscriberDetails from IDaaS.  Do this by creating a LoginTenant and replacing the Tenant
     * objects in the LoginDocument with them.
     *
     * @param doc - LoginDocument to add subscriber details to
     * @return LoginDocument that includes subscriber details
     */
    @Override
    public LoginDocument addSubscriberDetails(LoginDocument doc) {
        LoginDocument.LoginResult result = doc.getResult();
        List<Tenant> tenantList = result.getTenants();
        List<Tenant> augmentedTenantList = tenantList.stream().map(this::addSubscriberDetails).collect(Collectors.toList());
        doc.getResult().setTenants(augmentedTenantList);
        return doc;
    }

    /**
     * Create a LoginTenant and add additional information from IDaaS to it.
     * @param tenant - The Tenant to add subscriber details to
     * @return A new LoginTenant object
     */
    private LoginTenant addSubscriberDetails(Tenant tenant) {
        LoginTenant loginTenant = new LoginTenant();
        try {
            BeanUtils.copyProperties(loginTenant, tenant);
        } catch (IllegalAccessException iae) {
            String msg = "IllegalAccessException while trying to copy fields in Tenant to LoginTenant";
            log.error(msg, iae);
        } catch (InvocationTargetException ite) {
            String msg = "InvocationTargetException while trying to copy fields in Tenant to LoginTenant";
            log.error(msg, ite);
        }
        String subscriberNumber = tenant.getSubscriberNumber();
        if (StringUtils.isNotBlank(subscriberNumber)) {
            SubscriberDetails subscriberDetails = _self.getSubscriberDetails(subscriberNumber);
            if (subscriberDetails != null) {
                loginTenant.setCompanyName(subscriberDetails.getCompanyName());
                loginTenant.setDuns(subscriberDetails.getDunsNumber());
                loginTenant.setSubscriptionType(subscriberDetails.getSubscriberType());
                loginTenant.setCountry(subscriberDetails.getAddress() != null ? subscriberDetails.getAddress().getCountryCode() : null);
                loginTenant.setContractStartDate(subscriberDetails.getEffectiveDate());
                loginTenant.setContractEndDate(subscriberDetails.getExpirationDate());
            }
            else {
                log.warn("SubscriberDetails is null for subscriberNumber {}", subscriberNumber);
            }
        }
        else {
            log.info("SubscriberNumber is blank for Tenant {}", tenant.getName());
        }
        return loginTenant;
    }

    private boolean authenticate(Credentials credentials) {
        IDaaSCredentials iDaaSCreds = new IDaaSCredentials();
        iDaaSCreds.setUserName(credentials.getUsername());
        iDaaSCreds.setPassword(credentials.getPassword());
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRIES);
        try {
            return retryTemplate.execute(ctx -> {
                try (PerformanceTimer timer = new PerformanceTimer("Authenticate user against IDaaS.")) {
                    ResponseEntity<JsonNode> response = restTemplate.postForEntity(authenticateUri(), iDaaSCreds,
                            JsonNode.class);
                    if (HttpStatus.OK.equals(response.getStatusCode())) {
                        return true;
                    } else {
                        log.warn("Cannot authenticate user {} against IDaaS: {}", credentials.getUsername(),
                                response.getBody());
                        throw new RuntimeException(JsonUtils.serialize(response.getBody()));
                    }
                } catch (HttpClientErrorException.Unauthorized e) {
                    log.warn("Failed to authenticate user {}: {}", credentials.getUsername(), e.getMessage());
                    return false;
                }
            });
        } catch (Exception e) {
            log.warn("Cannot authenticate user {} against IDaaS", credentials.getUsername(), e);
            return false;
        }
    }

    @Override
    public IDaaSUser getIDaaSUser(String email) {
        refreshToken();
        IDaaSUser user = null;
        try {
            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRIES);
            user = retryTemplate.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("Attempt={} retrying to get IDaaS user for {}", ctx.getRetryCount() + 1, email,
                            ctx.getLastThrowable());
                }
                try (PerformanceTimer timer = new PerformanceTimer(
                        String.format("Check user detail %s in IDaaS.", email))) {
                    log.info("sending request to get IDaaS user {}", email);
                    ResponseEntity<IDaaSUser> response = restTemplate.getForEntity(userUri(email), IDaaSUser.class);
                    return response.getBody();
                } catch (HttpClientErrorException.Unauthorized e) {
                    log.warn("Failed to authenticate user {}: {}", email, e.getMessage());
                    return null;
                }
            });
        } catch (Exception e) {
            log.warn("Failed to check user detail in IDaaS for {}", email, e);
        }
        return user;
    }

    @Override
    public IDaaSUser updateIDaaSUser(IDaaSUser user) {
        refreshToken();
        String email = user.getEmailAddress();
        IDaaSUser returnedUser = null;
        try {
            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRIES);
            returnedUser = retryTemplate.execute(ctx -> {
                try (PerformanceTimer timer = new PerformanceTimer("update user in IDaaS.")) {
                    HttpEntity<IDaaSUser> entity = new HttpEntity<>(user);
                    ResponseEntity<IDaaSUser> responseEntity = restTemplate.exchange(userUri(email), HttpMethod.PUT,
                            entity, IDaaSUser.class);
                    return responseEntity.getBody();
                } catch (HttpClientErrorException.Unauthorized e) {
                    // TODO(penglong) re-evaluate this part after network is setup
                    log.warn("Failed to authenticate user {}: {}", email, e.getMessage());
                    return null;
                }
            });
        } catch (Exception e) {
            log.warn("Failed to update user detail in IDaaS for {}", email, e);
        }
        return returnedUser;
    }

    @Override
    public IDaaSUser createIDaaSUser(IDaaSUser user) {
        user.setAppName(DCP_PRODUCT);
        user.setSource(DCP_PRODUCT);
        user.setRequestor(DCP_PRODUCT);

        refreshToken();
        String email = user.getEmailAddress();
        IDaaSUser returnedUser = null;
        try {
            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRIES);
            returnedUser = retryTemplate.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("Attempt={} retrying to create IDaaS user for {}", ctx.getRetryCount() + 1, email,
                            ctx.getLastThrowable());
                }
                try (PerformanceTimer timer = new PerformanceTimer(String.format("create user %s in IDaaS.", email))) {
                    log.info("sending request to create IDaaS user {}", email);
                    ResponseEntity<IDaaSUser> responseEntity = restTemplate.postForEntity(createUserUri(), user,
                            IDaaSUser.class);
                    return responseEntity.getBody();
                } catch (HttpClientErrorException.Unauthorized e) {
                    // TODO(penglong) re-evaluate this part after network is setup
                    log.warn("Failed to authenticate user {}: {}", email, e.getMessage());
                    return null;
                }
            });
        } catch (Exception e) {
            log.warn("Failed to create user detail in IDaaS for {}", email, e);
        }
        // get invitation link for user if new user was created successfully
        if (returnedUser != null) {
            InvitationLinkResponse invitationLinkResponse = getUserInvitationLink(returnedUser.getEmailAddress());
            if (invitationLinkResponse != null) {
                returnedUser.setInvitationLink(invitationLinkResponse.getInviteLink());
            }
        }
        return returnedUser;
    }

    @Override
    public IDaaSResponse addProductAccessToUser(ProductRequest request) {
        request.setRequestor(DCP_PRODUCT);
        request.setProducts(Collections.singletonList(DCP_PRODUCT));

        refreshToken();
        IDaaSResponse response = null;
        String email = request.getEmailAddress();
        try {
            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRIES);
            response = retryTemplate.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("Attempt={} retrying to add product access to IDaaS user {}", ctx.getRetryCount() + 1,
                            email, ctx.getLastThrowable());
                }
                try (PerformanceTimer timer = new PerformanceTimer(
                        String.format("add product access to user %s.", email))) {
                    log.info("sending request to add product access to IDaaS user {}", email);
                    HttpEntity<ProductRequest> entity = new HttpEntity<>(request);
                    ResponseEntity<IDaaSResponse> responseEntity = restTemplate.exchange(addProductUri(email),
                            HttpMethod.PUT, entity, IDaaSResponse.class);
                    return responseEntity.getBody();
                } catch (Exception e) {
                    // TODO re-evaluate this part after network is setup
                    log.warn("Failed to execute api", e);
                    return null;
                }
            });
        } catch (Exception e) {
            log.warn("Failed to add product access to user {}", email);
        }
        return response;
    }

    @Override
    public IDaaSResponse addRoleToUser(RoleRequest request) {
        refreshToken();
        IDaaSResponse response = null;
        String email = request.getEmailAddress();
        try {
            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRIES);
            response = retryTemplate.execute(ctx -> {
                try (PerformanceTimer timer = new PerformanceTimer("add role to user.")) {
                    HttpEntity<RoleRequest> entity = new HttpEntity<>(request);
                    ResponseEntity<IDaaSResponse> responseEntity = restTemplate.exchange(addRoleUri(email),
                            HttpMethod.PUT, entity, IDaaSResponse.class);
                    return responseEntity.getBody();
                } catch (Exception e) {
                    // TODO re-evaluate this part after network is setup
                    log.warn("Failed to execute api", e);
                    return null;
                }
            });
        } catch (Exception e) {
            log.warn("Failed to add role to user {}", email);
        }
        return response;
    }

    @Override
    public InvitationLinkResponse getUserInvitationLink(String email) {
        refreshToken();

        // Special case of authorization header format
        List<ClientHttpRequestInterceptor> interceptors = restTemplate.getInterceptors();
        ClientHttpRequestInterceptor previous = interceptors.stream()
                .filter(interceptor -> interceptor instanceof AuthorizationHeaderHttpRequestInterceptor).findAny()
                .get();
        interceptors.removeIf(i -> i instanceof AuthorizationHeaderHttpRequestInterceptor);

        InvitationLinkResponse response = null;
        try {
            String userInviteToken = "bearer:" + tokenInUse;
            ClientHttpRequestInterceptor inviteAuth = new AuthorizationHeaderHttpRequestInterceptor(userInviteToken);
            interceptors.add(inviteAuth);
            restTemplate.setInterceptors(interceptors);

            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRIES);
            response = retryTemplate.execute(ctx -> {
                try (PerformanceTimer timer = new PerformanceTimer("get user invitation link")) {
                    URI invitationLinkURI = createInvitationLink(email, DCP_PRODUCT);
                    ResponseEntity<InvitationLinkResponse> responseEntity = restTemplate.exchange(invitationLinkURI,
                            HttpMethod.GET, null, InvitationLinkResponse.class);
                    return responseEntity.getBody();
                } catch (Exception e) {
                    log.warn("Failed to execute invitation link api", e);
                    return null;
                }
            });

        } catch (Exception e) {
            log.warn("Failed to get invitation link", e);
        } finally {
            interceptors.removeIf(i -> i instanceof AuthorizationHeaderHttpRequestInterceptor);
            interceptors.add(previous);
            restTemplate.setInterceptors(interceptors);
        }
        return response;
    }

    @Cacheable(cacheNames = CacheName.Constants.IDaaSSubscriberDetailsCacheName, //
            key ="T(java.lang.String).format(\"%s|idaas-subscriberDetails\", #subscriberNumber)", //
            unless="#result == null")
    @Override
    public SubscriberDetails getSubscriberDetails(@NotNull String subscriberNumber) {
        refreshToken();
        SubscriberDetails subscriberDetails = null;
        try {
            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRIES);
            subscriberDetails = retryTemplate.execute(ctx -> {
                try (PerformanceTimer timer = new PerformanceTimer("Get subscriber_details from IDaaS.")) {
                    URI subscriberDetailsV1URI = createSubscriberDetailsV1Link(subscriberNumber);
                    ResponseEntity<SuperSubscriberDetails> responseEntity = restTemplate
                            .getForEntity(subscriberDetailsV1URI, SuperSubscriberDetails.class);
                    return (responseEntity.getBody() != null) ? responseEntity.getBody().getSubscriberDetails() : null;
                }
            });
        } catch (HttpClientErrorException hcee) {
            String msg = String.format("HttpClientErrorException while trying to get subscriber_details "
                    + "from IDaaS. Error Code %d\nMsg %s", hcee.getRawStatusCode(), hcee.getStatusText());
            log.error(msg, hcee);
        } catch (Exception e) {
            String msg = "Exception while trying to get subscription_details from IDaaS.";
            log.error(msg, e);
        }
        return subscriberDetails;
    }

    /**
     * Check if this subscriber_number is in IDaaS and returns subscriber_details
     * when requested.
     *
     * @param vboRequest
     * @return
     */
    @Override
    public boolean doesSubscriberNumberExist(VboRequest vboRequest) {
        String subscriptionNumber = vboRequest.getSubscriber().getSubscriberNumber();
        if (!StringUtils.isEmpty(subscriptionNumber)) {
            SubscriberDetails subscriberDetails = _self.getSubscriberDetails(subscriptionNumber);
            return null != subscriberDetails;
        } else {
            return false; // no subscriber number in the VBO request.
        }
    }

    private boolean hasAccessToApp(IDaaSUser user) {
        return user.getApplications().contains(DCP_PRODUCT) || user.getRoles().contains(DCP_ROLE);
    }

    private URI authenticateUri() {
        return URI.create(apiUrl + "/user/v2/authenticate");
    }

    private URI userUri(String email) {
        return URI.create(apiUrl + "/user/" + email);
    }

    private URI createUserUri() {
        return URI.create(apiUrl + "/user");
    }

    private URI addProductUri(String email) {
        return URI.create(apiUrl + String.format("/user/%s/product", email));
    }

    private URI addRoleUri(String email) {
        return URI.create(apiUrl + String.format("/user/%s/role", email));
    }

    private URI createInvitationLink(String email, String source) {
        return URI.create(apiUrl + "/user/" + email + "/" + source.replaceAll(" ", "%20") + "/invitation");
    }

    private URI createSubscriberDetailsV1Link(String subscriberNumber) {
        return URI.create(apiUrl + String.format("/subscriber/v1?subscriber_number=%s", subscriberNumber));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SuperSubscriberDetails {

        @JsonProperty("subscriber_details")
        private SubscriberDetails subscriberDetails;

        public SubscriberDetails getSubscriberDetails() {
            return subscriberDetails;
        }

        public void setSubscriberDetails(SubscriberDetails subscriberDetails) {
            this.subscriberDetails = subscriberDetails;
        }
    }
}
