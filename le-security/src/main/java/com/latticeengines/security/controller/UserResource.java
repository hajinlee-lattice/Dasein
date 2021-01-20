package com.latticeengines.security.controller;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.idaas.IDaaSUser;
import com.latticeengines.domain.exposed.dcp.idaas.SubscriberDetails;
import com.latticeengines.domain.exposed.dcp.vbo.VboUserSeatUsageEvent;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.LoginException;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.domain.exposed.exception.UIActionCode;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.exception.UIActionUtils;
import com.latticeengines.domain.exposed.exception.View;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.pls.UserUpdateResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.monitor.tracing.TracingTags;
import com.latticeengines.monitor.util.TracingUtils;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserFilter;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.latticeengines.security.service.IDaaSService;
import com.latticeengines.security.service.VboService;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "user", description = "REST resource for user management")
@RestController
@RequestMapping("/users")
public class UserResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserResource.class);

    @Inject
    private SessionService sessionService;

    @Inject
    private UserService userService;

    @Inject
    private EmailService emailService;

    @Value("${security.app.public.url:http://localhost:8081}")
    private String apiPublicUrl;

    @Value("${common.dcp.public.url}")
    private String dcpPublicUrl;

    @Inject
    private TenantService tenantService;

    @Inject
    private BatonService batonService;

    @Inject
    private IDaaSService iDaaSService;

    @Inject
    private VboService vboService;

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get all users that have at least one access right to the current tenant")
    @PreAuthorize("hasRole('View_PLS_Users')")
    public ResponseDocument<List<User>> getAll(HttpServletRequest request) {
        ResponseDocument<List<User>> response = new ResponseDocument<>();

        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        UserFilter filter;
        AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
        if (loginLevel.equals(AccessLevel.EXTERNAL_USER) || loginLevel.equals(AccessLevel.EXTERNAL_ADMIN)) {
            filter = UserFilter.EXTERNAL_FILTER;
        } else {
            filter = UserFilter.TRIVIAL_FILTER;
        }
        List<User> users = userService.getUsers(tenant.getId(), filter, true);

        response.setSuccess(true);
        response.setResult(users);
        return response;
    }

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Register or validate a new user in the current tenant")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public ResponseDocument<RegistrationResult> register(@RequestBody UserRegistration userReg,
            @RequestHeader(value = Constants.SET_TEMP_PASS, required = false) Boolean setTempPass,
            HttpServletRequest request, HttpServletResponse httpResponse) {
        ResponseDocument<RegistrationResult> response = new ResponseDocument<>();
        response.setSuccess(false);

        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        UserRegistrationWithTenant uRegTenant = new UserRegistrationWithTenant();
        userReg.toLowerCase();
        uRegTenant.setUserRegistration(userReg);
        uRegTenant.setTenant(tenant.getId());
        User user = userReg.getUser();
        boolean isDCPTenant = batonService.hasProduct(CustomerSpace.parse(tenant.getId()), LatticeProduct.DCP);

        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        String loginUsername = loginUser.getUsername();
        AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
        AccessLevel targetLevel = AccessLevel.EXTERNAL_USER;
        if (userReg.getUser().getAccessLevel() != null) {
            targetLevel = AccessLevel.valueOf(userReg.getUser().getAccessLevel());
        }
        if ((AccessLevel.SUPER_ADMIN.equals(targetLevel) || AccessLevel.INTERNAL_ADMIN.equals(targetLevel))
                && !EmailUtils.isInternalUser(user.getEmail())) {
            httpResponse.setStatus(500);
            response.setErrors(Collections
                    .singletonList("Cannot create users with internal admin access and an external email address."));
            return response;
        }
        if (!userService.isSuperior(loginLevel, targetLevel)) {
            LOGGER.warn(
                    String.format("User %s at level %s attempts to create a user at level %s, which is not allowed.",
                            loginUsername, loginLevel, targetLevel));
            httpResponse.setStatus(403);
            response.setErrors(Collections.singletonList("Cannot create a user with higher access level."));
            return response;
        }
        if (isDCPTenant && !EmailUtils.isInternalUser(user.getEmail())
                && !hasAvailableSeats(tenant.getSubscriberNumber())) {
            httpResponse.setStatus(403);
            response.setErrors(Collections
                    .singletonList(String.format("User seat limit for tenant %s has been reached.", tenant.getId())));
            return response;
        }

        Tracer tracer = GlobalTracer.get();
        Span userSpan = null;
        try (Scope scope = startUserSpan(loginUsername, tenant.getId(), "Register User")) {
            userSpan = tracer.activeSpan();
            String traceId = userSpan.context().toTraceId();
            userSpan.log("Start - Register User");

            RegistrationResult result = userService.registerUserToTenant(loginUsername, uRegTenant);
            String tempPass = result.getPassword();
            if (!Boolean.TRUE.equals(setTempPass)) {
                result.setPassword(null);
            }
            response.setResult(result);
            if (!result.isValid()) {
                if (!result.isValidEmail()) {
                    httpResponse.setStatus(400);
                    response.setErrors(Collections.singletonList(result.getErrMsg()));
                }
                return response;
            }
            LOGGER.info(String.format("%s registered %s as a new user in tenant %s", loginUsername, user.getUsername(),
                    tenant.getId()));
            if (!isDCPTenant) {
                if (targetLevel.equals(AccessLevel.EXTERNAL_ADMIN) || targetLevel.equals(AccessLevel.EXTERNAL_USER)) {
                    emailService.sendNewUserEmail(user, tempPass, apiPublicUrl,
                            !tenantService.getTenantEmailFlag(tenant.getId()));
                    tenantService.updateTenantEmailFlag(tenant.getId(), true);
                } else {
                    emailService.sendNewUserEmail(user, tempPass, apiPublicUrl, false);
                }
            } else {
                IDaaSUser idaasUser = userService.createIDaaSUser(user, tenant.getSubscriberNumber());
                if (idaasUser == null) {
                    LOGGER.error(
                            String.format("Failed to create IDaaS user for %s at level %s.", loginUsername, loginLevel));
                    String title = "Failed to create IDaaS User. Trace ID: " + traceId;
                    userSpan.log(title);
                    UIActionCode uiActionCode = UIActionCode.fromLedpCode(LedpCode.LEDP_18004);
                    UIAction action = UIActionUtils.generateUIError(title, View.Banner, uiActionCode);
                    throw UIActionException.fromAction(action);
                } else {
                    String welcomeUrl = dcpPublicUrl;
                    if (idaasUser.getInvitationLink() != null) {
                        welcomeUrl = idaasUser.getInvitationLink();
                    }
                    emailService.sendDCPWelcomeEmail(user, tenant.getName(), welcomeUrl);
                }
                if (!EmailUtils.isInternalUser(user.getEmail())) {
                    VboUserSeatUsageEvent usageEvent = new VboUserSeatUsageEvent();
                    usageEvent.setTimeStamp(ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
                    usageEvent.setEmailAddress(loginUser.getEmail());
                    usageEvent.setSubscriberID(tenant.getSubscriberNumber());
                    usageEvent.setPOAEID(traceId);
                    usageEvent.setFeatureURI(VboUserSeatUsageEvent.FeatureURI.STCT);
                    usageEvent.setLUID(loginUser.getPid());
                    populateWithSubscriberDetails(usageEvent);
                    try {
                        vboService.sendUserUsageEvent(usageEvent);
                    } catch (Exception e) {
                        LOGGER.error("Exception in usage event: " + e.toString());
                        LOGGER.error("Exception in usage event: " + ExceptionUtils.getStackTrace(e));
                    }
                }
            }
        } finally {
            TracingUtils.finish(userSpan);
        }
        response.setSuccess(true);
        return response;
    }

    @PutMapping("/{username:.+}/creds")
    @ResponseBody
    @ApiOperation(value = "Update password of user")
    public SimpleBooleanResponse updateCredentials(@PathVariable String username, @RequestBody UserUpdateData data,
            HttpServletRequest request) {
        username = userService.getURLSafeUsername(username).toLowerCase();
        try {
            User user = SecurityUtils.getUserFromRequest(request, sessionService, userService);
            checkUser(user);
            if (!user.getUsername().equals(username)) {
                throw new LedpException(LedpCode.LEDP_18001, new String[] { username });
            }
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
        return updateCredentials(data, request);
    }

    @PutMapping("/creds")
    @ResponseBody
    @ApiOperation(value = "Update password of user")
    public SimpleBooleanResponse updateCredentials(@RequestBody UserUpdateData data, HttpServletRequest request) {
        User user = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(user);
        if (userService.updateCredentials(user, data)) {
            return SimpleBooleanResponse.successResponse();
        } else {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList("Could not change password."));
        }
    }

    @PutMapping("/{username:.+}")
    @ResponseBody
    @ApiOperation(value = "Update users")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public ResponseDocument<UserUpdateResponse> update(@PathVariable String username, @RequestBody UserUpdateData data,
                                                       HttpServletRequest request, HttpServletResponse response) {
        ResponseDocument<UserUpdateResponse> document = new ResponseDocument();
        username = userService.getURLSafeUsername(username).toLowerCase();
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String tenantId = tenant.getId();
        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        // Assumes the UI first calls `register`. If the user exists, it calls `update`. User shouldn't be null.
        User user = userService.findByUsername(username);
        if (user == null) {
            response.setStatus(403);
            document.setErrors(Collections.singletonList("Cannot update a non-existing user."));
            return document;
        }
        boolean newUser = !userService.inTenant(tenantId, username);
        boolean isDCPTenant = batonService.hasProduct(CustomerSpace.parse(tenantId), LatticeProduct.DCP);
        // update access level

        Tracer tracer = GlobalTracer.get();
        Span userSpan = null;
        try (Scope scope = startUserSpan(username, tenantId, "Update User")) {
            userSpan = tracer.activeSpan();
            String traceId = userSpan.context().toTraceId();

            UserUpdateResponse updateResponse = new UserUpdateResponse();
            updateResponse.setTraceId(traceId);
            document.setResult(updateResponse);

            if (data.getAccessLevel() != null && !data.getAccessLevel().equals("")) {
                // using access level if it is provided
                String loginUsername = loginUser.getUsername();
                AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
                AccessLevel targetLevel = AccessLevel.valueOf(data.getAccessLevel());
                if (!userService.isSuperior(loginLevel, targetLevel)) {
                    response.setStatus(403);
                    document.setErrors(Collections.singletonList("Cannot update to a level higher than that of the login user."));
                    return document;
                }

                if ((AccessLevel.SUPER_ADMIN.equals(targetLevel) || AccessLevel.INTERNAL_ADMIN.equals(targetLevel))
                        && !EmailUtils.isInternalUser(user.getEmail())) {
                    response.setStatus(500);
                    document.setErrors(Collections.singletonList(
                            "Cannot assign internal admin access level to users with external email addresses."));
                    return document;
                }

                if (newUser && isDCPTenant && !EmailUtils.isInternalUser(username)
                        && !hasAvailableSeats(tenant.getSubscriberNumber())) {
                    response.setStatus(403);
                    document.setErrors(Collections.
                            singletonList(String.format("User seat limit for tenant %s has been reached.", tenantId)));
                    return document;
                }

                boolean result = userService.assignAccessLevel(targetLevel, tenantId, username, loginUsername,
                        data.getExpirationDate(), false, !newUser, data.getUserTeams());
                if (!result) {
                    response.setStatus(500);
                    document.setErrors(Collections.singletonList("Failed to assign access level to user."));
                    return document;
                }
                LOGGER.info(String.format("%s assigned %s access level to %s in tenant %s", loginUsername,
                        targetLevel.name(), username, tenantId));
                if (newUser && user != null && !isDCPTenant) {
                    userSpan.log("Sending email");
                    if (targetLevel.equals(AccessLevel.EXTERNAL_ADMIN) || targetLevel.equals(AccessLevel.EXTERNAL_USER)) {
                        emailService.sendExistingUserEmail(tenant, user, apiPublicUrl,
                                !tenantService.getTenantEmailFlag(tenant.getId()));
                        tenantService.updateTenantEmailFlag(tenant.getId(), true);
                    } else {
                        emailService.sendExistingUserEmail(tenant, user, apiPublicUrl, false);
                    }
                }
            }
            // update other information
            if (!userService.inTenant(tenantId, username)) {
                document.setErrors(Collections.singletonList("Cannot update users in another tenant."));
                return document;
            }
            if (newUser && isDCPTenant) {
                IDaaSUser idaasUser = userService.createIDaaSUser(user, tenant.getSubscriberNumber());
                if (idaasUser == null) {
                    LOGGER.error(String.format("Failed to create IDaaS user for %s at level %s in tenant %s",
                            loginUser.getUsername(), loginUser.getAccessLevel(), tenantId));
                    String title = "Failed to create IDaaS User. Trace ID: " + traceId;
                    UIActionCode uiActionCode = UIActionCode.fromLedpCode(LedpCode.LEDP_18004);
                    UIAction action = UIActionUtils.generateUIError(title, View.Banner, uiActionCode);
                    throw UIActionException.fromAction(action);
                } else {
                    String welcomeUrl = dcpPublicUrl;
                    if (idaasUser.getInvitationLink() != null) {
                        welcomeUrl = idaasUser.getInvitationLink();
                    }
                    emailService.sendDCPWelcomeEmail(user, tenant.getName(), welcomeUrl);
                }
                if (!EmailUtils.isInternalUser(username)) {
                    VboUserSeatUsageEvent usageEvent = new VboUserSeatUsageEvent();
                    usageEvent.setEmailAddress(loginUser.getEmail());
                    usageEvent.setSubscriberID(tenant.getSubscriberNumber());
                    usageEvent.setPOAEID(traceId);
                    usageEvent.setFeatureURI(VboUserSeatUsageEvent.FeatureURI.STCT);
                    usageEvent.setLUID(loginUser.getPid());
                    usageEvent.setTimeStamp(ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
                    populateWithSubscriberDetails(usageEvent);
                    try {
                        vboService.sendUserUsageEvent(usageEvent);
                    } catch (Exception e) {
                        LOGGER.error("Exception in usage event: " + e.toString());
                        LOGGER.error("Exception in usage event: " + ExceptionUtils.getStackTrace(e));
                    }
                }
            }
        } finally {
            TracingUtils.finish(userSpan);
        }
        document.setSuccess(true);
        return document;
    }

    @DeleteMapping("/{username:.+}")
    @ResponseBody
    @ApiOperation(value = "Delete a user. The user must be in the tenant")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public ResponseDocument<UserUpdateResponse> deleteUser(@PathVariable String username, HttpServletRequest request,
            HttpServletResponse response) {
        ResponseDocument<UserUpdateResponse> document = new ResponseDocument();
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String tenantId = tenant.getId();
        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        String safeUsername = userService.getURLSafeUsername(username).toLowerCase();

        Tracer tracer = GlobalTracer.get();
        Span userSpan = null;
        try (Scope scope = startUserSpan(username, tenantId, "Delete User")) {
            userSpan = tracer.activeSpan();
            String traceId = userSpan.context().toTraceId();

            UserUpdateResponse updateResponse = new UserUpdateResponse();
            updateResponse.setTraceId(traceId);
            document.setResult(updateResponse);

            if (userService.inTenant(tenantId, safeUsername)) {
                String loginUsername = loginUser.getUsername();
                AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
                AccessLevel targetLevel = userService.getAccessLevel(tenantId, safeUsername);
                if (!userService.isSuperior(loginLevel, targetLevel)) {
                    response.setStatus(403);
                    document.setErrors(Collections.singletonList(String
                            .format("Could not delete a %s user using a %s user due to inferior access level.",
                                    targetLevel.name(), loginLevel.name())));
                    return document;
                }
                userService.deleteUser(tenantId, safeUsername, true);
                LOGGER.info(String.format("%s deleted %s from tenant %s", loginUsername, safeUsername, tenantId));
                if (!EmailUtils.isInternalUser(safeUsername)
                        && batonService.hasProduct(CustomerSpace.parse(tenant.getId()), LatticeProduct.DCP)) {
                    VboUserSeatUsageEvent usageEvent = new VboUserSeatUsageEvent();
                    usageEvent.setEmailAddress(loginUser.getEmail());
                    usageEvent.setSubscriberID(tenant.getSubscriberNumber());
                    usageEvent.setPOAEID(traceId);
                    usageEvent.setFeatureURI(VboUserSeatUsageEvent.FeatureURI.STDEC);
                    usageEvent.setLUID(loginUser.getPid());
                    usageEvent.setTimeStamp(ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
                    populateWithSubscriberDetails(usageEvent);
                    try {
                        vboService.sendUserUsageEvent(usageEvent);
                    } catch (Exception e) {
                        LOGGER.error("Exception in usage event: " + e.toString());
                        LOGGER.error("Exception in usage event: " + ExceptionUtils.getStackTrace(e));
                    }
                }
                document.setSuccess(true);
                return document;
            } else {
                document.setSuccess(false);
                document.setErrors(
                        Collections.singletonList("Could not delete a user that is not in the current tenant"));
                return document;
            }
        } finally {
            TracingUtils.finish(userSpan);
        }
    }

    @GetMapping("/newuser/levels")
    @ResponseBody
    @ApiOperation(value = "Get user levels that an admin can assign when creating a user")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public List<AccessLevel> getNewUserLevels() {
        return AccessLevel.getDnBConnectNewUserLevels();
    }

    private void checkUser(User user) {
        if (user == null) {
            throw new LedpException(LedpCode.LEDP_18221);
        }
    }

    private Scope startUserSpan(String username, String tenantId, String operation) {
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan(operation)
                .withTag(TracingTags.User.USERNAME, username)
                .withTag(TracingTags.TENANT_ID, tenantId)
                .withStartTimestamp(System.currentTimeMillis() * 1000)
                .start();
        return tracer.activateSpan(span);
    }

    private void populateWithSubscriberDetails(VboUserSeatUsageEvent usageEvent) {
        SubscriberDetails details = iDaaSService.getSubscriberDetails(usageEvent.getSubscriberID());
        if (details != null) {
            if (details.getAddress() != null) {
                usageEvent.setSubscriberCountry(details.getAddress().getCountryCode());
                usageEvent.setSubjectCountry(details.getAddress().getCountryCode());
            }
            usageEvent.setContractTermStartDate(details.getEffectiveDate());
            usageEvent.setContractTermEndDate(details.getExpirationDate());
        } else {
            LOGGER.info("Failed to retrieve subscriber details from IDaaS for sub id: " + usageEvent.getSubscriberID());
        }
    }

    private boolean hasAvailableSeats(String subscriberNumber) {
        if (subscriberNumber == null) {
            LOGGER.error("Unable to retrieve seat count meter: null subscriber number.");
            return true;
        }
        JsonNode meter = vboService.getSubscriberMeter(subscriberNumber);
        if (meter == null || !meter.has("limit") || !meter.has("current_usage")) {
            LOGGER.error("Unable to retrieve seat count meter for subscriber " +  subscriberNumber +
                    ": missing D&B Connect entitlement.");
            return true;
        }
        if (meter.get("current_usage") == null)
            LOGGER.info("Null current_usage in meter for subscriber: " + subscriberNumber);
        int current_usage = (meter.get("current_usage") == null) ? 0 : meter.get("current_usage").asInt();
        return current_usage < meter.get("limit").asInt();
    }
}
