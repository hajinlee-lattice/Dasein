package com.latticeengines.security.controller;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.exception.*;
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

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.idaas.IDaaSUser;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserFilter;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.exposed.util.SecurityUtils;

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
    public ResponseDocument<RegistrationResult> register(@RequestBody UserRegistration userReg, @RequestHeader(value = Constants.SET_TEMP_PASS, required = false) Boolean setTempPass,
                                                         HttpServletRequest request, HttpServletResponse httpResponse) {
        ResponseDocument<RegistrationResult> response = new ResponseDocument<>();
        response.setSuccess(false);

        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        UserRegistrationWithTenant uRegTenant = new UserRegistrationWithTenant();
        userReg.toLowerCase();
        uRegTenant.setUserRegistration(userReg);
        uRegTenant.setTenant(tenant.getId());
        User user = userReg.getUser();

        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        String loginUsername = loginUser.getUsername();
        AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
        AccessLevel targetLevel = AccessLevel.EXTERNAL_USER;
        if (userReg.getUser().getAccessLevel() != null) {
            targetLevel = AccessLevel.valueOf(userReg.getUser().getAccessLevel());
        }
        if (Boolean.TRUE.equals(userReg.isUseIDaaS())) {
            if ((AccessLevel.SUPER_ADMIN.equals(targetLevel) || AccessLevel.INTERNAL_ADMIN.equals(targetLevel))
                    && !EmailUtils.isInternalUser(userReg.getUser().getEmail())) {
                LOGGER.info("target level is {} while email is external, change it to {}", targetLevel,
                        AccessLevel.EXTERNAL_ADMIN);
                targetLevel = AccessLevel.EXTERNAL_ADMIN;
            }
            userReg.getUser().setAccessLevel(targetLevel.name());
        }
        if (!userService.isSuperior(loginLevel, targetLevel)) {
            LOGGER.warn(
                    String.format("User %s at level %s attempts to create a user at level %s, which is not allowed.",
                            loginUsername, loginLevel, targetLevel));
            httpResponse.setStatus(403);
            response.setErrors(Collections.singletonList("Cannot create a user with higher access level."));
            return response;
        }

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
        if (!batonService.hasProduct(CustomerSpace.parse(tenant.getId()), LatticeProduct.DCP)) {
            if (targetLevel.equals(AccessLevel.EXTERNAL_ADMIN) || targetLevel.equals(AccessLevel.EXTERNAL_USER)) {
                emailService.sendNewUserEmail(user, tempPass, apiPublicUrl,
                        !tenantService.getTenantEmailFlag(tenant.getId()));
                tenantService.updateTenantEmailFlag(tenant.getId(), true);
            } else {
                emailService.sendNewUserEmail(user, tempPass, apiPublicUrl, false);
            }
        }
        else {
            IDaaSUser idaasUser = userService.createIDaaSUser(user, tenant.getSubscriberNumber());
            if (idaasUser == null) {
                LOGGER.error(String.format("Failed to create IDaaS user for %s at level %s.",
                        loginUsername, loginLevel));
                String title = "Failed to create IDaaS User.";
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
                throw new LedpException(LedpCode.LEDP_18001, new String[]{username});
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
    public SimpleBooleanResponse update(@PathVariable String username, @RequestBody UserUpdateData data,
                                        HttpServletRequest request, HttpServletResponse response) {
        username = userService.getURLSafeUsername(username).toLowerCase();
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String tenantId = tenant.getId();
        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        User user = userService.findByUsername(username);
        boolean newUser = !userService.inTenant(tenantId, username);
        // update access level
        if (data.getAccessLevel() != null && !data.getAccessLevel().equals("")) {
            // using access level if it is provided
            String loginUsername = loginUser.getUsername();
            AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
            AccessLevel targetLevel = AccessLevel.valueOf(data.getAccessLevel());
            if (!userService.isSuperior(loginLevel, targetLevel)) {
                response.setStatus(403);
                return SimpleBooleanResponse.failedResponse(
                        Collections.singletonList("Cannot update to a level higher than that of the login user."));
            }
            userService.assignAccessLevel(targetLevel, tenantId, username, loginUsername, data.getExpirationDate(),
                    false, !newUser, data.getUserTeams());
            LOGGER.info(String.format("%s assigned %s access level to %s in tenant %s", loginUsername,
                    targetLevel.name(), username, tenantId));
            if (newUser && user != null && !batonService.hasProduct(CustomerSpace.parse(tenant.getId()), LatticeProduct.DCP)) {
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
            return SimpleBooleanResponse
                    .failedResponse(Collections.singletonList("Cannot update users in another tenant."));
        }
        if (newUser && batonService.hasProduct(CustomerSpace.parse(tenant.getId()), LatticeProduct.DCP)) {
            IDaaSUser idaasUser = userService.createIDaaSUser(user, tenant.getSubscriberNumber());
            if (idaasUser == null) {
                LOGGER.error(String.format("Failed to create IDaaS user for %s at level %s in tenant %s",
                        loginUser.getUsername(), loginUser.getAccessLevel(), tenantId));
                String title = "Failed to create IDaaS User.";
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
        }
        return SimpleBooleanResponse.successResponse();
    }

    @DeleteMapping("/{username:.+}")
    @ResponseBody
    @ApiOperation(value = "Delete a user. The user must be in the tenant")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public SimpleBooleanResponse deleteUser(@PathVariable String username, HttpServletRequest request,
                                            HttpServletResponse response) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String tenantId = tenant.getId();
        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        String safeUsername = userService.getURLSafeUsername(username).toLowerCase();
        if (userService.inTenant(tenantId, safeUsername)) {
            String loginUsername = loginUser.getUsername();
            AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
            AccessLevel targetLevel = userService.getAccessLevel(tenantId, safeUsername);
            if (!userService.isSuperior(loginLevel, targetLevel)) {
                response.setStatus(403);
                return SimpleBooleanResponse.failedResponse(Collections.singletonList(String
                        .format("Could not delete a %s user using a %s user.", targetLevel.name(), loginLevel.name())));
            }
            userService.deleteUser(tenantId, safeUsername);
            LOGGER.info(String.format("%s deleted %s from tenant %s", loginUsername, safeUsername, tenantId));
            return SimpleBooleanResponse.successResponse();
        } else {
            return SimpleBooleanResponse.failedResponse(
                    Collections.singletonList("Could not delete a user that is not in the current tenant"));
        }
    }

    private void checkUser(User user) {
        if (user == null) {
            throw new LedpException(LedpCode.LEDP_18221);
        }
    }
}
