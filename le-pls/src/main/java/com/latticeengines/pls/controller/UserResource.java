package com.latticeengines.pls.controller;

import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.ResponseDocument;
import com.latticeengines.domain.exposed.pls.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.exception.LoginException;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.EmailService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.UserFilter;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "user", description = "REST resource for user management")
@RestController
@RequestMapping("/users")
public class UserResource {

    private static final Log LOGGER = LogFactory.getLog(UserResource.class);

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private SessionService sessionService;

    @Autowired
    private UserService userService;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private EmailService emailService;

    @Value("${pls.api.hostport}")
    private String apiHostPort;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all users that have at least one access right to the current tenant")
    @PreAuthorize("hasRole('View_PLS_Users')")
    public ResponseDocument<List<User>> getAll(HttpServletRequest request) {
        ResponseDocument<List<User>> response = new ResponseDocument<>();

        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);

        UserFilter filter;
        AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
        if (loginLevel.equals(AccessLevel.EXTERNAL_USER) || loginLevel.equals(AccessLevel.EXTERNAL_ADMIN)) {
            filter = new UserFilter() {
                @Override
                public boolean visible(User user) {
                    if (StringUtils.isEmpty(user.getAccessLevel())) return false;
                    AccessLevel level = AccessLevel.valueOf(user.getAccessLevel());
                    return level.equals(AccessLevel.EXTERNAL_USER) || level.equals(AccessLevel.EXTERNAL_ADMIN);
                }
            };
        } else {
            filter = UserFilter.TRIVIAL_FILTER;
        }
        List<User> users = userService.getUsers(tenant.getId(), filter);

        response.setSuccess(true);
        response.setResult(users);
        return response;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register or validate a new user in the current tenant")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public ResponseDocument<RegistrationResult> register(@RequestBody UserRegistration userReg,
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
        String loginUsername = loginUser.getUsername();
        AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
        AccessLevel targetLevel = AccessLevel.EXTERNAL_USER;
        if (userReg.getUser().getAccessLevel() != null) {
            targetLevel = AccessLevel.valueOf(userReg.getUser().getAccessLevel());
        }
        if (!userService.isSuperior(loginLevel, targetLevel)) {
            httpResponse.setStatus(403);
            response.setErrors(Collections.singletonList("Cannot create a user with higher access level."));
            return response;
        }

        RegistrationResult result = userService.registerUserToTenant(uRegTenant);
        response.setResult(result);
        if (!result.isValid()) return response;

        LOGGER.info(String.format("%s registered %s as a new user in tenant %s",
                loginUsername, user.getUsername(), tenant.getId()));

        String tempPass = result.getPassword();
        if (targetLevel.equals(AccessLevel.EXTERNAL_ADMIN) || targetLevel.equals(AccessLevel.EXTERNAL_USER)) {
            emailService.sendPLSNewExternalUserEmail(user, tempPass, apiHostPort);
        } else {
            emailService.sendPLSNewInternalUserEmail(tenant, user, tempPass, apiHostPort);
        }

        response.setSuccess(true);
        return response;
    }

    @RequestMapping(value = "/{username:.+}/creds", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update password of user")
    public SimpleBooleanResponse updateCredentials(@PathVariable String username, @RequestBody UserUpdateData data,
                                                   HttpServletRequest request) {
        username = userService.getURLSafeUsername(username).toLowerCase();
        try {
            User user = SecurityUtils.getUserFromRequest(request, sessionService, userService);
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

    @RequestMapping(value = "/creds", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update password of user")
    public SimpleBooleanResponse updateCredentials(@RequestBody UserUpdateData data, HttpServletRequest request) {
        User user = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        if (userService.updateCredentials(user, data)) {
            return SimpleBooleanResponse.getSuccessResponse();
        } else {
            return SimpleBooleanResponse.getFailResponse(Collections.singletonList("Could not change password."));
        }
    }

    @RequestMapping(value = "/{username:.+}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update users")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public SimpleBooleanResponse update(@PathVariable String username,
                                        @RequestBody UserUpdateData data,
                                        HttpServletRequest request,
                                        HttpServletResponse response) {
        username = userService.getURLSafeUsername(username).toLowerCase();
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String tenantId = tenant.getId();

        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        String loginUsername = loginUser.getUsername();
        AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());

        // update access level
        if (data.getAccessLevel() != null && !data.getAccessLevel().equals("")) {
            // using access level if it is provided
            AccessLevel targetLevel = AccessLevel.valueOf(data.getAccessLevel());
            if (!userService.isSuperior(loginLevel, targetLevel)) {
                response.setStatus(403);
                return SimpleBooleanResponse.getFailResponse(
                        Collections.singletonList("Cannot update to a level higher than that of the login user.")
                );
            }

            boolean newUser = !userService.inTenant(tenantId, username);
            userService.assignAccessLevel(targetLevel, tenantId, username);
            LOGGER.info(String.format("%s assigned %s access level to %s in tenant %s",
                    loginUsername, targetLevel.name(), username, tenantId));
            User user = userService.findByUsername(username);
            if (newUser && user != null) {
                if (targetLevel.equals(AccessLevel.EXTERNAL_ADMIN) ||
                        targetLevel.equals(AccessLevel.EXTERNAL_USER)) {
                    emailService.sendPLSExistingExternalUserEmail(tenant, user, apiHostPort);
                } else {
                    emailService.sendPLSExistingInternalUserEmail(tenant, user, apiHostPort);
                }
            }
        }

        // update other information
        if (!userService.inTenant(tenantId, username)) {
            return SimpleBooleanResponse.getFailResponse(
                    Collections.singletonList("Cannot update users in another tenant.")
            );
        }

        return SimpleBooleanResponse.getSuccessResponse();
    }

    @RequestMapping(value = "/{username:.+}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a user. The user must be in the tenant")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public SimpleBooleanResponse deleteUser(@PathVariable String username, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String tenantId = tenant.getId();

        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        String loginUsername = loginUser.getUsername();
        AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());

        username = userService.getURLSafeUsername(username).toLowerCase();

        if (userService.inTenant(tenantId, username)) {
            AccessLevel targetLevel = userService.getAccessLevel(tenantId, username);
            if (!userService.isSuperior(loginLevel,  targetLevel)) {
                return SimpleBooleanResponse.getFailResponse(
                        Collections.singletonList(
                                String.format("Could not delete a %s user using a %s user.",
                                        targetLevel.name(), loginLevel.name())));
            }
            userService.deleteUser(tenantId, username);
            LOGGER.info(String.format("%s deleted %s from tenant %s", loginUsername, username, tenantId));
            return SimpleBooleanResponse.getSuccessResponse();
        } else {
            return SimpleBooleanResponse.getFailResponse(
                    Collections.singletonList("Could not delete a user that is not in the current tenant"));
        }
    }
}
