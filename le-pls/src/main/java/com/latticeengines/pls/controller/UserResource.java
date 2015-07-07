package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.exception.LoginException;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.EmailService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.UserService;
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
        try {
            Ticket ticket = new Ticket(request.getHeader(Constants.AUTHORIZATION));
            Session session = sessionService.retrieve(ticket);
            String tenantId = session.getTenant().getId();
            AccessLevel currentLevel;
            try {
                currentLevel = AccessLevel.valueOf(session.getAccessLevel());
            } catch (NullPointerException|IllegalArgumentException e) {
                throw new LedpException(LedpCode.LEDP_18016, e, new String[] { tenantId });
            }

            List<User> users = new ArrayList<>();
            for (User user : userService.getUsers(tenantId)) {
                String targetLevelString = user.getAccessLevel();
                AccessLevel targetLevel = null;
                if (targetLevelString != null) {
                    targetLevel = AccessLevel.valueOf(targetLevelString);
                }
                if (userService.isVisible(currentLevel, targetLevel)) {
                    users.add(user);
                }
            }
            response.setSuccess(true);
            response.setResult(users);
            return response;
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register or validate a new user in the current tenant")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public ResponseDocument<RegistrationResult> register(@RequestBody UserRegistration userReg,
                                                         HttpServletRequest request, HttpServletResponse httpResponse) {
        ResponseDocument<RegistrationResult> response = new ResponseDocument<>();
        RegistrationResult result = new RegistrationResult();
        User user = userReg.getUser();

        user.setUsername(user.getUsername().toLowerCase());
        userReg.getCredentials().setUsername(user.getUsername().toLowerCase());

        Tenant tenant;
        String tenantId;
        String loginUsername;
        AccessLevel loginLevel;
        try {
            Ticket ticket = new Ticket(request.getHeader(Constants.AUTHORIZATION));
            Session session = sessionService.retrieve(ticket);
            tenant = session.getTenant();
            tenantId = tenant.getId();
            loginUsername = session.getEmailAddress();
            loginLevel = AccessLevel.valueOf(session.getAccessLevel());
        } catch (LedpException e) {
            response.setErrors(Collections.singletonList("Could not authenticate current user."));
            return response;
        }

        // validate new user
        User oldUser = globalUserManagementService.getUserByEmail(user.getEmail());
        if (oldUser != null) {
            result.setValid(false);
            response.setErrors(Collections.singletonList(
                    "The requested email conflicts with that of an existing user."
            ));
            if (!userService.inTenant(tenantId, oldUser.getUsername())) {
                result.setConflictingUser(oldUser);
            }
        } else {
            result.setValid(true);
        }

        if (!result.isValid()) {
            response.setResult(result);
            return response;
        }

        // register new user
        if (tenantId != null) {
            Credentials creds = userReg.getCredentials();
            AccessLevel targetLevel = AccessLevel.EXTERNAL_USER;
            if (userReg.getUser().getAccessLevel() != null) {
                targetLevel = AccessLevel.valueOf(userReg.getUser().getAccessLevel());
            }

            if (!userService.isSuperior(loginLevel, targetLevel)) {
                httpResponse.setStatus(403);
                response.setErrors(Collections.singletonList("Cannot create a user with higher access level."));
                return response;
            }

            if (!globalUserManagementService.registerUser(user, creds)) {
                globalUserManagementService.deleteUser(user.getUsername());
                throw new LedpException(LedpCode.LEDP_18004, new String[]{creds.getUsername()});
            }
            userService.assignAccessLevel(targetLevel, tenantId, user.getUsername());
            response.setSuccess(true);
            String tempPass = globalUserManagementService.resetLatticeCredentials(user.getUsername());
            result.setPassword(tempPass);
            response.setResult(result);
            LOGGER.info(String.format("%s registered %s as a new user in tenant %s",
                    loginUsername, user.getUsername(), tenantId));

            if (targetLevel.equals(AccessLevel.EXTERNAL_ADMIN) || targetLevel.equals(AccessLevel.EXTERNAL_USER)) {
                emailService.sendPLSNewExternalUserEmail(user, tempPass, apiHostPort);
            } else {
                emailService.sendPLSNewInternalUserEmail(tenant, user, tempPass, apiHostPort);
            }
        }
        return response;
    }

    @RequestMapping(value = "/{username:.+}/creds", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update password of user")
    public SimpleBooleanResponse updateCredentials(@PathVariable String username, @RequestBody UserUpdateData data,
                                                   HttpServletRequest request) {
        User user;
        Ticket ticket;
        username = userService.getURLSafeUsername(username).toLowerCase();
        try {
            ticket = new Ticket(request.getHeader(Constants.AUTHORIZATION));
            user = globalUserManagementService.getUserByEmail(
                    sessionService.retrieve(ticket).getEmailAddress()
            );
            if (!user.getUsername().equals(username)) {
                throw new LedpException(LedpCode.LEDP_18001, new String[]{username});
            }
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }

        // change password
        String oldPassword = data.getOldPassword();
        String newPassword = data.getNewPassword();
        if (oldPassword != null && newPassword != null) {
            Credentials oldCreds = new Credentials();
            oldCreds.setUsername(user.getUsername());
            oldCreds.setPassword(oldPassword);

            Credentials newCreds = new Credentials();
            newCreds.setUsername(user.getUsername());
            newCreds.setPassword(newPassword);

            try {
                globalAuthenticationService.authenticateUser(user.getUsername(), oldPassword);
            } catch (LedpException e) {
                if (e.getCode() == LedpCode.LEDP_18001) {
                    throw new LoginException(e);
                }
            }

            if (globalUserManagementService.modifyLatticeCredentials(ticket, oldCreds, newCreds)) {
                LOGGER.info(String.format("%s changed his/her password", user.getUsername()));
                return SimpleBooleanResponse.getSuccessResponse();
            }
        }
        return SimpleBooleanResponse.getFailResponse(Collections.singletonList("Could not change password."));
    }

    @RequestMapping(value = "/{username:.+}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update users")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public SimpleBooleanResponse update(@PathVariable String username,
                                        @RequestBody UserUpdateData data,
                                        HttpServletRequest request,
                                        HttpServletResponse response) {
        String tenantId;
        AccessLevel currentLevel;
        String loginUsername;
        username = userService.getURLSafeUsername(username).toLowerCase();
        try {
            Ticket ticket = new Ticket(request.getHeader(Constants.AUTHORIZATION));
            Session session = sessionService.retrieve(ticket);
            tenantId = session.getTenant().getId();
            currentLevel = AccessLevel.valueOf(session.getAccessLevel());
            loginUsername = session.getEmailAddress();
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }

        // update access level
        if (data.getAccessLevel() != null && !data.getAccessLevel().equals("")) {
            // using access level if it is provided
            AccessLevel targetLevel = AccessLevel.valueOf(data.getAccessLevel());
            if (userService.isSuperior(currentLevel, targetLevel)) {
                boolean newUser = !userService.inTenant(tenantId, username);

                userService.assignAccessLevel(targetLevel, tenantId, username);
                LOGGER.info(String.format("%s assigned %s access level to %s in tenant %s",
                        loginUsername, targetLevel.name(), username, tenantId));

                User user = userService.findByUsername(username);
                Tenant tenant = tenantService.findByTenantId(tenantId);

                if (newUser && tenant != null && user != null) {
                    if (targetLevel.equals(AccessLevel.EXTERNAL_ADMIN) ||
                            targetLevel.equals(AccessLevel.EXTERNAL_USER)) {
                        emailService.sendPLSExistingExternalUserEmail(tenant, user, apiHostPort);
                    } else {
                        emailService.sendPLSExistingInternalUserEmail(tenant, user, apiHostPort);
                    }
                }
            } else {
                response.setStatus(403);
                return SimpleBooleanResponse.getFailResponse(
                        Collections.singletonList("Cannot update to a level higher than that of the login user.")
                );
            }
        }

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
        String tenantId;
        AccessLevel loginLevel;
        String loginUsername;
        username = userService.getURLSafeUsername(username).toLowerCase();
        try {
            Ticket ticket = new Ticket(request.getHeader(Constants.AUTHORIZATION));
            Session session = sessionService.retrieve(ticket);
            Tenant tenant = session.getTenant();
            tenantId = tenant.getId();
            loginLevel = AccessLevel.valueOf(session.getAccessLevel());
            loginUsername = session.getEmailAddress();
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }

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

    @RequestMapping(value = "/logout", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Logout the user")
    public UserDocument logout(HttpServletRequest request) {
        UserDocument doc = new UserDocument();

        Ticket ticket = new Ticket(request.getHeader(Constants.AUTHORIZATION));
        globalAuthenticationService.discard(ticket);
        doc.setSuccess(true);

        return doc;
    }
}
