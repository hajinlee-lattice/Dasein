package com.latticeengines.pls.controller;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.DeleteUsersResult;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.ResponseDocument;
import com.latticeengines.domain.exposed.pls.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.exception.LoginException;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalSessionManagementService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.AccessLevel;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.security.RestGlobalAuthenticationFilter;
import com.latticeengines.pls.security.RightsUtilities;
import com.latticeengines.pls.service.UserService;
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
    private GlobalSessionManagementService globalSessionManagementService;

    @Autowired
    private UserService userService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all users that have at least one access right to the current tenant")
    @PreAuthorize("hasRole('View_PLS_Users')")
    public ResponseDocument<List<User>> getAll(HttpServletRequest request) {
        ResponseDocument<List<User>> response = new ResponseDocument<>();
        try {
            Ticket ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            String tenantId = globalSessionManagementService.retrieve(ticket).getTenant().getId();
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList
                = globalUserManagementService.getAllUsersOfTenant(tenantId);
            List<User> users = new ArrayList<>();
            for (Map.Entry<User, List<String>> userRights : userRightsList) {
                User user = userRights.getKey();
                AccessLevel accessLevel = userService.getAccessLevel(tenantId, user.getUsername());
                if (accessLevel == null) {
                    if (!RightsUtilities.isAdmin(RightsUtilities.translateRights(userRights.getValue()))) {
                        users.add(user);
                    }
                } else if (!accessLevel.equals(AccessLevel.SUPER_ADMIN)) {
                    user.setAccessLevel(accessLevel.name());
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
                                                         HttpServletRequest request) {
        ResponseDocument<RegistrationResult> response = new ResponseDocument<>();
        RegistrationResult result = new RegistrationResult();
        User user = userReg.getUser();

        String tenantId;
        try {
            Ticket ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            tenantId = globalSessionManagementService.retrieve(ticket).getTenant().getId();
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
            if (!inTenant(tenantId, oldUser.getUsername())) {
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

            if (!globalUserManagementService.registerUser(user, creds)) {
                globalUserManagementService.deleteUser(user.getUsername());
                throw new LedpException(LedpCode.LEDP_18004, new String[]{creds.getUsername()});
            }
            if (userReg.getUser().getAccessLevel() != null) {
                userService.assignAccessLevel(AccessLevel.valueOf(userReg.getUser().getAccessLevel()),
                        tenantId, user.getUsername());
            } else {
                userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, tenantId, user.getUsername());
            }
            response.setSuccess(true);
            String tempPass = globalUserManagementService.resetLatticeCredentials(user.getUsername());
            result.setPassword(tempPass);
            response.setResult(result);
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
        try {
            ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            user = globalUserManagementService.getUserByEmail(
                globalSessionManagementService.retrieve(ticket).getEmailAddress()
            );
            if (!user.getUsername().equals(username)) {
                throw new LedpException(LedpCode.LEDP_18001, new String[] { username });
            }
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }

        // change password
        String oldPassword  = data.getOldPassword();
        String newPassword  = data.getNewPassword();
        if (oldPassword != null && newPassword != null) {
            Credentials oldCreds = new Credentials();
            oldCreds.setUsername(user.getUsername());
            oldCreds.setPassword(oldPassword);

            Credentials newCreds = new Credentials();
            newCreds.setUsername(user.getUsername());
            newCreds.setPassword(newPassword);

            try {
                if (globalUserManagementService.modifyLatticeCredentials(ticket, oldCreds, newCreds)) {
                    return SimpleBooleanResponse.getSuccessResponse();
                }
            } catch (LedpException e) {
                if (e.getCode() == LedpCode.LEDP_18001) {
                    throw new LoginException(e);
                }
            }
        }
        return SimpleBooleanResponse.getFailResponse(Collections.singletonList("Could not change password."));
    }

    @RequestMapping(value = "/{username:.+}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update users")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public SimpleBooleanResponse update(@PathVariable String username, @RequestBody UserUpdateData data,
                                        HttpServletRequest request) {
        String tenantId;
        try {
            Ticket ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            tenantId = globalSessionManagementService.retrieve(ticket).getTenant().getId();
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }

        // update rights
        if (data.getAccessLevel() != null && !data.getAccessLevel().equals("")) {
            // using access level if it is provided
            userService.assignAccessLevel(AccessLevel.valueOf(data.getAccessLevel()), tenantId, username);
        } else {
            softDelete(tenantId, username);
            for (String right : RightsUtilities.translateRights(data.getRights())) {
                globalUserManagementService.grantRight(right, tenantId, username);
            }
        }



        if (!inTenant(tenantId, username)) {
            return SimpleBooleanResponse.getFailResponse(
                Collections.singletonList("Cannot update users in another tenant.")
            );
        }

        return SimpleBooleanResponse.getSuccessResponse();
    }

    @RequestMapping(value = "", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete users (?usernames=) from current tenant. "
        + "Request parameter should be a stringified JSON array.")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public ResponseDocument<DeleteUsersResult> delete(@RequestParam(value = "usernames") String usernames,
                                                      HttpServletRequest request)
    {
        String tenantId;
        try {
            Ticket ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            Tenant tenant = globalSessionManagementService.retrieve(ticket).getTenant();
            tenantId = tenant.getId();
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }

        ResponseDocument<DeleteUsersResult> response = new ResponseDocument<>();
        JsonNode userNodes;
        ObjectMapper mapper = new ObjectMapper();
        try {
            userNodes = mapper.readTree(usernames);
        } catch (IOException e) {
            response.setErrors(Collections.singletonList("Could not parse the input name array."));
            return response;
        }

        List<String> successUsers = new ArrayList<>();
        List<String> failUsers = new ArrayList<>();

        for (JsonNode node: userNodes) {
            String username = node.asText();
            if (!isAdmin(tenantId, username)) {
                try {
                    if (softDelete(tenantId, username)) {
                        if (globalUserManagementService.isRedundant(username)) {
                            globalUserManagementService.deleteUser(username);
                        }
                        successUsers.add(username);
                        continue;
                    } else {
                        LOGGER.warn(String.format("Failed to delete the user %s in the tenant %s", username, tenantId));
                    }
                } catch (LedpException e) {
                    // ignore
                    LOGGER.warn(String.format(
                        "Exception encountered when deleting the user %s in the tenant %s",
                        username, tenantId
                    ));
                }
            } else {
                LOGGER.warn(String.format("Trying to delete the admin user %s in the tenant %s", username, tenantId));
            }
            failUsers.add(username);
        }

        DeleteUsersResult result = new DeleteUsersResult();
        result.setSuccessUsers(successUsers);
        result.setFailUsers(failUsers);
        response.setResult(result);
        response.setSuccess(true);
        return response;
    }

    @RequestMapping(value = "/logout", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Logout the user")
    public UserDocument logout(HttpServletRequest request) {
        UserDocument doc = new UserDocument();

        Ticket ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
        globalAuthenticationService.discard(ticket);
        doc.setSuccess(true);

        return doc;
    }

    private boolean softDelete(String tenantId, String username) {
        try {
            return userService.softDelete(tenantId, username);
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
    }

    private boolean isAdmin(String tenantId, String username) {
        return RightsUtilities.isAdmin(
            RightsUtilities.translateRights(
                globalUserManagementService.getRights(username, tenantId)
            )
        );
    }

    private boolean inTenant(String tenantId, String username) {
        return !globalUserManagementService.getRights(username, tenantId).isEmpty();
    }
}
