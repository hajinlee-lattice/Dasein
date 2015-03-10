package com.latticeengines.pls.controller;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.exception.LoginException;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalSessionManagementService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.security.RestGlobalAuthenticationFilter;
import com.latticeengines.pls.security.RightsUtilities;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "user", description = "REST resource for user management")
@RestController
@RequestMapping("/users")
public class UserResource {

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all users that have at least one access right to the current tenant")
    @PreAuthorize("hasRole('View_PLS_Users')")
    public ResponseDocument<List<User>> getAll(HttpServletRequest request) {
        ResponseDocument<List<User>> response = new ResponseDocument<>();
        try {
            Ticket ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            String tenantId = globalSessionManagementService.retrieve(ticket).getTenant().getId();
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = globalUserManagementService.getAllUsersOfTenant(tenantId);
            List<User> users = new ArrayList<>();
            for (AbstractMap.SimpleEntry<User, List<String>> userRights : userRightsList) {
                if (!RightsUtilities.isAdmin(RightsUtilities.translateRights(userRights.getValue()))) {
                    users.add(userRights.getKey());
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
    public ResponseDocument<RegistrationResult> register(@RequestBody UserRegistration userReg, HttpServletRequest request) throws  JsonProcessingException {
        ResponseDocument<RegistrationResult> response = new ResponseDocument<>();
        RegistrationResult result = new RegistrationResult();
        User user = userReg.getUser();

        String tenantId;
        try {
            Ticket ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            tenantId = globalSessionManagementService.retrieve(ticket).getTenant().getId();
        } catch (Exception e) {
            response.setErrors(Arrays.asList("Could not authenticate current user."));
            return response;
        }

        // validate new user
        User oldUser = globalUserManagementService.getUserByEmail(user.getEmail());
        if (oldUser != null) {
            result.setValid(false);
            response.setErrors(Arrays.asList("The requested email conflicts with that of an existing user."));
            if (!inTenant(tenantId, oldUser.getUsername()) || !isAdmin(tenantId, oldUser.getUsername())) {
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
            grantDefaultRights(tenantId, user.getUsername());
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
    public SimpleBooleanResponse updateCredentials(@PathVariable String username, @RequestBody UserUpdateData data, HttpServletRequest request) {
        User user;
        Ticket ticket;
        try {
            ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            user = globalUserManagementService.getUserByEmail(globalSessionManagementService.retrieve(ticket).getEmailAddress());
            if (!user.getUsername().equals(username)) {
                throw new LedpException(LedpCode.LEDP_18001);
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
        return SimpleBooleanResponse.getFailResponse(Arrays.asList("Could not change password."));
    }

    @RequestMapping(value = "/{username:.+}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update users")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public SimpleBooleanResponse update(@PathVariable String username, @RequestBody UserUpdateData data, HttpServletRequest request) {
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

        if (!inTenant(tenantId, username)) {
            return SimpleBooleanResponse.getFailResponse(Arrays.asList("Cannot update users in another tenant."));
        }

        // update rights
        List<String> rights = RightsUtilities.translateRights(data.getRights());
        if (!rights.isEmpty()) {
            for (String right: rights) {
                globalUserManagementService.grantRight(right, tenantId, username);
            }
        }

        return SimpleBooleanResponse.getSuccessResponse();
    }

    @RequestMapping(value = "", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete users (?usernames=) from current tenant. Request parameter should be a stringified JSON array.")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public ResponseDocument<DeleteUsersResult> delete(@RequestParam(value = "usernames") String usernames, HttpServletRequest request)
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
            response.setErrors(Arrays.asList("Could not parse the input name array."));
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
                    }
                } catch (Exception e) {
                    // ignore
                }
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

    private void revokeRightWithoutException(String right, String tenant, String username) {
        try {
            globalUserManagementService.revokeRight(right, tenant, username);
        } catch (Exception e) {
            // ignore
        }
    }

    private Boolean softDelete(String tenantId, String username) {
        try {
            revokeRightWithoutException(GrantedRight.VIEW_PLS_MODELS.getAuthority(), tenantId, username);
            revokeRightWithoutException(GrantedRight.VIEW_PLS_CONFIGURATION.getAuthority(), tenantId, username);
            revokeRightWithoutException(GrantedRight.VIEW_PLS_USERS.getAuthority(), tenantId, username);
            revokeRightWithoutException(GrantedRight.VIEW_PLS_REPORTING.getAuthority(), tenantId, username);
            revokeRightWithoutException(GrantedRight.EDIT_PLS_MODELS.getAuthority(), tenantId, username);
            revokeRightWithoutException(GrantedRight.EDIT_PLS_CONFIGURATION.getAuthority(), tenantId, username);
            revokeRightWithoutException(GrantedRight.EDIT_PLS_USERS.getAuthority(), tenantId, username);
            return true;
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
    }

    private void grantDefaultRights(String tenantId, String username) {
        List<GrantedRight> rights = GrantedRight.getDefaultRights();
        for (GrantedRight right: rights) {
            try {
                globalUserManagementService.grantRight(right.getAuthority(), tenantId, username);
            } catch (LedpException e) {
                // ignore
            }
        }
    }

    private boolean isAdmin(String tenantId, String username) {
        //TODO:song this is temporary until GlboalAuth's GetRights method works properly
        List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = globalUserManagementService.getAllUsersOfTenant(tenantId);
        for (AbstractMap.SimpleEntry<User, List<String>> userRight: userRightsList) {
            if (userRight.getKey().getUsername().equals(username)) {
                return RightsUtilities.isAdmin(RightsUtilities.translateRights(userRight.getValue()));
            }
        }
        return false;
    }

    private boolean inTenant(String tenantId, String username) {
        //TODO:song this is temporary until GlboalAuth's GetRights method works properly
        List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = globalUserManagementService.getAllUsersOfTenant(tenantId);
        for (AbstractMap.SimpleEntry<User, List<String>> userRight: userRightsList) {
            if (userRight.getKey().getUsername().equals(username)) {
                return true;
            }
        }
        return false;
    }
}
