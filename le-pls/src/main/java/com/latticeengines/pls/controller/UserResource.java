package com.latticeengines.pls.controller;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.security.*;
import com.latticeengines.pls.globalauth.authentication.GlobalSessionManagementService;
import com.latticeengines.pls.security.RightsUtilities;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.pls.exception.LoginException;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.security.RestGlobalAuthenticationFilter;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

import java.util.*;

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

    @RequestMapping(value = "/add", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register new users")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public Boolean registerUser(@RequestBody UserRegistration userRegistration) {
        Credentials creds = userRegistration.getCredentials();
        boolean registered = globalUserManagementService.registerUser(userRegistration.getUser(), creds);

        if (!registered) {
            throw new LedpException(LedpCode.LEDP_18004, new String[] { creds.getUsername() });
        }

        AttributeMap aMap = new AttributeMap();
        aMap.put("Username", creds.getUsername());
        aMap.put("TenantId", userRegistration.getTenant().getId());
        return grantDefaultRights(aMap);
    }

    @RequestMapping(value = "/grant", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Grant default rights to a user")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public Boolean grantDefaultRights(@RequestBody AttributeMap attrMap) {
        String username = attrMap.get("Username");
        String tenantId = attrMap.get("TenantId");
        return globalUserManagementService.grantRight(GrantedRight.VIEW_PLS_MODELS.getAuthority(), tenantId, username);
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

    @RequestMapping(value = "/{userName:.+}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a user")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public Boolean update(@PathVariable String userName, @RequestBody AttributeMap attrMap) {
        boolean success = false;
        if (userName.charAt(0) == '"' && userName.charAt(userName.length() - 1) == '"') {
            userName = userName.substring(1, userName.length() - 1);
        }
        // change password
        if (attrMap.containsKey("OldPassword") && attrMap.containsKey("NewPassword")) {
            String oldPassword  = attrMap.get("OldPassword");
            String newPassword  = attrMap.get("NewPassword");

            Credentials oldCreds = new Credentials();
            oldCreds.setUsername(userName);
            oldCreds.setPassword(oldPassword);

            Credentials newCreds = new Credentials();
            newCreds.setUsername(userName);
            newCreds.setPassword(newPassword);

            try {
                Ticket ticket = globalAuthenticationService.authenticateUser(userName, oldPassword);
                success = globalUserManagementService.modifyLatticeCredentials(ticket, oldCreds, newCreds);
            } catch (LedpException e) {
                if (e.getCode() == LedpCode.LEDP_18001) {
                    throw new LoginException(e);
                }
                throw e;
            }
        }

        //TODO:song update other User attributes
        return success;
    }

    @RequestMapping(value = "/{username:.+}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Soft delete a user by revoking all rights to current tenant")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public Boolean delete(@PathVariable String username, HttpServletRequest request) {
        try {
            Ticket ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            Tenant tenant = globalSessionManagementService.retrieve(ticket).getTenant();
            return softDelete(tenant.getId(), username);
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
    }

    @RequestMapping(value = "/bulkdelete", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Soft delete a group of users in a tenant")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public Map<String, Object> bulkDelete(@RequestBody List<User> users, HttpServletRequest request) {
        Map<String, Object> result = new HashMap<>();
        try {
            Ticket ticket = new Ticket(request.getHeader(RestGlobalAuthenticationFilter.AUTHORIZATION));
            Tenant tenant = globalSessionManagementService.retrieve(ticket).getTenant();
            String tenantId = tenant.getId();
            List<User> successUsers = new ArrayList<>();
            List<User> failUsers = new ArrayList<>();
            for (User user : users) {
                try {
                    if (softDelete(tenantId, user.getUsername())) {
                        successUsers.add(user);
                    }
                } catch (Exception e) {
                    failUsers.add(user);
                }
            }
            result.put("SuccessUsers", successUsers);
            result.put("FailUsers", failUsers);
            return result;
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
    }

    @RequestMapping(value = "/byemail/{email:.+}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a user by email address")
    public User getByEmail(@PathVariable String email) {
        try {
            return globalUserManagementService.getUserByEmail(email);
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
    }

    @RequestMapping(value = "/resetpassword/{username:.+}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Reset the password for a user")
    @PreAuthorize("hasRole('Edit_PLS_Users')")
    public String resetPassword(@PathVariable String username) {
        try {
            return globalUserManagementService.resetLatticeCredentials(username);
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
    }

    @RequestMapping(value = "/all/{tenantId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register new users")
    @PreAuthorize("hasRole('View_PLS_Users')")
    public List<User> getAllUsersOfTenant(@PathVariable String tenantId) {
        try {
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = globalUserManagementService.getAllUsersOfTenant(tenantId);
            List<User> users = new ArrayList<>();
            for (AbstractMap.SimpleEntry<User, List<String>> userRights : userRightsList) {
                if (RightsUtilities.isAdmin(RightsUtilities.translateRights(userRights.getValue()))) { continue; }
                users.add(userRights.getKey());
            }
            return users;
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
    }

    private void revokeRightWithoutException(String right, String tenant, String username) {
        try {
            globalUserManagementService.revokeRight(right, tenant, username);
        } catch (Exception e) {
            return;
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
}
