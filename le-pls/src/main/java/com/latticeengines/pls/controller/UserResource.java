package com.latticeengines.pls.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.exception.LoginException;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.security.RestGlobalAuthenticationFilter;
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

        return globalUserManagementService.grantRight(GrantedRight.VIEW_PLS_MODELS.getAuthority(), //
                userRegistration.getTenant().getId(), creds.getUsername());
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

    @RequestMapping(value = "/{userName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a user")
    public Boolean update(@PathVariable String userName, @RequestBody AttributeMap attrMap) {
        boolean success = false;

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


        //TODO:[13Feb2015] update other User attributes

        return success;
    }
}
