package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.exception.LoginException;
import com.latticeengines.pls.globalauth.authentication.GlobalSessionManagementService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.GrantedRight;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "user", description = "REST resource for user management")
@RestController
@RequestMapping("/users")
public class UserResource {

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

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

        return globalUserManagementService.grantRight(GrantedRight.VIEW_PLS_MODELS.getAuthority(), //
                userRegistration.getTenant().getId(), creds.getUsername());
    }

    @RequestMapping(value = "/logout", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Logout the user")
    public UserDocument logout() {
        UserDocument doc = new UserDocument();

        try {
            doc.setSuccess(true);
        } catch (LedpException e) {
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }
        return doc;
    }
}
