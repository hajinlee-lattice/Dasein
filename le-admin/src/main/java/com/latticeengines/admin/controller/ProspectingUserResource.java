package com.latticeengines.admin.controller;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.service.ProspectingUserService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.security.UserRegistration;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "internal_user_resource", description = "REST service resource for internally create prospecting users")
@RestController
@RequestMapping(value = "/prospectingusers")
public class ProspectingUserResource {

    @Inject
    private ProspectingUserService prospectingUserService;

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Register or validate a new user in the current tenant")
    @PreAuthorize("hasRole('Platform Operations')")
    public RegistrationResult addPrevisionUser(@RequestBody UserRegistration userReg) {
        RegistrationResult result = prospectingUserService.createUser(userReg);
        if (!result.isValid()) {
            throw new LedpException(LedpCode.LEDP_19111, new String[] { userReg.getUser().getUsername() });
        }
        return result;
    }
}
