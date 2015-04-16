package com.latticeengines.security.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "adlogin", description = "REST resource for logging in using Active Directory")
@RestController
public class ActiveDirectoryLoginResource {

    @RequestMapping(value = "/adlogin", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Login using ActiveDirectory")
    public void login() {
    }

}