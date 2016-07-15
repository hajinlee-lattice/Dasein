package com.latticeengines.securityapi.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Api(value = "adlogin", description = "REST resource for logging in using Active Directory")
@RestController
//@RequestMapping("/adlogin")
public class ActiveDirectoryLoginResource {

    @RequestMapping(value = "/adlogin", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Login using ActiveDirectory")
    public void login() {
    }

}
