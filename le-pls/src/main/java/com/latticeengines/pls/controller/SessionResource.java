package com.latticeengines.pls.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Session;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api("View current session info")
@RestController
public class SessionResource {

    @GetMapping("/session")
    @ResponseBody
    @ApiOperation(value = "Get current session")
    public Session getCurrentSession() {
        return MultiTenantContext.getSession();
    }

}
