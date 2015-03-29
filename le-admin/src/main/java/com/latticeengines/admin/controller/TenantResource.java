package com.latticeengines.admin.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;


@Api(value = "tenantadmin", description = "REST resource for managing Lattice tenants across all products")
@RestController
@RequestMapping(value = "/tenants")
public class TenantResource {

    
    @RequestMapping(value = "/{tenantId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Bootstrap a Lattice tenant")
    public void bootstrap(@RequestParam String tenantId) {
       
    }
}
