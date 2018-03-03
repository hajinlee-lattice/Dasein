package com.latticeengines.apps.cdl.controller;


import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.apps.core.service.ApplicationTenantService;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLBootstrapRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "tenant", description = "REST resource for tenant management in a service app")
@RestController
@RequestMapping("/tenant")
public class ApplicationTenantResource {

    @Inject
    private ApplicationTenantService applicationTenantService;

    @PostMapping(value = "/")
    @ApiOperation(value = "Bootstrap a tenant for a service app")
    @NoCustomerSpace
    public SimpleBooleanResponse bootstrap(CDLBootstrapRequest bootstrapRequest) {
        applicationTenantService.bootstrap(bootstrapRequest);
        return SimpleBooleanResponse.successResponse();
    }

    @DeleteMapping(value = "/{tenantId}/")
    @ApiOperation(value = "Wipe out a tenant from a service app")
    @NoCustomerSpace
    public SimpleBooleanResponse wipeout(@PathVariable(name = "tenantId") String tenantId) {
        applicationTenantService.cleanup(tenantId);
        return SimpleBooleanResponse.successResponse();
    }

}
