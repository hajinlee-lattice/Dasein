package com.latticeengines.apps.dcp.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.EntitlementService;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Entitlement Management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/entitlement")
public class EntitlementResource {

    @Inject
    private EntitlementService entitlementService;

    @GetMapping("/{domainName}/{recordType}")
    @ResponseBody
    @ApiOperation(value = "Get block drt entitlement")
    public DataBlockEntitlementContainer getEntitlement(@PathVariable String customerSpace,
            @PathVariable String domainName, @PathVariable String recordType) {
        return entitlementService.getEntitlement(customerSpace, domainName, recordType);
    }

}
