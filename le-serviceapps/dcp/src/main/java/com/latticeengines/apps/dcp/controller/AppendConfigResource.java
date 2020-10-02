package com.latticeengines.apps.dcp.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.AppendConfigService;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Append Configuration")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/append-config")
public class AppendConfigResource {

    @Inject
    private AppendConfigService appendConfigService;

    @GetMapping("/entitlement/{domainName}/{recordType}")
    @ResponseBody
    @ApiOperation(value = "Get block drt entitlement")
    public DataBlockEntitlementContainer getEntitlement(@PathVariable String customerSpace,
            @PathVariable String domainName, @PathVariable String recordType) {
        return appendConfigService.getEntitlement(customerSpace, domainName, recordType);
    }

}
