package com.latticeengines.metadata.controller;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.MetadataProvisioningService;
import com.latticeengines.metadata.service.MetadataService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "admin", description = "REST resource for provisioning metadata component")
@RestController
@RequestMapping(value = "/admin")
public class AdminResource {

    @Inject
    private MetadataProvisioningService metadataProvisioningService;

    @Inject
    private MetadataService mdService;

    @PostMapping("/provision")
    @ResponseBody
    @ApiOperation(value = "Provision Import Tables")
    public Boolean provisionImportTables(@RequestBody Tenant tenant) {
        CustomerSpace space = CustomerSpace.parse(tenant.getId());
        if (CollectionUtils.isEmpty(mdService.getImportTables(space))) {
            metadataProvisioningService.provisionImportTables(space);
        }
        return true;
    }
}
