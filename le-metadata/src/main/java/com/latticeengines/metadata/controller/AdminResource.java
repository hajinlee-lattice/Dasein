package com.latticeengines.metadata.controller;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AdminResource.class);

    @Autowired
    private MetadataProvisioningService metadataProvisioningService;

    @Autowired
    private MetadataService mdService;

    @RequestMapping(value = "/provision", method = RequestMethod.POST, headers = "Accept=application/json")
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
