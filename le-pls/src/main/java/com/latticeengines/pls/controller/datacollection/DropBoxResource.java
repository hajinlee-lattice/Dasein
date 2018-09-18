package com.latticeengines.pls.controller.datacollection;


import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dropbox", description = "REST resource for atlas drop box")
@RestController
@RequestMapping("/dropbox")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class DropBoxResource {

    @Inject
    private DropBoxProxy dropBoxProxy;

    @GetMapping("")
    @ApiOperation(value = "Get drop box summary")
    public DropBoxSummary getDropBox() {
        String customerSpace = MultiTenantContext.getShortTenantId();
        return dropBoxProxy.getDropBox(customerSpace);
    }

    @PutMapping("/access")
    @ApiOperation(value = "Grant external access to drop box")
    @PreAuthorize("hasRole('Edit_PLS_CDL_Data')")
    public GrantDropBoxAccessResponse grantAccess(@RequestBody GrantDropBoxAccessRequest request) {
        String customerSpace = MultiTenantContext.getShortTenantId();
        return dropBoxProxy.grantAccess(customerSpace, request);
    }


}
