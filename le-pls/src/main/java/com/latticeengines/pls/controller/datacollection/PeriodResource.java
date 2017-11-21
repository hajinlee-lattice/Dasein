package com.latticeengines.pls.controller.datacollection;

import java.util.List;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "periods", description = "REST resource for serving data about periods")
@RestController
@RequestMapping("/datacollection/periods")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class PeriodResource {

    private final PeriodProxy periodProxy;

    @Inject
    public PeriodResource(PeriodProxy periodProxy) {
        this.periodProxy = periodProxy;
    }

    @RequestMapping(value = "/names", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all period names defined in a tenant")
    public List<String> getPeriodNames() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return periodProxy.getPeriodNames(customerSpace);
    }

}
