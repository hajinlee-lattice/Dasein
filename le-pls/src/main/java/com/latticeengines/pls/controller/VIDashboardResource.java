package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardResponse;
import com.latticeengines.pls.service.vidashboard.DashboardService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "VI dashboard resource", description = "REST resource for action")
@RestController
@RequestMapping("/vidashboards")
@PreAuthorize("hasRole('View_PLS_Data')")
public class VIDashboardResource {

    @Inject
    private DashboardService dashboardService;

    @GetMapping("/dashboards")
    @ResponseBody
    @ApiOperation("get all related dashboards")
    public DashboardResponse getDashboardList() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return dashboardService.getDashboardList(customerSpace.toString());
    }
}
