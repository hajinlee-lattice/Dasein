package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DashboardFilterService;
import com.latticeengines.apps.cdl.service.DashboardService;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dashboard", description = "REST resource for dashboard management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/vireports/dashboard")
public class DashboardResource {

    @Inject
    private DashboardService dashboardService;
    @Inject
    private DashboardFilterService dashboardFilterService;

    @GetMapping("")
    @ResponseBody
    @ApiOperation("Get all Dashboard under current tenant")
    public List<Dashboard> getDashboards(@PathVariable(value = "customerSpace") String customerSpace) {
        return dashboardService.findAllByTenant(customerSpace);
    }

    @GetMapping("/filters")
    @ResponseBody
    @ApiOperation("Get all DashboardFilter under current tenant")
    public List<DashboardFilter> getDashboardFilters(@PathVariable(value = "customerSpace") String customerSpace) {
        return dashboardFilterService.findAllByTenant(customerSpace);
    }

    @PostMapping("")
    @ResponseBody
    @ApiOperation("Create or update a Dashboard under current tenant")
    public Dashboard createDashboard( //
                                    @PathVariable(value = "customerSpace") String customerSpace, //
                                    @RequestBody Dashboard dashboard) {
        return dashboardService.createOrUpdate(customerSpace, dashboard);
    }

    @PostMapping("/createList")
    @ResponseBody
    @ApiOperation("Create or update a Dashboard list under current tenant")
    public void createDashboard( //
                                      @PathVariable(value = "customerSpace") String customerSpace, //
                                      @RequestBody List<Dashboard> dashboards) {
        dashboardService.createOrUpdateAll(customerSpace, dashboards);
    }

    @PostMapping("/createFilter")
    @ResponseBody
    @ApiOperation("Create or update a DashboardFilter under current tenant")
    public DashboardFilter createDashboardFilter( //
                                      @PathVariable(value = "customerSpace") String customerSpace, //
                                      @RequestBody DashboardFilter dashboardFilter) {
        return dashboardFilterService.createOrUpdate(customerSpace, dashboardFilter);
    }

    @GetMapping("/name/{name}")
    @ResponseBody
    @ApiOperation("Get a Dashboard by name from current tenant")
    public Dashboard getDashboard(@PathVariable(value = "customerSpace") String customerSpace,
                                 @PathVariable String name) {
        return dashboardService.findByName(customerSpace, name);
    }

    @GetMapping("/filterName/{filterName}")
    @ResponseBody
    @ApiOperation("Get a DashboardFilter by filterName from current tenant")
    public DashboardFilter getDashboardFilter(@PathVariable(value = "customerSpace") String customerSpace,
                                  @PathVariable String filterName) {
        return dashboardFilterService.findByName(customerSpace, filterName);
    }
}
