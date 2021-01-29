package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardResponse;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.ListSegmentSummary;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.pls.service.vidashboard.DashboardService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Self Service Visitor Intelligence APIs")
@RestController
@RequestMapping("/ssvi")
@PreAuthorize("hasRole('View_PLS_Data')")
public class SSVIResource {

    @Inject
    private DashboardService dashboardService;

    @GetMapping("/dashboards")
    @ResponseBody
    @ApiOperation("get all related dashboards")
    public DashboardResponse getDashboardList() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return dashboardService.getDashboardList(customerSpace.toString());
    }

    @PostMapping("/target-account-lists")
    @ResponseBody
    @ApiOperation("create default target account list segment")
    public MetadataSegment createDefaultTargetAccountList() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return dashboardService.createTargetAccountList(customerSpace.toString(), null);
    }

    @GetMapping("/target-account-lists/default")
    @ResponseBody
    @ApiOperation("retrieve default target account list segment")
    public ListSegmentSummary getDefaultTargetAccountList() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return dashboardService.getTargetAccountList(customerSpace.toString(), null);
    }

    @PutMapping("/target-account-lists/default/mappings")
    @ResponseBody
    @ApiOperation("update default target account list segment's field Mappings")
    public ListSegment updateTargetAccountListMappings(@RequestBody CSVAdaptor csvAdaptor) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return dashboardService.updateTargetAccountListMapping(customerSpace.toString(), null, csvAdaptor);
    }
}
