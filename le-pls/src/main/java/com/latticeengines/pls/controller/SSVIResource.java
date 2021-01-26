package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.NamingUtils;
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

    @PostMapping("/createListSegment")
    @ResponseBody
    @ApiOperation("create list segment")
    public MetadataSegment createSegment(@RequestParam(value = "segmentName", required = false) String segmentName) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (segmentName == null) {
            segmentName = NamingUtils.timestamp(customerSpace.toString());
        }
        return dashboardService.createListSegment(customerSpace.toString(), segmentName);
    }

    @PostMapping("/updateMappings")
    @ResponseBody
    @ApiOperation("update list segment field Mappings")
    public ListSegment createSegment(@RequestParam("segmentName") String segmentName, @RequestBody CSVAdaptor csvAdaptor) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return dashboardService.updateSegmentFieldMapping(customerSpace.toString(), segmentName, csvAdaptor);
    }

    @GetMapping("/getMappings")
    @ResponseBody
    @ApiOperation("get list segment field Mappings")
    public ListSegmentSummary getSegmentMappings(@RequestParam("segmentName") String segmentName) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return dashboardService.getListSegmentMappings(customerSpace.toString(), segmentName);
    }

    @GetMapping("/getSegmentSummary")
    @ResponseBody
    @ApiOperation("get list segment summary")
    public ListSegmentSummary getSegmentSummary(@RequestParam("segmentName") String segmentName) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return dashboardService.getListSegmentSummary(customerSpace.toString(), segmentName);
    }
}
