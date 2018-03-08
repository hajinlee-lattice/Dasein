package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.PurchaseMetricsService;
import com.latticeengines.domain.exposed.serviceapps.cdl.PurchaseMetrics;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "purchasemetrics", description = "REST resource for purchase metrics management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/purchasemetrics")
public class PurchaseMetricsResource {
    @Inject
    private PurchaseMetricsService purchaseMetricsService;

    @GetMapping(value = "")
    @ApiOperation(value = "Get active purchase metrics")
    public List<PurchaseMetrics> getPurchaseMetrics(@PathVariable String customerSpace) {
        return purchaseMetricsService.findAllActive();
    }

    @PostMapping(value = "")
    @ApiOperation(value = "Save purchase metrics")
    public List<PurchaseMetrics> savePurchaseMetrics(@PathVariable String customerSpace,
            @RequestBody List<PurchaseMetrics> purchaseMetrics) {
        return purchaseMetricsService.save(purchaseMetrics);
    }
}
