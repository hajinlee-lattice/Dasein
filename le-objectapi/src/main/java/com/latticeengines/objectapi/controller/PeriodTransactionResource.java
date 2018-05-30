package com.latticeengines.objectapi.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;
import com.latticeengines.objectapi.service.PurchaseHistoryService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "entities", description = "REST resource for support Purchase History use cases")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/periodtransaction")
public class PeriodTransactionResource {

    @Inject
    private PurchaseHistoryService purchaseHistoryService;

    @GetMapping(value = "/accountid/{accountId}")
    @ResponseBody
    @ApiOperation("Get Period Transaction by AccountID")
    public List<PeriodTransaction> getPeriodTransactionByAccountId(@PathVariable String customerSpace,
            @PathVariable String accountId, //
            @RequestParam(value = "periodname", required = false, defaultValue = "Month") String periodName, //
            @RequestParam(value = "version", required = false) DataCollection.Version version, //
            @RequestParam(value = "producttype", required = false, defaultValue = "Spending") ProductType productType) {
        return purchaseHistoryService.getPeriodTransactionByAccountId(accountId, periodName, version, productType);
    }

    @GetMapping(value = "/segments")
    @ResponseBody
    @ApiOperation("Get All Segments")
    public List<String> getAllSegments(@PathVariable String customerSpace) {
        return purchaseHistoryService.getAllSegments();
    }

    @GetMapping(value = "/segment/{segment}")
    @ResponseBody
    @ApiOperation("Get Period Transaction for Segment Account")
    public List<PeriodTransaction> getPeriodTransactionForSegmentAccount(@PathVariable String customerSpace,
            @PathVariable String segment, //
            @RequestParam(value = "periodname", required = false, defaultValue = "Month") String periodName, //
            @RequestParam(value = "producttype", required = false, defaultValue = "Spending") ProductType productType) {
        return purchaseHistoryService.getPeriodTransactionForSegmentAccount(segment, periodName, productType);
    }

    @GetMapping(value = "/producthierarchy")
    @ResponseBody
    @ApiOperation("Get ProductHierarchy")
    public List<ProductHierarchy> getProductHierarchy(@PathVariable String customerSpace,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return purchaseHistoryService.getProductHierarchy(version);
    }

}
