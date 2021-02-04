package com.latticeengines.objectapi.controller;

import static com.latticeengines.query.factory.RedshiftQueryProvider.USER_SEGMENT;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;
import com.latticeengines.objectapi.service.PurchaseHistoryService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "entities", description = "REST resource for support Purchase History use cases")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/periodtransactions")
public class PeriodTransactionResource {

    private static final Logger log = LoggerFactory.getLogger(PeriodTransactionResource.class);

    @Inject
    private PurchaseHistoryService purchaseHistoryService;

    @GetMapping("/accountid/{accountId}")
    @ResponseBody
    @ApiOperation("Get all PeriodTransactions for the given AccountID")
    public List<PeriodTransaction> getPeriodTransactionByAccountId(@PathVariable String customerSpace,
            @PathVariable String accountId, //
            @RequestParam(value = "sqlUser", required = false, defaultValue = USER_SEGMENT) String sqlUser, //
            @RequestParam(value = "periodname", required = false, defaultValue = "Month") String periodName, //
            @RequestParam(value = "producttype", required = false, defaultValue = "Spending") ProductType productType) {
        if (!"Month".equalsIgnoreCase(periodName) || !ProductType.Spending.equals(productType)) {
            log.warn("Invalid period transaction query, customer={}, accountId={}, periodName={} and productType={}", //
                    CustomerSpace.shortenCustomerSpace(customerSpace), accountId, periodName, productType);
        }

        PeriodStrategy.Template period = PeriodStrategy.Template.fromName(periodName);
        if (null == period) {
            throw new LedpException(LedpCode.LEDP_42001, new String[] { periodName });
        }

        return purchaseHistoryService.getPeriodTransactionsByAccountId(accountId, period.name(), productType, sqlUser);
    }

    @GetMapping("/spendanalyticssegments")
    @ResponseBody
    @ApiOperation("Get All Segments")
    public DataPage getAllSegments(@PathVariable String customerSpace, //
                                   @RequestParam(value = "sqlUser", required = false, defaultValue = USER_SEGMENT) String sqlUser) {
        return purchaseHistoryService.getAllSpendAnalyticsSegments(sqlUser);
    }

    @GetMapping("/spendanalyticssegment/{spendAnalyticsSegment}")
    @ResponseBody
    @ApiOperation("Get Period Transactions for all accounts with the given spendAnalyticsSegment value")
    public List<PeriodTransaction> getPeriodTransactionsForSegmentAccounts(@PathVariable String customerSpace,
            @PathVariable String spendAnalyticsSegment, //
            @RequestParam(value = "sqlUser", required = false, defaultValue = USER_SEGMENT) String sqlUser, //
            @RequestParam(value = "periodname", required = false, defaultValue = "Month") String periodName, //
            @RequestParam(value = "producttype", required = false, defaultValue = "Spending") ProductType productType) {
        if (!"Month".equalsIgnoreCase(periodName) || !ProductType.Spending.equals(productType)) {
            log.warn("Invalid period transaction query, customer={}, segment={}, periodName={} and productType={}", //
                    CustomerSpace.shortenCustomerSpace(customerSpace), spendAnalyticsSegment, periodName, productType);
        }

        PeriodStrategy.Template period = PeriodStrategy.Template.fromName(periodName);
        if (null == period) {
            throw new LedpException(LedpCode.LEDP_42001, new String[] { periodName });
        }

        return purchaseHistoryService.getPeriodTransactionsForSegmentAccounts(spendAnalyticsSegment, period.name(),
                productType, sqlUser);
    }

    @GetMapping("/producthierarchy")
    @ResponseBody
    @ApiOperation("Get ProductHierarchy")
    public List<ProductHierarchy> getProductHierarchy(@PathVariable String customerSpace,
            @RequestParam(value = "sqlUser", required = false, defaultValue = USER_SEGMENT) String sqlUser, //
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return purchaseHistoryService.getProductHierarchy(version, sqlUser);
    }

    @GetMapping("/transaction/maxmindate")
    @ResponseBody
    @ApiOperation("Get final and first transaction date")
    public List<String> getFinalAndFirstTransactionDate(@PathVariable String customerSpace,
            @RequestParam(value = "sqlUser", required = false, defaultValue = USER_SEGMENT) String sqlUser, //
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return purchaseHistoryService.getFinalAndFirstTransactionDate(sqlUser);
    }

}
