package com.latticeengines.ulysses.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.PeriodTransactionProxy;
import com.latticeengines.ulysses.utils.PurchaseHistoryDanteFormatter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "PurchaseHistory", description = "Common REST resource for Purchase History and Spend Analytics data")
@RestController
@RequestMapping("/purchasehistory")
public class PurchaseHistoryResource {
    private static final Logger log = LoggerFactory.getLogger(PurchaseHistoryResource.class);

    @Inject
    private PeriodTransactionProxy periodTransactionProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    @Qualifier(PurchaseHistoryDanteFormatter.Qualifier)
    private PurchaseHistoryDanteFormatter purchaseHistoryDanteFormatter;

    private String defaultPeriodName = PeriodStrategy.Template.Month.name();

    @RequestMapping(value = "/account/{accountId}/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the purchase history data for the given account")
    public FrontEndResponse<String> getPurchaseHistoryAccountById(@PathVariable String accountId) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        try {
            List<PeriodTransaction> periodTransactions = periodTransactionProxy.getPeriodTransactionByAccountId(
                    customerSpace, accountId, defaultPeriodName, dataCollectionProxy.getActiveVersion(customerSpace),
                    ProductType.Spending);
            return new FrontEndResponse<>(purchaseHistoryDanteFormatter.format(accountId,
                    JsonUtils.convertList(periodTransactions, PeriodTransaction.class)));
        } catch (LedpException le) {
            log.error("Failed to get the purchase history data", le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get the purchase history data", e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }

    }

    @RequestMapping(value = "/segment/{segmentName}/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the purchase history data for all the accounts in the given spend analytics segment")
    public FrontEndResponse<List<String>> getPurchaseHistoryAccountBySegment(@PathVariable String segmentName) {
        throw new UnsupportedOperationException();
    }

}