package com.latticeengines.ulysses.controller;

import java.net.URLDecoder;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.RequestEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.proxy.exposed.objectapi.PeriodTransactionProxy;
import com.latticeengines.ulysses.utils.PurchaseHistoryDanteFormatter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import reactor.core.publisher.Flux;

@Api(value = "PurchaseHistory", description = "Common REST resource for Purchase History and Spend Analytics data")
@RestController
@RequestMapping("/purchasehistory")
public class PurchaseHistoryResource {
    private static final Logger log = LoggerFactory.getLogger(PurchaseHistoryResource.class);

    @Inject
    private PeriodTransactionProxy periodTransactionProxy;

    @Inject
    private Oauth2RestApiProxy tenantProxy;

    @Inject
    private DataLakeService dataLakeService;

    private final int DEFAULT_START_YEAR = 2000;
    // Spend Analytics always uses Natural Calendar with a start date of Jan 1st
    // 2000
    private final LocalDate DEFAULT_SPEND_ANALYTICS_START_DATE = LocalDate.of(DEFAULT_START_YEAR, 1, 1);

    @Inject
    @Qualifier(PurchaseHistoryDanteFormatter.Qualifier)
    private PurchaseHistoryDanteFormatter purchaseHistoryDanteFormatter;

    private String defaultPeriodName = PeriodStrategy.Template.Month.name();

    @GetMapping(value = "/account/{crmAccountId}/danteformat", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the purchase history data for the given account")
    public FrontEndResponse<List<String>> getPurchaseHistoryAccountById(RequestEntity<String> requestEntity,
            @PathVariable String crmAccountId) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        try {
            DataPage accountData = dataLakeService.getAccountById(crmAccountId, ColumnSelection.Predefined.TalkingPoint,
                    tenantProxy.getOrgInfoFromOAuthRequest(requestEntity));

            if (CollectionUtils.isEmpty(accountData.getData())) {
                throw new LedpException(LedpCode.LEDP_39001, new String[] { crmAccountId, customerSpace });
            } else {
                String accountId = accountData.getData().get(0).get(InterfaceName.AccountId.name()).toString();
                List<PeriodTransaction> periodTransactions = periodTransactionProxy.getPeriodTransactionsByAccountId(
                        customerSpace, accountId, defaultPeriodName, ProductType.Spending);
                List<String> maxAndMinTransactionDates = periodTransactionProxy
                        .getFinalAndFirstTransactionDate(customerSpace);
                return new FrontEndResponse<>(Collections.singletonList(
                        purchaseHistoryDanteFormatter.format(crmAccountId, DEFAULT_SPEND_ANALYTICS_START_DATE,
                                JsonUtils.convertList(periodTransactions, PeriodTransaction.class),
                                maxAndMinTransactionDates.get(0), maxAndMinTransactionDates.get(1))));
            }
        } catch (LedpException le) {
            log.warn("Failed to populate purchase history for account: " + crmAccountId, le.getMessage());
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to populate purchase history for account: " + crmAccountId, e.getMessage());
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }

    }

    @GetMapping(value = "/spendanalyticssegment/{spendAnalyticsSegment}/danteformat", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the purchase history data for all the accounts in the given spend analytics segment")
    public FrontEndResponse<List<String>> getPurchaseHistoryAccountBySegment(
            @PathVariable String spendAnalyticsSegment) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();

        Map<String, ColumnMetadata> attributeMap = Flux
                .fromIterable(dataLakeService.getCachedServingMetadataForEntity(customerSpace, BusinessEntity.Account))
                .collect(HashMap<String, ColumnMetadata>::new, (returnMap, cm) -> returnMap.put(cm.getAttrName(), cm))
                .block();
        if (!attributeMap.containsKey(InterfaceName.SpendAnalyticsSegment.name())) {
            log.error(InterfaceName.SpendAnalyticsSegment.name() + " does not exist for tenant: " + customerSpace);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_39008).getErrorDetails());
        }

        try {
            List<PeriodTransaction> periodTransactions = periodTransactionProxy.getPeriodTransactionsForSegmentAccounts(
                    customerSpace, URLDecoder.decode(spendAnalyticsSegment, "UTF-8"), defaultPeriodName);
            List<String> maxAndMinTransactionDates = periodTransactionProxy
                    .getFinalAndFirstTransactionDate(customerSpace);
            return new FrontEndResponse<>(Collections.singletonList(purchaseHistoryDanteFormatter.format(
                    URLDecoder.decode(spendAnalyticsSegment, "UTF-8"), DEFAULT_SPEND_ANALYTICS_START_DATE,
                    JsonUtils.convertList(periodTransactions, PeriodTransaction.class),
                    maxAndMinTransactionDates.get(0), maxAndMinTransactionDates.get(1))));
        } catch (LedpException le) {
            log.warn("Failed to populate purchase history for segment: " + spendAnalyticsSegment, le.getMessage());
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to populate purchase history for segment: " + spendAnalyticsSegment, e.getMessage());
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }

    }
}
