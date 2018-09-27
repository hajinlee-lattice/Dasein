package com.latticeengines.ulysses.controller;

import java.time.LocalDate;
import java.util.Arrays;
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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.util.BusinessCalendarUtils;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
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

    @Inject
    private PeriodProxy periodProxy;

    private final int DEFAULT_START_YEAR = 2000;

    @Inject
    @Qualifier(PurchaseHistoryDanteFormatter.Qualifier)
    private PurchaseHistoryDanteFormatter purchaseHistoryDanteFormatter;

    private String defaultPeriodName = PeriodStrategy.Template.Month.name();

    @RequestMapping(value = "/account/{crmAccountId}/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
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
                if (CollectionUtils.isEmpty(periodTransactions)) {
                    throw new LedpException(LedpCode.LEDP_39006, new String[] { crmAccountId, customerSpace });
                }
                BusinessCalendar businessCalendar = periodProxy.getBusinessCalendar(customerSpace);
                LocalDate startDate;
                if (businessCalendar == null) {
                    // Use Natural calendar since no Business calender has been
                    // defined
                    startDate = LocalDate.of(DEFAULT_START_YEAR, 1, 1);
                } else if (businessCalendar.getMode() == BusinessCalendar.Mode.STARTING_DATE) {
                    startDate = BusinessCalendarUtils.parseLocalDateFromStartingDate(businessCalendar.getStartingDate(),
                            DEFAULT_START_YEAR);
                } else {
                    startDate = BusinessCalendarUtils.parseLocalDateFromStartingDay(businessCalendar.getStartingDay(),
                            DEFAULT_START_YEAR);
                }
                return new FrontEndResponse<>(Collections.singletonList(purchaseHistoryDanteFormatter.format(crmAccountId,
                        startDate, JsonUtils.convertList(periodTransactions, PeriodTransaction.class))));
            }
        } catch (LedpException le) {
            log.error("Failed to populate purchase history for account: " + crmAccountId, le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to populate purchase history for account: " + crmAccountId, e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }

    }

    @RequestMapping(value = "/spendanalyticssegment/{spendAnalyticsSegment}/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
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
            List<PeriodTransaction> periodTransactions = periodTransactionProxy
                    .getPeriodTransactionsForSegmentAccounts(customerSpace, spendAnalyticsSegment, defaultPeriodName);

            if (CollectionUtils.isEmpty(periodTransactions)) {
                throw new LedpException(LedpCode.LEDP_39006, new String[] { spendAnalyticsSegment, customerSpace });
            }

            BusinessCalendar businessCalendar = periodProxy.getBusinessCalendar(customerSpace);
            LocalDate startDate;
            if (businessCalendar.getMode() == BusinessCalendar.Mode.STARTING_DATE) {
                startDate = BusinessCalendarUtils.parseLocalDateFromStartingDate(businessCalendar.getStartingDate(),
                        DEFAULT_START_YEAR);
            } else {
                startDate = BusinessCalendarUtils.parseLocalDateFromStartingDay(businessCalendar.getStartingDay(),
                        DEFAULT_START_YEAR);
            }
            return new FrontEndResponse<>(Collections.singletonList(purchaseHistoryDanteFormatter.format(spendAnalyticsSegment,
                    startDate, JsonUtils.convertList(periodTransactions, PeriodTransaction.class))));
        } catch (LedpException le) {
            log.error("Failed to populate purchase history for segment: " + spendAnalyticsSegment, le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to populate purchase history for segment: " + spendAnalyticsSegment, e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }

    }
}