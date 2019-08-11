package com.latticeengines.ulysses.controller;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.RequestEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.proxy.exposed.objectapi.PeriodTransactionProxy;
import com.latticeengines.ulysses.utils.AccountDanteFormatter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "CDLAccounts", description = "Common REST resource to lookup CDL accounts")
@RestController
@RequestMapping("/datacollection/accounts")
public class DataLakeAccountResource {
    private static final Logger log = LoggerFactory.getLogger(DataLakeAccountResource.class);
    private final DataLakeService dataLakeService;
    private final Oauth2RestApiProxy tenantProxy;
    private final PeriodTransactionProxy periodTransactionProxy;
    private final BatonService batonService;

    @Inject
    public DataLakeAccountResource(DataLakeService dataLakeService, Oauth2RestApiProxy tenantProxy,
            PeriodTransactionProxy periodTransactionProxy, BatonService batonService) {
        this.dataLakeService = dataLakeService;
        this.tenantProxy = tenantProxy;
        this.periodTransactionProxy = periodTransactionProxy;
        this.batonService = batonService;
    }

    @Inject
    @Qualifier(AccountDanteFormatter.Qualifier)
    private AccountDanteFormatter accountDanteFormatter;

    @RequestMapping(value = "/{accountId}/{attributeGroup}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id ")
    public DataPage getAccountById(RequestEntity<String> requestEntity, @PathVariable String accountId, //
            @PathVariable Predefined attributeGroup) {
        return getAccountById(requestEntity, accountId, attributeGroup, null);
    }

    @RequestMapping(value = "/{accountId}/{attributeGroup}/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id in dante format")
    public FrontEndResponse<String> getAccountByIdInDanteFormat(RequestEntity<String> requestEntity,
            @PathVariable String accountId, //
            @PathVariable Predefined attributeGroup) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId());
            List<String> requiredAttributes = Collections.singletonList(InterfaceName.SpendAnalyticsSegment.name());
            DataPage accountRawData = getAccountById(requestEntity, accountId, attributeGroup, requiredAttributes);
            if (accountRawData.getData().size() != 1) {
                String message = MessageFormat.format(LedpCode.LEDP_39003.getMessage(), "Account", 0);
                log.warn("Failed to get account data for account id: " + message);
                return new FrontEndResponse<>(new ErrorDetails(LedpCode.LEDP_39003, message, null));
            }
            accountDanteFormatter.setIsEntityMatchEnabled(batonService.isEntityMatchEnabled(customerSpace));
            return new FrontEndResponse<>(accountDanteFormatter.format(accountRawData.getData().get(0)));
        } catch (LedpException le) {
            log.error("Failed to get account data for account id: " + accountId, le.getMessage());
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get account data for account id: " + accountId, e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

    @RequestMapping(value = "/{accountId}/{attributeGroup}/danteformat/aslist", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id in dante format")
    public FrontEndResponse<List<String>> getAccountsByIdInDanteFormat(RequestEntity<String> requestEntity,
            @PathVariable String accountId, //
            @PathVariable Predefined attributeGroup) {
        try {
            DataPage accountRawData = getAccountById(requestEntity, accountId, attributeGroup);
            if (accountRawData.getData().size() != 1) {
                String message = MessageFormat.format(LedpCode.LEDP_39003.getMessage(), "Account", 0);
                log.warn("Failed to get account data for account id: " + message);
                return new FrontEndResponse<>(new ErrorDetails(LedpCode.LEDP_39003, message, null));
            }
            CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId());
            batonService.isEntityMatchEnabled(customerSpace);
            accountDanteFormatter.setIsEntityMatchEnabled(batonService.isEntityMatchEnabled(customerSpace));
            return new FrontEndResponse<>(
                    Collections.singletonList(accountDanteFormatter.format(accountRawData.getData().get(0))));
        } catch (LedpException le) {
            log.error("Failed to get account data for account id: " + accountId, le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get account data for account id: " + accountId, e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

    @RequestMapping(value = "/spendanalyticssegments/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id in dante format")
    public FrontEndResponse<List<String>> getAccountSegmentsInDanteFormat() {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        try {
            return new FrontEndResponse<>(accountDanteFormatter
                    .format(periodTransactionProxy.getAllSpendAnalyticsSegments(customerSpace).getData()));
        } catch (LedpException le) {
            log.error("Failed to get spend analytics segments for customerspace : " + customerSpace, le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get spend analytics segments for customerSpace: " + customerSpace, e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

    private DataPage getAccountById(RequestEntity<String> requestEntity, String accountId, Predefined attributeGroup,
            List<String> requiredAttributes) {
        return CollectionUtils.isEmpty(requiredAttributes) //
                ? dataLakeService.getAccountById(accountId, attributeGroup,
                        tenantProxy.getOrgInfoFromOAuthRequest(requestEntity))
                : dataLakeService.getAccountById(accountId, attributeGroup,
                        tenantProxy.getOrgInfoFromOAuthRequest(requestEntity), requiredAttributes);
    }
}
