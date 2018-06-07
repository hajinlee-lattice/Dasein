package com.latticeengines.ulysses.controller;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

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
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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

    @Inject
    public DataLakeAccountResource(DataLakeService dataLakeService, Oauth2RestApiProxy tenantProxy,
            PeriodTransactionProxy periodTransactionProxy) {
        this.dataLakeService = dataLakeService;
        this.tenantProxy = tenantProxy;
        this.periodTransactionProxy = periodTransactionProxy;
    }

    @Inject
    @Qualifier(AccountDanteFormatter.Qualifier)
    private AccountDanteFormatter accountDanteFormatter;

    @RequestMapping(value = "/{accountId}/{attributeGroup}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id ")
    public DataPage getAccountById(RequestEntity<String> requestEntity, @PathVariable String accountId, //
            @PathVariable Predefined attributeGroup) {
        return dataLakeService.getAccountById(accountId, attributeGroup,
                tenantProxy.getOrgInfoFromOAuthRequest(requestEntity));
    }

    @RequestMapping(value = "/{accountId}/{attributeGroup}/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id in dante format")
    public FrontEndResponse<String> getAccountByIdInDanteFormat(RequestEntity<String> requestEntity,
            @PathVariable String accountId, //
            @PathVariable Predefined attributeGroup) {
        try {
            DataPage accountRawData = getAccountById(requestEntity, accountId, attributeGroup);
            if (accountRawData.getData().size() != 1) {
                throw new LedpException(LedpCode.LEDP_39003,
                        new String[] { "Account", "" + accountRawData.getData().size() });
            }
            return new FrontEndResponse<>(accountDanteFormatter.format(accountRawData.getData().get(0)));
        } catch (LedpException le) {
            log.error("Failed to get account data for account id: " + accountId, le);
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
                throw new LedpException(LedpCode.LEDP_39003,
                        new String[] { "Account", "" + accountRawData.getData().size() });
            }
            return new FrontEndResponse<>(Arrays.asList(accountDanteFormatter.format(accountRawData.getData().get(0))));
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
}
