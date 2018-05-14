package com.latticeengines.ulysses.controller;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.ulysses.utils.AccountDanteFormatter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "CDLAccounts", description = "Common REST resource to lookup CDL accounts")
@RestController
@RequestMapping("/datacollection/accounts")
public class DataLakeAccountResource {

    private final DataLakeService dataLakeService;

    @Inject
    public DataLakeAccountResource(DataLakeService dataLakeService) {
        this.dataLakeService = dataLakeService;
    }

    @Inject
    @Qualifier(AccountDanteFormatter.Qualifier)
    private AccountDanteFormatter accountDanteFormatter;

    @RequestMapping(value = "/{accountId}/{attributeGroup}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id ")
    public DataPage getAccountById(@PathVariable String accountId, //
            @PathVariable Predefined attributeGroup) {
        return dataLakeService.getAccountById(accountId, attributeGroup);
    }

    @RequestMapping(value = "/{accountId}/{attributeGroup}/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id in dante format")
    public FrontEndResponse<String> getAccountByIdInDanteFormat(@PathVariable String accountId, //
            @PathVariable Predefined attributeGroup) {
        try {
            DataPage accountRawData = getAccountById(accountId, attributeGroup);
            if (accountRawData.getData().size() != 1) {
                throw new LedpException(LedpCode.LEDP_39003,
                        new String[] { "Account", "" + accountRawData.getData().size() });
            }
            return new FrontEndResponse<>(accountDanteFormatter.format(accountRawData.getData().get(0)));
        } catch (LedpException le) {
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

    @RequestMapping(value = "/accountsegments/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id in dante format")
    public FrontEndResponse<String> getAccountSegmentsInDanteFormat() {
        try {
            return new FrontEndResponse<>(tempData);
        } catch (LedpException le) {
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

    private static final String tempData = "{\"BaseExternalID\":\"AllSegment\",\"NotionName\":\"DanteAccount\",\"Address1\":null,\"Address2\":null,\"BestLikelihood\":null,\"City\":null,\"Country\":null,\"DisplayName\":\"All Segment\",\"EstimatedRevenue\":null,\"IsSegment\":true,\"LastModified\":null,\"LeadCount\":null,\"NAICSCode\":null,\"NumberOfEmployees\":null,\"OwnerDisplayName\":null,\"RepresentativeAccounts\":1,\"SICCode\":null,\"SalesforceAccountID\":\"AllSegment\",\"Segment1Name\":\"AllSegment\",\"StateProvince\":null,\"Territory\":null,\"TopLeads\":null,\"TotalMonetaryValue\":null,\"URL\":null,\"Vertical\":null,\"Zip\":null}";
}
