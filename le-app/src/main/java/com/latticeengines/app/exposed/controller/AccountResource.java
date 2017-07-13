package com.latticeengines.app.exposed.controller;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.QueryDecorator;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "accounts", description = "REST resource for serving data about accounts")
@RestController
@RequestMapping("/accounts")
public class AccountResource extends BaseFrontEndEntityResource {

    @RequestMapping(value = "/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@RequestBody FrontEndQuery frontEndQuery) {
        return super.getCount(BusinessEntity.Account, frontEndQuery);
    }

    @RequestMapping(value = "/count/restriction", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified restriction")
    public long getCountForRestriction(@RequestBody FrontEndRestriction restriction) {
        return super.getCountForRestriction(BusinessEntity.Account, restriction);
    }

    @Override
    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@RequestBody FrontEndQuery frontEndQuery) {
        return super.getData(frontEndQuery);
    }

    @Override
    public QueryDecorator getQueryDecorator(boolean addSelects) {
        return new QueryDecorator() {

            @Override
            public BusinessEntity getLookupEntity() {
                return BusinessEntity.Account;
            }

            @Override
            public String[] getEntityLookups() {
                return new String[] { "SalesforceAccountID" };
            }

            @Override
            public String[] getLDCLookups() {
                return new String[] { "LDC_Domain", "LDC_Name", "LDC_Country", "LDC_City", "LDC_State" };
            }

            @Override
            public BusinessEntity getFreeTextSearchEntity() {
                return BusinessEntity.LatticeAccount;
            }

            @Override
            public String[] getFreeTextSearchAttrs() {
                return new String[] { "LDC_Domain", "LDC_Name" };
            }

            @Override
            public boolean addSelects() {
                return addSelects;
            }
        };
    }

}
