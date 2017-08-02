package com.latticeengines.app.exposed.controller;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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
    public long getCount(@RequestBody FrontEndQuery frontEndQuery,
            @RequestParam(value = "segment", required = false) String segment) {
        try {
            return super.getCount(frontEndQuery, segment);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_36002, e);
        }
    }

    @Deprecated
    @RequestMapping(value = "/count/restriction", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified restriction")
    public long getCountForRestriction(@RequestBody FrontEndRestriction restriction) {
        try {
            return super.getCountForRestriction(restriction);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_36002, e);
        }
    }

    @Override
    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@RequestBody FrontEndQuery frontEndQuery,
            @RequestParam(value = "segment", required = false) String segment) {
        try {
            return super.getData(frontEndQuery, segment);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_36002, e);
        }
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

    @Override
    BusinessEntity getMainEntity() {
        return BusinessEntity.Account;
    }

}
