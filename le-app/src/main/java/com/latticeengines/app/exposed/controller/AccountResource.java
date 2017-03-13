package com.latticeengines.app.exposed.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.QueryTranslator;
import com.latticeengines.proxy.exposed.objectapi.AccountProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "accounts", description = "REST resource for serving data about accounts")
@RestController
@RequestMapping("/accounts")
public class AccountResource {
    @Autowired
    private AccountProxy accountProxy;

    @RequestMapping(value = "/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@RequestBody FrontEndQuery query) {
        return accountProxy.getCount(MultiTenantContext.getCustomerSpace().toString(), translate(query));
    }

    @RequestMapping(value = "/count/restriction", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified restriction")
    public long getCountForRestriction(@RequestBody FrontEndRestriction restriction) {
        FrontEndQuery query = new FrontEndQuery();
        query.setRestriction(restriction);
        return accountProxy.getCount(MultiTenantContext.getCustomerSpace().toString(), translate(query));
    }

    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@RequestBody FrontEndQuery query) {
        return accountProxy.getData(MultiTenantContext.getCustomerSpace().toString(), translate(query));
    }

    private Query translate(FrontEndQuery query) {
        QueryTranslator translator = new QueryTranslator(query, SchemaInterpretation.Account);
        return translator.translate();
    }
}
