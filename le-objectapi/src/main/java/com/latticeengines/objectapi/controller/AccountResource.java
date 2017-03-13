package com.latticeengines.objectapi.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.network.exposed.objectapi.AccountInterface;
import com.latticeengines.objectapi.object.AccountObject;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "accounts", description = "REST resource for accounts")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class AccountResource implements AccountInterface {

    @Autowired
    private AccountObject accountObject;

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(AccountResource.class);

    @Override
    @RequestMapping(value = "/accounts/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@PathVariable String customerSpace, @RequestBody Query query) {
        return accountObject.getCount(query);
    }

    @Override
    @RequestMapping(value = "/accounts/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@PathVariable String customerSpace, @RequestBody Query query) {
        return accountObject.getData(query);
    }
}
