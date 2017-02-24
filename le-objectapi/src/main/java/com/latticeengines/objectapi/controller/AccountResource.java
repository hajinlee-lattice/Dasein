package com.latticeengines.objectapi.controller;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.network.exposed.objectapi.AccountInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "accounts", description = "REST resource for accounts")
@RestController
@RequestMapping("/accounts")
public class AccountResource implements AccountInterface {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(AccountResource.class);

    @Override
    @RequestMapping(value = "/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@RequestBody Query query) {
        return 0;
    }

    @Override
    @RequestMapping(value = "/lift", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the lift for the specified query")
    public double getLift(@RequestBody Query query) {
        return 0;
    }

    @Override
    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public List<Map<String, Object>> getData(@RequestBody Query query) {
        return null;
    }
}
