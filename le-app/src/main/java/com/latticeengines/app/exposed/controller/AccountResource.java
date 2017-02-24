package com.latticeengines.app.exposed.controller;

import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "accounts", description = "REST resource for serving data about accounts")
@RestController
@RequestMapping("/accounts")
public class AccountResource {
    @RequestMapping(value = "/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@RequestBody FrontEndQuery query) {
        return 1;
    }

    @RequestMapping(value = "/lift", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the lift for the specified query")
    public double getLift(@RequestBody FrontEndQuery query) {
        return 0;
    }

    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public List<Map<String, Object>> getData(@RequestBody FrontEndQuery query) {
        return null;
    }
}
