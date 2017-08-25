package com.latticeengines.datacloudapi.api.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "customerreport", description = "REST resource for source customer reports")
@RestController
@RequestMapping("/customerreports")
public class CustomerReportsResource {

    private static final Logger log = LoggerFactory.getLogger(CustomerReportsResource.class);

    @RequestMapping(value = "/incorrectlookups", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one customer report")
    public ResponseDocument<String> createLookupCustomerReport(@RequestBody CustomerReport report) {
        log.debug(String.format("customer report %s is created", report));
        return null;
    }

    @RequestMapping(value = "/incorrectmatchedattrs", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one customer report")
    public ResponseDocument<String> createMatchedAttrsCustomerReport(@RequestBody CustomerReport report) {
        log.debug(String.format("customer report %s is created", report));
        return null;
    }
}
