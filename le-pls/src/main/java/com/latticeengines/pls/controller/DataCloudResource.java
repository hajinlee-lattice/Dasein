package com.latticeengines.pls.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datacloud", description = "REST resource for source datacloud")
@RestController
@RequestMapping("/datacloud")
@PreAuthorize("hasRole('View_PLS_Models')")
public class DataCloudResource {
    private static final Logger log = LoggerFactory.getLogger(DataCloudResource.class);

    @RequestMapping(value = "/customerreports/incorrectlookups", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one customer report")
    public ResponseDocument<String> createLookupCustomerReport(@RequestBody CustomerReport report) {
        log.debug(String.format("customer report %s is created", report));
        return null;
    }

    @RequestMapping(value = "/customerreports/incorrectmatchedattrs", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one customer report")
    public ResponseDocument<String> createMatchedAttrsCustomerReport(@RequestBody CustomerReport report) {
        log.debug(String.format("customer report %s is created", report));
        return null;
    }
}
