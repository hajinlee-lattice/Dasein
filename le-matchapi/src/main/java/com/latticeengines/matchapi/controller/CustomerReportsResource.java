package com.latticeengines.matchapi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.matchapi.service.CustomerReportService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "customerreport", description = "REST resource for source customer reports")
@RestController
@RequestMapping("/customerreports")
public class CustomerReportsResource {

    private static final Logger log = LoggerFactory.getLogger(CustomerReportsResource.class);
    @Autowired
    private CustomerReportService customerReportService;

    @RequestMapping(value = "/", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Insert one customer report")
    public Boolean createCustomerReport(@RequestBody CustomerReport report) {
        log.debug(String.format("customer report %s is created", report));
        customerReportService.saveReport(report);
        return Boolean.TRUE;
    }

    @RequestMapping(value = "/{customerId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Insert one customer report")
    public CustomerReport getById(@PathVariable String customerId) {
        log.debug(String.format("Get customer report with id %s", customerId));
        return customerReportService.findById(customerId);
    }
}
