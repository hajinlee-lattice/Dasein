package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.SLAFulfillmentService;
import com.latticeengines.apps.cdl.service.SLATermService;
import com.latticeengines.domain.exposed.cdl.sla.SLAFulfillment;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "slaTerm", description = "REST resource for SLATerm management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/slaTerm")
public class SLATermResource {

    @Inject
    private SLATermService slaTermService;
    @Inject
    private SLAFulfillmentService slaFulfillmentService;

    @GetMapping("")
    @ResponseBody
    @ApiOperation("Get all slaTerms under current tenant")
    public List<SLATerm> getSlaTerms(@PathVariable(value = "customerSpace") String customerSpace) {
        return slaTermService.findByTenant(customerSpace);
    }

    @GetMapping("/getSlaFulfillments")
    @ResponseBody
    @ApiOperation("Get all slaFulfillment under current tenant")
    public List<SLAFulfillment> getSlaFulfillments(@PathVariable(value = "customerSpace") String customerSpace) {
        return slaFulfillmentService.findByTenant(customerSpace);
    }

    @PostMapping("")
    @ResponseBody
    @ApiOperation("Create or update a SLATerm under current tenant")
    public SLATerm createSlaTerm(@PathVariable(value = "customerSpace") String customerSpace, //
                                 @RequestBody SLATerm slaTerm) {
        return slaTermService.createOrUpdate(customerSpace, slaTerm);
    }

    @PostMapping("/createFulfillment")
    @ResponseBody
    @ApiOperation("Create or update a SLAFulfillment under current tenant")
    public SLAFulfillment createSlaFulfillment(@PathVariable(value = "customerSpace") String customerSpace, //
                                 @RequestBody SLAFulfillment slaFulfillment) {
        return slaFulfillmentService.createOrUpdate(customerSpace, slaFulfillment);
    }
}
