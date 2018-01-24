package com.latticeengines.apps.cdl.controller;


import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.PeriodService;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratingengine", description = "REST resource for periods")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/periods")
public class PeriodResource {

    private final PeriodService periodService;

    @Inject
    public PeriodResource(PeriodService periodService) {
        this.periodService = periodService;
    }

    @RequestMapping(value = "/names", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all period names defined in a tenant")
    public List<String> getPeriodNames(@PathVariable String customerSpace) {
        return periodService.getPeriodNames();
    }

    @RequestMapping(value = "/strategies", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all period names defined in a tenant")
    public List<PeriodStrategy> getPeriodStrategies(@PathVariable String customerSpace) {
        return periodService.getPeriodStrategies();
    }

}
