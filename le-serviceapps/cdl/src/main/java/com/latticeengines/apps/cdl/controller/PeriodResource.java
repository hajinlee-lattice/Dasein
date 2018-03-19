package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.BusinessCalendarService;
import com.latticeengines.apps.cdl.service.PeriodService;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "periods", description = "REST resource for periods management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/periods")
public class PeriodResource {

    private final PeriodService periodService;

    private final BusinessCalendarService businessCalendarService;

    @Inject
    public PeriodResource(PeriodService periodService, BusinessCalendarService businessCalendarService) {
        this.periodService = periodService;
        this.businessCalendarService = businessCalendarService;
    }

    @GetMapping(value = "/names")
    @ApiOperation(value = "Get all period names defined in a tenant")
    public List<String> getPeriodNames(@PathVariable String customerSpace) {
        return periodService.getPeriodNames();
    }

    @GetMapping(value = "/strategies")
    @ApiOperation(value = "Get all period names defined in a tenant")
    public List<PeriodStrategy> getPeriodStrategies(@PathVariable String customerSpace) {
        return periodService.getPeriodStrategies();
    }

    @GetMapping(value = "/calendar")
    @ApiOperation(value = "Get business calendar")
    public BusinessCalendar getBusinessCalendar(@PathVariable String customerSpace) {
        return businessCalendarService.find();
    }

    @GetMapping(value = "/evaluationdate")
    @ApiOperation(value = "Get evaluation date")
    public String getEvaluationDate(@PathVariable String customerSpace) {
        return periodService.getEvaluationDate();
    }

    @PostMapping(value = "/calendar")
    @ApiOperation(value = "Save business calendar")
    public BusinessCalendar saveBusinessCalendar(@PathVariable String customerSpace,
            @RequestBody BusinessCalendar businessCalendar) {
        return businessCalendarService.save(businessCalendar);
    }

    @PostMapping(value = "/calendar/validate")
    @ApiOperation(value = "Validate business calendar")
    public String validateBusinessCalendar(@PathVariable String customerSpace,
            @RequestBody BusinessCalendar businessCalendar) {
        return businessCalendarService.validate(businessCalendar);
    }

}
