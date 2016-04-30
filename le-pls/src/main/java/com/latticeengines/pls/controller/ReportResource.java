package com.latticeengines.pls.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.workflow.exposed.service.ReportService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "report", description = "REST resource for reports")
@RestController
@RequestMapping("/reports")
@PreAuthorize("hasRole('View_PLS_Reports')")
public class ReportResource {

    @Autowired
    private ReportService reportService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a report")
    @PreAuthorize("hasRole('Edit_PLS_Reports')")
    public SimpleBooleanResponse createOrUpdate(@RequestBody Report report) {
        reportService.createOrUpdateReport(report);
        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/{reportName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a report by a name")
    public Report findReportByName(@PathVariable String reportName) {
        return reportService.getReportByName(reportName);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all reports")
    public List<Report> findAll() {
        List<Report> reports = reportService.findAll();
        for (Report report : reports) {
            report.setJson(null);
        }
        return reports;
    }

    @RequestMapping(value = "/{reportName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a report by a name")
    @PreAuthorize("hasRole('Edit_PLS_Reports')")
    public SimpleBooleanResponse deleteReportByName(@PathVariable String reportName) {
        reportService.deleteReportByName(reportName);
        return SimpleBooleanResponse.successResponse();
    }

}
