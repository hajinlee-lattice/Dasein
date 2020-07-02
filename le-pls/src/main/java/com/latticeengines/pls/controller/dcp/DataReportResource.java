package com.latticeengines.pls.controller.dcp;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.pls.service.dcp.DataReportService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Data Report")
@RestController
@PreAuthorize("hasRole('View_DCP_Projects')")
@RequestMapping("/datareport")
public class DataReportResource {

    @Inject
    private DataReportService dataReportService;

    @GetMapping
    @ResponseBody
    @ApiOperation("Get data report")
    public DataReport getDataReport(@RequestParam(value = "level") DataReportRecord.Level level,
                                    @RequestParam(value = "ownerId", required = false) String ownerId,
                                    @RequestParam(value = "mock", required = false, defaultValue = "false") Boolean mock) {
        return dataReportService.getDataReport(level, ownerId, mock);
    }
}
