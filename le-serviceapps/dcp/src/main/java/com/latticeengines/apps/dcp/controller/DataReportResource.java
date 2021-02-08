package com.latticeengines.apps.dcp.controller;

import java.util.Set;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.DataReportService;
import com.latticeengines.apps.dcp.workflow.DCPDataReportWorkflowSubmitter;
import com.latticeengines.common.exposed.annotation.UseReaderConnection;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DCPReportRequest;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.dcp.dataReport.DataReportRollupStatus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "DataReport")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datareport")
public class DataReportResource {

    @Inject
    private DataReportService dataReportService;

    @Inject
    private DCPDataReportWorkflowSubmitter dataReportWorkflowSubmitter;

    @GetMapping
    @ResponseBody
    @ApiOperation(value = "Get Data Report")
    @UseReaderConnection
    public DataReport getDataReport(@PathVariable String customerSpace, @RequestParam DataReportRecord.Level level,
                                    @RequestParam(required = false) String ownerId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataReportService.getDataReport(customerSpace, level, ownerId);
    }

    @GetMapping("/readyforrollup")
    @ResponseBody
    @ApiOperation(value = "Get Ready For Rollup Data Report")
    public DataReport getReadyForRollupDataReport(@PathVariable String customerSpace, @RequestParam DataReportRecord.Level level,
                                 @RequestParam String ownerId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataReportService.getReadyForRollupDataReport(customerSpace, level, ownerId);
    }

    @GetMapping("/basicstats")
    @ResponseBody
    @ApiOperation(value = "Get Data Report Only Basic Stats")
    @UseReaderConnection
    public DataReport.BasicStats getDataReportBasicStats(@PathVariable String customerSpace,
                                              @RequestParam DataReportRecord.Level level,
                                              @RequestParam(required = false) String ownerId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataReportService.getDataReportBasicStats(customerSpace, level, ownerId);
    }


    @PutMapping("/readyforrollup")
    @ResponseBody
    @ApiOperation(value = "Update data report")
    public void updateDataReport(@PathVariable String customerSpace, @RequestParam DataReportRecord.Level level,
                                 @RequestParam String ownerId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataReportService.updateReadyForRollup(customerSpace, level, ownerId);
    }

    @PostMapping
    @ResponseBody
    @ApiOperation(value = "Update DataReport")
    public void updateDataReport(@PathVariable String customerSpace, @RequestParam DataReportRecord.Level level,
                                 @RequestParam(required = false) String ownerId,
                                 @RequestBody DataReport dataReport) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataReportService.updateDataReport(customerSpace, level, ownerId, dataReport);
    }

    @PutMapping("/dunscount")
    @ResponseBody
    @ApiOperation(value = "Register duns count")
    public void registerDunsCount(@PathVariable String customerSpace,
                                  @RequestParam DataReportRecord.Level level,
                                  @RequestParam(required = false) String ownerId,
                                  @RequestBody DunsCountCache cache) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataReportService.registerDunsCount(customerSpace, level, ownerId, cache);
    }

    @GetMapping("/childrenids")
    @ResponseBody
    @ApiOperation(value = "Get sub owner ids")
    public Set<String> getChildrenIds(@PathVariable String customerSpace,
                                      @RequestParam DataReportRecord.Level level,
                                      @RequestParam(required = false) String ownerId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataReportService.getChildrenIds(customerSpace, level, ownerId);
    }

    @GetMapping("/dunscount")
    @ResponseBody
    @ApiOperation(value = "Get duns count cache")
    public DunsCountCache getDunsCountCache(@PathVariable String customerSpace,
                                            @RequestParam DataReportRecord.Level level,
                                            @RequestParam(required = false) String ownerId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataReportService.getDunsCount(customerSpace, level, ownerId);
    }

    @PostMapping("/basicstats")
    @ResponseBody
    @ApiOperation(value = "Update DataReport")
    public void updateBasicStats(@PathVariable String customerSpace, @RequestParam DataReportRecord.Level level,
                                 @RequestParam(required = false) String ownerId,
                                 @RequestBody DataReport.BasicStats basicStats) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataReportService.updateDataReport(customerSpace, level, ownerId, basicStats);
    }

    @PostMapping("/inputpresencereport")
    @ResponseBody
    @ApiOperation(value = "Update DataReport")
    public void updateInputPresenceReport(@PathVariable String customerSpace, @RequestParam DataReportRecord.Level level,
                                 @RequestParam(required = false) String ownerId,
                                 @RequestBody DataReport.InputPresenceReport inputPresenceReport) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataReportService.updateDataReport(customerSpace, level, ownerId, inputPresenceReport);
    }

    @PostMapping("/geodistributionreport")
    @ResponseBody
    @ApiOperation(value = "Update DataReport")
    public void updateGeoDistributionReport(@PathVariable String customerSpace, @RequestParam DataReportRecord.Level level,
                                 @RequestParam(required = false) String ownerId,
                                 @RequestBody DataReport.GeoDistributionReport geoDistributionReport) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataReportService.updateDataReport(customerSpace, level, ownerId, geoDistributionReport);
    }

    @PostMapping("/matchtodunsreport")
    @ResponseBody
    @ApiOperation(value = "Update DataReport")
    public void updateMatchToDUNSReport(@PathVariable String customerSpace, @RequestParam DataReportRecord.Level level,
                                 @RequestParam(required = false) String ownerId,
                                 @RequestBody DataReport.MatchToDUNSReport matchToDUNSReport) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataReportService.updateDataReport(customerSpace, level, ownerId, matchToDUNSReport);
    }

    @PostMapping("/duplicationreport")
    @ResponseBody
    @ApiOperation(value = "Update DataReport")
    public void updateDuplicationReport(@PathVariable String customerSpace, @RequestParam DataReportRecord.Level level,
                                 @RequestParam(required = false) String ownerId,
                                 @RequestBody DataReport.DuplicationReport duplicationReport) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataReportService.updateDataReport(customerSpace, level, ownerId, duplicationReport);
    }

    @PostMapping("/rollup")
    @ResponseBody
    @ApiOperation(value = "roll up data report")
    public String rollupDataReport(@PathVariable String customerSpace, @RequestBody DCPReportRequest request) {
        ApplicationId appId = dataReportWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), request,
                new WorkflowPidWrapper(-1L));
        return appId.toString();
    }

    @PutMapping("/copytoparent")
    @ResponseBody
    @ApiOperation(value = "Copy data report to parent")
    public void copyDataReportToParent(@PathVariable String customerSpace, @RequestParam DataReportRecord.Level level,
                                 @RequestParam String ownerId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataReportService.copyDataReportToParent(customerSpace, level, ownerId);
    }

    @PutMapping("/rollup_status")
    @ApiOperation(value = "Update the value of RollupStatus")
    public void updateRollupStatus(@PathVariable String customerSpace, @RequestParam DataReportRollupStatus rollupStatus) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataReportService.updateRollupStatus(customerSpace, rollupStatus);
    }
}
