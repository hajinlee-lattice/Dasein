package com.latticeengines.app.exposed.controller.datacollection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "statistics", description = "Statistics of entities in data collection")
@RestController
@RequestMapping("/datacollection/statistics")
public class CommonStatisticsResource {

    @Autowired
    private DataLakeService dataLakeService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get statistics")
    private Statistics getStatistics() {
        return dataLakeService.getStatistics();
    }

    @RequestMapping(value = "/demo", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get demo statistics")
    private Statistics getDemoStatistics() {
        return dataLakeService.getDemoStatistics();
    }



}
