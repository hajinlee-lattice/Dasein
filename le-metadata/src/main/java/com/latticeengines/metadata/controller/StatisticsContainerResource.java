package com.latticeengines.metadata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.metadata.service.StatisticsContainerService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for statistics")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/statistics")
public class StatisticsContainerResource {

    @Autowired
    private StatisticsContainerService statisticsContainerService;

    @RequestMapping(value = "/{statisticsName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get statistics by name")
    public StatisticsContainer getStatistics(@PathVariable String customerSpace, //
            @PathVariable String statisticsName) {
        return statisticsContainerService.findByName(customerSpace, statisticsName);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Persist the specified statistics")
    public StatisticsContainer createOrUpdateStatistics(@PathVariable String customerSpace, //
            @RequestBody StatisticsContainer statistics) {
        return statisticsContainerService.createOrUpdate(customerSpace, statistics);
    }

    @RequestMapping(value = "/{statisticsName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete the specified statistics")
    public void deleteStatistics(@PathVariable String customerSpace, //
            @PathVariable String statisticsName) {
        statisticsContainerService.delete(customerSpace, statisticsName);
    }
}
