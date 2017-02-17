package com.latticeengines.metadata.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.QuerySource;
import com.latticeengines.metadata.service.QuerySourceService;
import com.latticeengines.network.exposed.metadata.QuerySourceInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata query sources")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/querysources")
public class QuerySourceResource implements QuerySourceInterface {
    @Autowired
    private QuerySourceService querySourceService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all query sources")
    @Override
    public List<QuerySource> getQuerySources(@PathVariable String customerSpace) {
        return querySourceService.getQuerySources(customerSpace);
    }

    @RequestMapping(value = "/default", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get default query sources")
    @Override
    public QuerySource getDefaultQuerySource(@PathVariable String customerSpace) {
        return querySourceService.getDefaultQuerySource(customerSpace);
    }

    @RequestMapping(value = "/default", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create default query source")
    @Override
    public QuerySource createDefaultQuerySource(@PathVariable String customerSpace, //
            @RequestParam String statisticsId, //
            @RequestBody List<String> tableNames) {
        return querySourceService.createDefaultQuerySource(customerSpace, statisticsId, tableNames);
    }

    @RequestMapping(value = "/names/{querySourceName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get query source")
    @Override
    public QuerySource getQuerySource(@PathVariable String customerSpace, @PathVariable String querySourceName) {
        return querySourceService.getQuerySource(customerSpace, querySourceName);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create query source")
    @Override
    public QuerySource createQuerySource(@PathVariable String customerSpace, //
            @RequestParam String statisticsId, //
            @RequestBody List<String> tableNames) {
        return querySourceService.createQuerySource(customerSpace, statisticsId, tableNames);
    }
}
