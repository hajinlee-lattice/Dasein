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

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.network.exposed.metadata.DataCollectionInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata data collections")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollections")
public class DataCollectionResource implements DataCollectionInterface {
    @Autowired
    private DataCollectionService dataCollectionService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all data collections")
    @Override
    public List<DataCollection> getDataCollections(@PathVariable String customerSpace) {
        return dataCollectionService.getDataCollections(customerSpace);
    }

    @RequestMapping(value = "/default", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get default data collections")
    @Override
    public DataCollection getDefaultDataCollection(@PathVariable String customerSpace) {
        return dataCollectionService.getDefaultDataCollection(customerSpace);
    }

    @RequestMapping(value = "/default", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create default data collection")
    @Override
    public DataCollection createDefaultDataCollection(@PathVariable String customerSpace, //
            @RequestParam String statisticsId, //
            @RequestBody List<String> tableNames) {
        return dataCollectionService.createDefaultDataCollection(customerSpace, statisticsId, tableNames);
    }

    @RequestMapping(value = "/names/{dataCollectionName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data collection")
    @Override
    public DataCollection getDataCollection(@PathVariable String customerSpace, @PathVariable String dataCollectionName) {
        return dataCollectionService.getDataCollection(customerSpace, dataCollectionName);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create data collection")
    @Override
    public DataCollection createDataCollection(@PathVariable String customerSpace, //
            @RequestParam String statisticsId, //
            @RequestBody List<String> tableNames) {
        return dataCollectionService.createDataCollection(customerSpace, statisticsId, tableNames);
    }
}
