package com.latticeengines.metadata.controller;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.metadata.service.DataCollectionService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datacollections", description = "REST resource for metadata data collections")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollections")
public class DataCollectionResource {

    @Autowired
    private DataCollectionService dataCollectionService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all data collections")
    public List<DataCollection> getDataCollections(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getDataCollections(customerSpace);
    }

    @RequestMapping(value = "/types/{dataCollectionType}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data collection by type")
    public DataCollection getDataCollectionByType(@PathVariable String customerSpace,
            @PathVariable DataCollectionType dataCollectionType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getDataCollectionByType(customerSpace, dataCollectionType);
    }

    @RequestMapping(value = "/{collectionName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data collection by type")
    public DataCollection getDataCollection(@PathVariable String customerSpace, @PathVariable String collectionName) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getDataCollection(customerSpace, collectionName);
    }

    @RequestMapping(value = "/{collectionName}/tables", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data collection by type")
    public List<Table> getTables(@PathVariable String customerSpace, @PathVariable String collectionName,
            @RequestParam(value = "role", required = false) TableRoleInCollection tableRole) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getTables(customerSpace, collectionName, tableRole);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update data collection")
    public DataCollection createOrUpdateDataCollection(@PathVariable String customerSpace, //
            @RequestBody DataCollection dataCollection) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.createOrUpdateDataCollection(customerSpace, dataCollection);
    }

    @RequestMapping(value = "/{collectionName}/tables/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update data collection")
    public DataCollection upsertTableToDataCollection(@PathVariable String customerSpace, //
            @PathVariable String collectionName, //
            @PathVariable String tableName, //
            @RequestParam(value = "role") TableRoleInCollection role) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.upsertTable(customerSpace, collectionName, tableName, role);
    }

    @RequestMapping(value = "/{collectionName}/stats", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update data collection")
    public DataCollection upsertMainStats(@PathVariable String customerSpace, //
            @PathVariable String collectionName, //
            @RequestBody StatisticsContainer statisticsContainer, //
            @RequestParam(value = "model", required = false) String modelId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.upsertStats(customerSpace, collectionName, statisticsContainer, modelId);
    }

    @RequestMapping(value = "/{collectionName}/stats", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update data collection")
    public StatisticsContainer getMainStats(@PathVariable String customerSpace, //
            @PathVariable String collectionName, //
            @RequestParam(value = "model", required = false) String modelId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getStats(customerSpace, collectionName, modelId);
    }

}
