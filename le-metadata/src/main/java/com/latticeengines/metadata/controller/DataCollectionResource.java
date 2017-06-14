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
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
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
        return dataCollectionService.getDataCollections(customerSpace);
    }

    @RequestMapping(value = "/types/{dataCollectionType}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data collection by type")
    public DataCollection getDataCollectionByType(@PathVariable String customerSpace,
            @PathVariable DataCollectionType dataCollectionType) {
        return dataCollectionService.getDataCollectionByType(customerSpace, dataCollectionType);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update data collection")
    public DataCollection createOrUpdateDataCollection(@PathVariable String customerSpace, //
            @RequestBody DataCollection dataCollection) {
        return dataCollectionService.createOrUpdateDataCollection(customerSpace, dataCollection);
    }

    @RequestMapping(value = "/types/{dataCollectionType}/tables/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update data collection")
    public DataCollection upsertTableToDataCollection(@PathVariable String customerSpace, //
                                                      @PathVariable DataCollectionType dataCollectionType, //
                                                      @PathVariable String tableName, //
                                                      @RequestParam(value = "purgeOld", defaultValue = "false") boolean purgeOldTable) {
        return dataCollectionService.upsertTableToCollection(customerSpace, dataCollectionType, tableName, purgeOldTable);
    }

    @RequestMapping(value = "/types/{dataCollectionType}/stats", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update data collection")
    public DataCollection upsertStatsToDataCollection(@PathVariable String customerSpace, //
                                                      @PathVariable DataCollectionType dataCollectionType, //
                                                      @RequestBody StatisticsContainer statisticsContainer, //
                                                      @RequestParam(value = "purgeOld", defaultValue = "false") boolean purgeOldTable) {
        return dataCollectionService.upsertStatsToCollection(customerSpace, dataCollectionType, statisticsContainer, purgeOldTable);
    }
}
