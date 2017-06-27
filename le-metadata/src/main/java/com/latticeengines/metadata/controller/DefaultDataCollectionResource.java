package com.latticeengines.metadata.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.metadata.service.SegmentService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * This controller assumes operation on the default datacollection
 */
@Api(value = "datacollection", description = "REST resource for default metadata data collection")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection")
public class DefaultDataCollectionResource {

    @Autowired
    private DataCollectionService dataCollectionService;

    @Autowired
    private SegmentService segmentService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the default data collection")
    public DataCollection getDataCollection(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getDataCollection(customerSpace, null);
    }

    @RequestMapping(value = "/tables", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the default data collection")
    public Table getTable(@PathVariable String customerSpace, TableRoleInCollection role) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        List<Table> tables = dataCollectionService.getTables(customerSpace, null, role);
        if (tables == null || tables.isEmpty()) {
            return null;
        } else {
            return tables.get(0);
        }
    }

    @RequestMapping(value = "/segments", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the all segments in the default collection.")
    public List<MetadataSegment> getSegments(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        DataCollection collection = dataCollectionService.getDataCollection(customerSpace, null);
        return segmentService.getSegments(customerSpace, collection.getName());
    }

    @RequestMapping(value = "/stats", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the main statistics of the default collection.")
    public StatisticsContainer getMainStats(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getStats(customerSpace, null);
    }

    @RequestMapping(value = "/attrrepo", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the main statistics of the default collection.")
    public AttributeRepository getAttrRepo(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getAttrRepo(customerSpace, null);
    }

}
