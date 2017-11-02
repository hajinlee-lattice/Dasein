package com.latticeengines.metadata.controller;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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

    @RequestMapping(value = "/version/{version}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Switch the version of default data collection")
    public ResponseDocument<DataCollection.Version> switchVersion(@PathVariable String customerSpace,
            @PathVariable DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        DataCollection.Version version1 = dataCollectionService.switchDataCollectionVersion(customerSpace, null,
                version);
        return ResponseDocument.successResponse(version1);
    }

    @RequestMapping(value = "/tables", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the default data collection")
    public Table getTable(@PathVariable String customerSpace, @RequestParam(value = "role") TableRoleInCollection role,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        List<Table> tables = dataCollectionService.getTables(customerSpace, null, role, version);
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
    public StatisticsContainer getMainStats(@PathVariable String customerSpace,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getStats(customerSpace, null, version);
    }

    @RequestMapping(value = "/attrrepo", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the attribute repository of the default collection.")
    public AttributeRepository getAttrRepo(@PathVariable String customerSpace,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getAttrRepo(customerSpace, null, version);
    }

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or insert a table into the collection")
    public SimpleBooleanResponse upsertTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestParam(value = "role") TableRoleInCollection role,
            @RequestParam(value = "version") DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.upsertTable(customerSpace, null, tableName, role, version);
        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/reset", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or insert a table into the collection")
    public SimpleBooleanResponse resetTable(@PathVariable String customerSpace, //
            @RequestParam(value = "role") TableRoleInCollection role) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.resetTable(customerSpace, null, role);
        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/stats", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update the main statistics of the collection")
    public SimpleBooleanResponse upsertStats(@PathVariable String customerSpace, //
            @RequestBody StatisticsContainer statisticsContainer) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.addStats(customerSpace, null, statisticsContainer);
        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/attributegroups", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get mocked metadat of attribute group for company profile and talking point")
    public List<String> getAttributeGroupsForCompanyProfileAndTalkingPoints(@PathVariable String customerSpace) {
        List<String> result = Arrays.asList(InterfaceName.CompanyName.toString(), InterfaceName.City.toString(),
                InterfaceName.Country.toString(), InterfaceName.Industry.toString(), InterfaceName.Website.toString(),
                InterfaceName.YearStarted.toString());
        return result;
    }

}
