package com.latticeengines.metadata.controller;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
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
public class DataCollectionResource {

    @Autowired
    private DataCollectionService dataCollectionService;

    @Autowired
    private SegmentService segmentService;

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Get the default data collection")
    public DataCollection getDataCollection(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getDataCollection(customerSpace, null);
    }

    @PutMapping(value = "/version/{version}")
    @ResponseBody
    @ApiOperation(value = "Switch the version of default data collection")
    public ResponseDocument<DataCollection.Version> switchVersion(@PathVariable String customerSpace,
            @PathVariable DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        DataCollection.Version version1 = dataCollectionService.switchDataCollectionVersion(customerSpace, null,
                version);
        return ResponseDocument.successResponse(version1);
    }

    @PutMapping(value = "/datacloudbuildnumber/{dataCloudBuildNumber:.+}")
    @ResponseBody
    @ApiOperation(value = "Switch the version of default data collection")
    public ResponseDocument<String> updateDataCloudVersion(@PathVariable String customerSpace,
            @PathVariable("dataCloudBuildNumber") String dataCloudBuildNumber) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        String newDataCloudVersion = dataCollectionService.updateDataCloudBuildNumber(customerSpace, null,
                dataCloudBuildNumber);
        return ResponseDocument.successResponse(newDataCloudVersion);
    }

    @GetMapping(value = "/tables")
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

    @GetMapping(value = "/tablenames")
    @ResponseBody
    @ApiOperation(value = "Get the default data collection")
    public List<String> getTableNames(@PathVariable String customerSpace,
            @RequestParam(value = "role") TableRoleInCollection role,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getTableNames(customerSpace, null, role, version);
    }

    @GetMapping(value = "/segments")
    @ResponseBody
    @ApiOperation(value = "Get the all segments in the default collection.")
    public List<MetadataSegment> getSegments(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        DataCollection collection = dataCollectionService.getDataCollection(customerSpace, null);
        return segmentService.getSegments(customerSpace, collection.getName());
    }

    @GetMapping(value = "/stats")
    @ResponseBody
    @ApiOperation(value = "Get the main statistics of the default collection.")
    public StatisticsContainer getMainStats(@PathVariable String customerSpace,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        StatisticsContainer container = dataCollectionService.getStats(customerSpace, null, version);
        return container == null ? null : container.detachHibernate();
    }

    @GetMapping(value = "/attrrepo")
    @ResponseBody
    @ApiOperation(value = "Get the attribute repository of the default collection.")
    public AttributeRepository getAttrRepo(@PathVariable String customerSpace,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getAttrRepo(customerSpace, null, version);
    }

    @PostMapping(value = "/tables/{tableName}")
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

    @DeleteMapping(value = "/tables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Create or insert a table into the collection")
    public SimpleBooleanResponse removeTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestParam(value = "role") TableRoleInCollection role,
            @RequestParam(value = "version") DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.removeTable(customerSpace, null, tableName, role, version);
        return SimpleBooleanResponse.successResponse();
    }

    @PostMapping(value = "/reset")
    @ResponseBody
    @ApiOperation(value = "Create or insert a table into the collection")
    public SimpleBooleanResponse resetTable(@PathVariable String customerSpace, //
            @RequestParam(value = "role") TableRoleInCollection role) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.resetTable(customerSpace, null, role);
        return SimpleBooleanResponse.successResponse();
    }

    @PostMapping(value = "/stats")
    @ResponseBody
    @ApiOperation(value = "Create or update the main statistics of the collection")
    public SimpleBooleanResponse upsertStats(@PathVariable String customerSpace, //
            @RequestBody StatisticsContainer statisticsContainer) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.addStats(customerSpace, null, statisticsContainer);
        return SimpleBooleanResponse.successResponse();
    }

    @DeleteMapping(value = "/stats")
    @ResponseBody
    @ApiOperation(value = "Remove the main statistics of the collection")
    public SimpleBooleanResponse removeStats(@PathVariable String customerSpace,
            @RequestParam(value = "version") DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.removeStats(customerSpace, null, version);
        return SimpleBooleanResponse.successResponse();
    }

    @GetMapping(value = "/attributegroups")
    @ResponseBody
    @ApiOperation(value = "Get mocked metadat of attribute group for company profile and talking point")
    public List<String> getAttributeGroupsForCompanyProfileAndTalkingPoints(@PathVariable String customerSpace) {
        List<String> result = Arrays.asList(InterfaceName.CompanyName.toString(), InterfaceName.City.toString(),
                InterfaceName.Country.toString(), InterfaceName.Industry.toString(), InterfaceName.Website.toString(),
                InterfaceName.YearStarted.toString());
        return result;
    }

}
