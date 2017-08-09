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

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.StorageMechanism;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.metadata.service.impl.TableResourceHelper;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for data tables")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class TableResource {

    @Autowired
    private TableResourceHelper tableResourceHelper;

    @RequestMapping(value = "/tables", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public List<String> getTables(@PathVariable String customerSpace) {
        return tableResourceHelper.getTables(customerSpace);
    }

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public Table getTable(@PathVariable String customerSpace, @PathVariable String tableName) {
        return tableResourceHelper.getTable(customerSpace, tableName);
    }

    @RequestMapping(value = "/tables/{tableName}/metadata", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get table metadata by name")
    public ModelingMetadata getTableMetadata(@PathVariable String customerSpace, @PathVariable String tableName) {
        return tableResourceHelper.getTableMetadata(customerSpace, tableName);
    }

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create table")
    public Boolean createTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody Table table) {
        return tableResourceHelper.createTable(customerSpace, tableName, table);
    }

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update table")
    public Boolean updateTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody Table table) {
        return tableResourceHelper.updateTable(customerSpace, tableName, table);
    }

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete table and cleanup data dir")
    public Boolean deleteTable(@PathVariable String customerSpace, //
            @PathVariable String tableName) {
        return tableResourceHelper.deleteTableAndCleanup(customerSpace, tableName);
    }

    @RequestMapping(value = "/tables/{tableName}/clone", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Clone table and underlying extracts")
    public Table cloneTable(@PathVariable String customerSpace, //
            @PathVariable String tableName) {
        return tableResourceHelper.cloneTable(customerSpace, tableName);
    }

    @RequestMapping(value = "/tables/{tableName}/copy", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Copy table and underlying extracts")
    public Table copyTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestParam("targetcustomerspace") String targetCustomerSpace) {
        return tableResourceHelper.copyTable(customerSpace, targetCustomerSpace, tableName);
    }

    @RequestMapping(value = "/tables/{tableName}/storage", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add a storage mechanism")
    public Boolean addStorageMechanism(@PathVariable String customerSpace, //
            @PathVariable String tableName, @PathVariable String storageName, //
            @RequestBody StorageMechanism storageMechanism) {
        return true;
    }

    @RequestMapping(value = "/tables/reset", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Reset tables")
    public Boolean resetTables(@PathVariable String customerSpace) {
        return tableResourceHelper.resetTables(customerSpace);
    }

    @RequestMapping(value = "/validations", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Validate metadata")
    public SimpleBooleanResponse validateMetadata(@PathVariable String customerSpace, //
            @RequestBody ModelingMetadata metadata) {
        return tableResourceHelper.validateMetadata(customerSpace, metadata);
    }
}
