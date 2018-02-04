package com.latticeengines.metadata.controller;

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

    @GetMapping(value = "/tables")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public List<String> getTables(@PathVariable String customerSpace) {
        return tableResourceHelper.getTables(customerSpace);
    }

    @GetMapping(value = "/tables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public Table getTable(@PathVariable String customerSpace, @PathVariable String tableName) {
        return tableResourceHelper.getTable(customerSpace, tableName);
    }

    @GetMapping(value = "/tables/{tableName}/metadata")
    @ResponseBody
    @ApiOperation(value = "Get table metadata by name")
    public ModelingMetadata getTableMetadata(@PathVariable String customerSpace, @PathVariable String tableName) {
        return tableResourceHelper.getTableMetadata(customerSpace, tableName);
    }

    @PostMapping(value = "/tables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Create table")
    public Boolean createTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody Table table) {
        return tableResourceHelper.createTable(customerSpace, tableName, table);
    }

    @PutMapping(value = "/tables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Update table")
    public Boolean updateTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody Table table) {
        return tableResourceHelper.updateTable(customerSpace, tableName, table);
    }

    @DeleteMapping(value = "/tables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Delete table and cleanup data dir")
    public Boolean deleteTable(@PathVariable String customerSpace, //
            @PathVariable String tableName) {
        return tableResourceHelper.deleteTableAndCleanup(customerSpace, tableName);
    }

    @PostMapping(value = "/tables/{tableName}/clone")
    @ResponseBody
    @ApiOperation(value = "Clone table and underlying extracts")
    public Table cloneTable(@PathVariable String customerSpace, //
            @PathVariable String tableName) {
        return tableResourceHelper.cloneTable(customerSpace, tableName);
    }

    @PostMapping(value = "/tables/{tableName}/copy")
    @ResponseBody
    @ApiOperation(value = "Copy table and underlying extracts")
    public Table copyTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestParam("targetcustomerspace") String targetCustomerSpace) {
        return tableResourceHelper.copyTable(customerSpace, targetCustomerSpace, tableName);
    }

    @PostMapping(value = "/tables/{tableName}/storage")
    @ResponseBody
    @ApiOperation(value = "Add a storage mechanism")
    public Boolean addStorageMechanism(@PathVariable String customerSpace, //
            @PathVariable String tableName, @PathVariable String storageName, //
            @RequestBody StorageMechanism storageMechanism) {
        return true;
    }

    @PostMapping(value = "/tables/reset")
    @ResponseBody
    @ApiOperation(value = "Reset tables")
    public Boolean resetTables(@PathVariable String customerSpace) {
        return tableResourceHelper.resetTables(customerSpace);
    }

    @PostMapping(value = "/validations")
    @ResponseBody
    @ApiOperation(value = "Validate metadata")
    public SimpleBooleanResponse validateMetadata(@PathVariable String customerSpace, //
            @RequestBody ModelingMetadata metadata) {
        return tableResourceHelper.validateMetadata(customerSpace, metadata);
    }
}
