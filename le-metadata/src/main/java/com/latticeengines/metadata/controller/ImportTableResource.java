package com.latticeengines.metadata.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.metadata.service.impl.TableResourceHelper;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "importtable", description = "REST resource for import table definition")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class ImportTableResource {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ImportTableResource.class);

    @Autowired
    private TableResourceHelper tableResourceHelper;
    
    @RequestMapping(value = "/importtables", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public List<String> getTables(@PathVariable String customerSpace, HttpServletRequest request) {
        return tableResourceHelper.getTables(customerSpace, request);
    }

    @RequestMapping(value = "/importtables/{tableName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public Table getTable(@PathVariable String customerSpace, @PathVariable String tableName, HttpServletRequest request) {
        return tableResourceHelper.getTable(customerSpace, tableName, request);
    }

    @RequestMapping(value = "/importtables/{tableName}/metadata", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get table metadata by name")
    public ModelingMetadata getTableMetadata(@PathVariable String customerSpace, @PathVariable String tableName, HttpServletRequest request) {
        return tableResourceHelper.getTableMetadata(customerSpace, tableName, request);
    }

    @RequestMapping(value = "/importtables/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create table")
    public Boolean createTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody Table table, //
            HttpServletRequest request) {
        return tableResourceHelper.createTable(customerSpace, tableName, table, request);
    }

    @RequestMapping(value = "/importtables/{tableName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update table")
    public Boolean updateTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody Table table, //
            HttpServletRequest request) {
        return tableResourceHelper.updateTable(customerSpace, tableName, table, request);
    }

    @RequestMapping(value = "/importtables/{tableName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete table")
    public Boolean deleteTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            HttpServletRequest request) {
        return tableResourceHelper.deleteTable(customerSpace, tableName, request);
    }

}
