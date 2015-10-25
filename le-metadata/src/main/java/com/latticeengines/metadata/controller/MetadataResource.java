package com.latticeengines.metadata.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.expection.AnnotationValidationError;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class MetadataResource extends InternalResourceBase {
    
    @Autowired
    private MetadataService mdService;

    @RequestMapping(value = "/tables", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public List<String> getTables(@PathVariable String customerSpace, HttpServletRequest request) {
        checkHeader(request);
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        List<Table> tables = mdService.getTables(space);
        List<String> tableNames = new ArrayList<>();
        for (Table table : tables) {
            tableNames.add(table.getName());
        }
        return tableNames;
    }

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public Table getTable(@PathVariable String customerSpace, @PathVariable String tableName, HttpServletRequest request) {
        checkHeader(request);
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return mdService.getTable(space, tableName);
    }

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create table")
    public void createTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody Table table, //
            HttpServletRequest request) {
        checkHeader(request);
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        mdService.createTable(space, table);
    }

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update table")
    public void updateTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody Table table, //
            HttpServletRequest request) {
        checkHeader(request);
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        mdService.updateTable(space, table);
    }

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete table")
    public void deleteTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            HttpServletRequest request) {
        checkHeader(request);
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        mdService.deleteTable(space, tableName);
    }

    @RequestMapping(value = "/tables/{tableName}/validations", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Validate metadata")
    public SimpleBooleanResponse validateMetadata(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody ModelingMetadata metadata) {
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        Map<String, Set<AnnotationValidationError>> validationErrors = mdService.validateTableMetadata(space, metadata);
        SimpleBooleanResponse response = SimpleBooleanResponse.successResponse();
        if (validationErrors.size() > 0) {
            List<String> errors = new ArrayList<>();
            for (Map.Entry<String, Set<AnnotationValidationError>> entry : validationErrors.entrySet()) {
                
                for (AnnotationValidationError error : entry.getValue()) {
                    errors.add(String.format("Error with field %s for column %s.", error.getFieldName(), entry.getKey()));
                }
                
            }
            response = SimpleBooleanResponse.failedResponse(errors);
        }
        return response;
    }
}
