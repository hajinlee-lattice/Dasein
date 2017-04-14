package com.latticeengines.metadata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.metadata.service.DependableObjectService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dependencies", description = "REST resource for dependency management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/dependencies")
public class DependableObjectResource {

    @Autowired
    private DependableObjectService dependableObjectService;

    @RequestMapping(value = "/", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get by type and name")
    public DependableObject find(@PathVariable String customerSpace, @RequestParam String type,
            @RequestParam String name) {
        return dependableObjectService.find(customerSpace, type, name);
    }

    @RequestMapping(value = "/", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update")
    public boolean createOrUpdate(@PathVariable String customerSpace, @RequestBody DependableObject dependableObject) {
        dependableObjectService.createOrUpdate(customerSpace, dependableObject);
        return true;
    }

    @RequestMapping(value = "/", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete")
    public boolean delete(@PathVariable String customerSpace, @RequestParam String type, @RequestParam String name) {
        dependableObjectService.delete(customerSpace, type, name);
        return true;
    }

}