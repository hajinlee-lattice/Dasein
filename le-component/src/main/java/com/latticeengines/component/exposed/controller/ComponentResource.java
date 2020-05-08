package com.latticeengines.component.exposed.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.component.exposed.service.ComponentService;
import com.latticeengines.domain.exposed.component.ComponentStatus;
import com.latticeengines.domain.exposed.component.InstallDocument;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "component", description = "Rest resource for install/uninstall le component")
@RestController
@RequestMapping("/component/customerSpace/{customerSpace}")
public class ComponentResource {

    @Inject
    private ComponentService componentService;

    @RequestMapping(value = "/install", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Install component")
    public boolean installComponent(@PathVariable String customerSpace,
                                    @RequestBody InstallDocument installDocument) {
        return componentService.install(customerSpace, installDocument);
    }

    @RequestMapping(value = "/destroy", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Uninstall component")
    public boolean destroyComponent(@PathVariable String customerSpace) {
        return componentService.destroy(customerSpace);
    }

    @RequestMapping(value = "/status", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status for component")
    public ComponentStatus getComponentStatus(@PathVariable String customerSpace) {
        return componentService.getComponentStatus(customerSpace);
    }

    @RequestMapping(value = "/status/{status}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status for component")
    public void setComponentStatus(@PathVariable String customerSpace, @PathVariable ComponentStatus status) {
        componentService.updateComponentStatus(customerSpace, status);
    }
}
