package com.latticeengines.metadata.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.metadata.service.ModuleService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata module")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class ModuleResource {

    @Inject
    private ModuleService moduleService;

    @GetMapping("/modules/{moduleName}")
    @ResponseBody
    @ApiOperation(value = "Get Module")
    public Module getModule(@PathVariable String customerSpace, //
            @PathVariable String moduleName) {
        return moduleService.getModuleByName(customerSpace, moduleName);
    }
}
