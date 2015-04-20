package com.latticeengines.admin.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "serviceadmin", description = "REST resource for managing Lattice services across all tenants")
@RestController
@RequestMapping(value = "/services")
public class ServiceResource {

    @Autowired
    private ServiceService serviceService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of services")
    public List<String> getServices() {
        return new ArrayList<>(serviceService.getRegisteredServiceKeySet());
    }

    @RequestMapping(value = "{serviceName}/default", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get default config for a service")
    public SerializableDocumentDirectory getServiceDefaultConfig(@PathVariable String serviceName) {
        return serviceService.getDefaultServiceConfig(serviceName);
    }
}
