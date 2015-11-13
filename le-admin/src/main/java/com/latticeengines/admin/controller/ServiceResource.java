package com.latticeengines.admin.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.dynamicopts.DynamicOptionsService;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "serviceadmin", description = "REST resource for managing Lattice services across all tenants")
@RestController
@RequestMapping(value = "/services")
@PostAuthorize("hasRole('Platform Operations') or hasRole('DeveloperSupport') or hasRole('QA')")
public class ServiceResource {

    @Autowired
    private ServiceService serviceService;

    @Autowired
    private DynamicOptionsService dynamicOptionsService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of services")
    public List<String> getServices() {
        return new ArrayList<>(serviceService.getRegisteredServices());
    }

    @RequestMapping(value = "/products", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of services and their associated products")
    public Map<String, Set<LatticeProduct>> getServicesWithProducts() {
        return new HashMap<>(serviceService.getRegisteredServicesWithProducts());
    }

    @RequestMapping(value = "{serviceName}/default", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get default config for a service")
    public SerializableDocumentDirectory getServiceDefaultConfig(@PathVariable String serviceName) {
        SerializableDocumentDirectory dir = serviceService.getDefaultServiceConfig(serviceName);
        dir.setRootPath("/" + serviceName);
        return dynamicOptionsService.bind(dir);
    }

    @RequestMapping(value = "{serviceName}/options", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all configuration fields that are the type of option")
    public SelectableConfigurationDocument getServiceOptionalConfigs(@PathVariable String serviceName,
            @RequestParam(value = "include_dynamic_opts") boolean includeDynamicOpts) {
        SelectableConfigurationDocument doc = serviceService.getSelectableConfigurationFields(serviceName,
                includeDynamicOpts);
        if (doc == null) {
            throw new LedpException(LedpCode.LEDP_19102, new String[] { serviceName });
        }
        return dynamicOptionsService.bind(doc);
    }

    @RequestMapping(value = "{serviceName}/options", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all configuration fields that are the type of option")
    public Boolean patchServiceOptionalConfigs(@PathVariable String serviceName,
            @RequestBody SelectableConfigurationField patch) {
        return serviceService.patchOptions(serviceName, patch);
    }
}
