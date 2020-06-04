package com.latticeengines.admin.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.dynamicopts.DynamicOptionsService;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.util.NodesSort;
import com.latticeengines.domain.exposed.admin.LatticeModule;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "serviceadmin")
@RestController
@RequestMapping(value = "/services")
@PostAuthorize("hasRole('adminconsole')")
public class ServiceResource {

    @Inject
    private ServiceService serviceService;

    @Inject
    private DynamicOptionsService dynamicOptionsService;

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get list of services")
    public List<String> getServices() {
        return new ArrayList<>(serviceService.getRegisteredServices());
    }

    @GetMapping("/products")
    @ResponseBody
    @ApiOperation(value = "Get list of services and their associated products")
    public Map<String, Set<LatticeProduct>> getServicesWithProducts() {
        return new HashMap<>(serviceService.getRegisteredServicesWithProducts());
    }

    @GetMapping("/modules")
    @ResponseBody
    @ApiOperation(value = "Get map of module -> name")
    public List<LatticeModule> getModules() {
        return serviceService.getRegisteredModules();
    }

    @GetMapping("{serviceName}/default")
    @ResponseBody
    @ApiOperation(value = "Get default config for a service")
    public SerializableDocumentDirectory getServiceDefaultConfig(@PathVariable String serviceName) {
        SerializableDocumentDirectory dir = serviceService.getDefaultServiceConfig(serviceName);
        NodesSort.sortByServiceName(dir, serviceName);
        dir.setRootPath("/" + serviceName);
        return dynamicOptionsService.bind(dir);
    }

    @GetMapping("{serviceName}/options")
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

    @PutMapping("{serviceName}/options")
    @ResponseBody
    @ApiOperation(value = "Get all configuration fields that are the type of option")
    public Boolean patchServiceOptionalConfigs(@PathVariable String serviceName,
            @RequestBody SelectableConfigurationField patch) {
        return serviceService.patchOptions(serviceName, patch);
    }
}
