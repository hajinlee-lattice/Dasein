package com.latticeengines.admin.controller.internal;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "internal_service_resource", description = "REST service resource for internal operations")
@RestController
@RequestMapping(value = "/internal/service")
public class InternalServiceResource extends InternalResourceBase {

    @Autowired
    private ServiceService serviceService;

    @RequestMapping(value = "{serviceName}/options", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all configuration fields that are the type of option")
    public List<SelectableConfigurationField> getServiceOptionalConfigs(@PathVariable String serviceName) {
        return serviceService.getSelectableConfigurationFields(serviceName);
    }

}
