package com.latticeengines.admin.controller.internal;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "internal_service_resource", description = "REST service resource for internal operations")
@RestController
@RequestMapping(value = "/internal/services")
public class InternalServiceResource extends InternalResourceBase {

    @Autowired
    private ServiceService serviceService;

    @RequestMapping(value = "dropdown_options", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all configuration fields that are the type of option")
    public SelectableConfigurationDocument getServiceOptionalConfigs(
            @RequestParam(value = "component", required = false) String component, HttpServletRequest request) {
        checkHeader(request);
        return serviceService.getSelectableConfigurationFields(component);
    }

}
