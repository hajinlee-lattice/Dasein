package com.latticeengines.admin.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.dynamicopts.DynamicOptionsService;
import com.latticeengines.admin.dynamicopts.impl.DataStoreProvider;
import com.latticeengines.admin.dynamicopts.impl.PermStoreProvider;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "internal_service_resource", description = "REST service resource for internal operations")
@RestController
@RequestMapping(value = "/internal")
public class InternalResource extends InternalResourceBase {

    @Autowired
    private ServiceService serviceService;

    @Autowired
    private DynamicOptionsService dynamicOptionsService;

    @Autowired
    private DataStoreProvider dataStoreProvider;

    @Autowired
    private PermStoreProvider permStoreProvider;

    @RequestMapping(value = "services/options", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all configuration fields that are the type of option")
    public SelectableConfigurationDocument getServiceOptionalConfigs(
            @RequestParam(value = "component") String component, HttpServletRequest request) {
        checkHeader(request);
        SelectableConfigurationDocument doc = serviceService.getSelectableConfigurationFields(component, false);
        if (doc == null) {
            throw new LedpException(LedpCode.LEDP_19102, new String[]{component});
        }
        return dynamicOptionsService.bind(doc);
    }

    @RequestMapping(value = "services/options", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update dropdown options of a field")
    public Boolean patchServiceOptionalConfigs(
            @RequestParam(value = "component") String component,
            @RequestBody SelectableConfigurationField patch,
            HttpServletRequest request) {
        checkHeader(request);
        return serviceService.patchOptions(component, patch);
    }

    @RequestMapping(value = "permstore/{server}/{vdbname}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get file names in permstore")
    public Boolean hasVDBInPermstore(@PathVariable String server, @PathVariable String vdbname, HttpServletRequest request) {
        checkHeader(request);
        return permStoreProvider.getVDBFolder(server, vdbname.toUpperCase()).exists();
    }

    @RequestMapping(value = "permstore/{server}/{vdbname}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete file in permstore")
    public Boolean deleteVDBInPermstore(@PathVariable String server, @PathVariable String vdbname, HttpServletRequest request) {
        checkHeader(request);
        permStoreProvider.deleteVDBFolder(server, vdbname.toUpperCase());
        return true;
    }

    @RequestMapping(value = "datastore/{server}/{tenantId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get files of a tenant in datastore")
    public List<String> getTenantFoldersInDatastore(@PathVariable String server, @PathVariable String tenantId, HttpServletRequest request) {
        checkHeader(request);
        File dir = dataStoreProvider.getTenantFolder(server, tenantId);
        if (dir.exists()) {
            return Arrays.asList(dir.list());
        } else {
            return new ArrayList<>();
        }
    }

    @RequestMapping(value = "datastore/{server}/{tenantId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a tenant from datastore")
    public Boolean deleteTenantInDatastore(@PathVariable String server, @PathVariable String tenantId, HttpServletRequest request) {
        checkHeader(request);
        dataStoreProvider.deleteTenantFolder(server, tenantId);
        return true;
    }

}
