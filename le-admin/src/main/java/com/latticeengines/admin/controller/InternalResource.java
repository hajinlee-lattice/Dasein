package com.latticeengines.admin.controller;

import java.io.File;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.dynamicopts.DynamicOptionsService;
import com.latticeengines.admin.service.FileSystemService;
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

    @Value("${admin.vdb.permstore}")
    private String permStore;

    @Value("${admin.dl.datastore}")
    private String dataStore;

    @Autowired
    private ServiceService serviceService;

    @Autowired
    private DynamicOptionsService dynamicOptionsService;

    @Autowired
    private FileSystemService fileSystemService;

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

    @RequestMapping(value = "permstore", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get file names in permstore")
    public List<String> getFilesInPermstore(HttpServletRequest request) {
        checkHeader(request);
        return fileSystemService.filesInDirectory(new File(permStore));
    }

    @RequestMapping(value = "permstore", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete file in permstore")
    public Boolean deleteFileInPermstore(@RequestParam(value = "file") String file, HttpServletRequest request) {
        checkHeader(request);
        fileSystemService.deleteFile(new File(permStore + "/" + file));
        return true;
    }

    @RequestMapping(value = "datastore/{tenantId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get files of a tenant in datastore")
    public List<String> getFilesInDatastore(@PathVariable String tenantId, HttpServletRequest request) {
        checkHeader(request);
        return fileSystemService.filesInDirectory(new File(dataStore + "/" + tenantId));
    }

    @RequestMapping(value = "datastore/{tenantId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a tenant from datastore")
    public Boolean deleteFileInDatastore(@PathVariable String tenantId, HttpServletRequest request) {
        checkHeader(request);
        fileSystemService.deleteFile(new File(dataStore + "/" + tenantId));
        return true;
    }

}
