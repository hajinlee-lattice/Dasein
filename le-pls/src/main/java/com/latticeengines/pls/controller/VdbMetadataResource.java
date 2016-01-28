package com.latticeengines.pls.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.VdbMetadataConstants;
import com.latticeengines.pls.service.VdbMetadataService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata in VisiDB")
@RestController
@RequestMapping(value = "/vdbmetadata")
@PreAuthorize("hasRole('Edit_PLS_Configurations')")
public class VdbMetadataResource {

    private static final Log log = LogFactory.getLog(VdbMetadataResource.class);

    @Autowired
    private SessionService sessionService;

    @Autowired
    private VdbMetadataService vdbMetadataService;

    @RequestMapping(value = "/options", method=RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get map of metadata attribute options")
    public Map<String, String[]> getOptions(HttpServletRequest request) {
        Map<String, String[]> map = new HashMap<String, String[]>();
        map.put("CategoryOptions", VdbMetadataConstants.CATEGORY_OPTIONS);
        map.put("ApprovedUsageOptions", VdbMetadataConstants.APPROVED_USAGE_OPTIONS);
        map.put("FundamentalTypeOptions", VdbMetadataConstants.FUNDAMENTAL_TYPE_OPTIONS);
        map.put("StatisticalTypeOptions", VdbMetadataConstants.STATISTICAL_TYPE_OPTIONS);

        return map;
    }

    @RequestMapping(value = "/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of metadata fields")
    public List<VdbMetadataField> getFields(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return vdbMetadataService.getFields(tenant);
    }

    @RequestMapping(value = "/fields/{fieldName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a metadata field")
    public Boolean updateField(@PathVariable String fieldName, @RequestBody VdbMetadataField field, HttpServletRequest request) {
        log.info("updateField:" + field);

        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        vdbMetadataService.UpdateField(tenant, field);
        return true;
    }

    @RequestMapping(value = "/fields", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a list of metadata fields")
    public Boolean updateFields(@RequestBody List<VdbMetadataField> fields, HttpServletRequest request) {
        log.info("updateFields:" + fields);

        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        vdbMetadataService.UpdateFields(tenant, fields);
        return true;
    }

    @RequestMapping(value = "/runninggroups/{groupName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a value specifying load group is running or not")
    public Boolean isLoadGroupRunning(@PathVariable String groupName, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return vdbMetadataService.isLoadGroupRunning(tenant, groupName);
    }

    @RequestMapping(value = "/executegroup/{groupName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Execute load group")
    public Boolean executeLoadGroup(@PathVariable String groupName, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        vdbMetadataService.executeLoadGroup(tenant, groupName);
        return true;
    }
}
