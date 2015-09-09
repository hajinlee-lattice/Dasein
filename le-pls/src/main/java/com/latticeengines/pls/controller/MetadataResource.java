package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.exception.ExceptionUtils;
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

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.MetadataService;
import com.latticeengines.pls.service.impl.TenantConfigServiceImpl;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata")
@RestController
@RequestMapping(value = "/metadata")
@PreAuthorize("hasRole('Edit_PLS_Configuration')")
public class MetadataResource {

    private static final Log log = LogFactory.getLog(MetadataResource.class);

    @Autowired
    private SessionService sessionService;

    @Autowired
    private TenantConfigServiceImpl tenantConfigService;

    @Autowired
    private MetadataService metadataService;

    @RequestMapping(value = "/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of metadata fields")
    public ResponseDocument<List<MetadataField>> getFields(HttpServletRequest request) {
        ResponseDocument<List<MetadataField>> response = new ResponseDocument<>();
        try
        {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
            List<MetadataField> fields = metadataService.getMetadataFields(tenantName, dlUrl);

            response.setSuccess(true);
            response.setResult(fields);
        } catch (Exception ex) {
            response.setSuccess(false);
            response.setErrors(Arrays.asList(ex.getMessage()));
        }

        return response;
    }

    @RequestMapping(value = "/fields/{fieldName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a metadata field")
    public SimpleBooleanResponse updateField(@PathVariable String fieldName, @RequestBody MetadataField field, HttpServletRequest request) {
        try {
            log.info("updateField:" + field);

            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
            metadataService.UpdateField(tenantName, dlUrl, field);

            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

    @RequestMapping(value = "/fields", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a list of metadata fields")
    public SimpleBooleanResponse updateFields(@RequestBody List<MetadataField> fields, HttpServletRequest request) {
        try {
            log.info("updateFields:" + fields);

            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
            metadataService.UpdateFields(tenantName, dlUrl, fields);

            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

}
