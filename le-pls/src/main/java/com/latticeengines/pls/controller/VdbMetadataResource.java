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
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.VdbMetadataService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata in VisiDB")
@RestController
@RequestMapping(value = "/vdbmetadata")
@PreAuthorize("hasRole('Edit_PLS_Configuration')")
public class VdbMetadataResource {

    private static final Log log = LogFactory.getLog(VdbMetadataResource.class);

    @Autowired
    private SessionService sessionService;

    @Autowired
    private VdbMetadataService vdbMetadataService;

    @RequestMapping(value = "/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of metadata fields")
    public ResponseDocument<List<VdbMetadataField>> getFields(HttpServletRequest request) {
        ResponseDocument<List<VdbMetadataField>> response = new ResponseDocument<>();
        try
        {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            List<VdbMetadataField> fields = vdbMetadataService.getFields(tenant);

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
    public SimpleBooleanResponse updateField(@PathVariable String fieldName, @RequestBody VdbMetadataField field, HttpServletRequest request) {
        try {
            log.info("updateField:" + field);

            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            vdbMetadataService.UpdateField(tenant, field);

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
    public SimpleBooleanResponse updateFields(@RequestBody List<VdbMetadataField> fields, HttpServletRequest request) {
        try {
            log.info("updateFields:" + fields);

            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            vdbMetadataService.UpdateFields(tenant, fields);

            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

}
