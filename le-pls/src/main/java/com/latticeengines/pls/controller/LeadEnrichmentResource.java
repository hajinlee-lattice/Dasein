package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.pls.service.LeadEnrichmentService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "leadenrichment", description = "REST resource for lead enrichment")
@RestController
@RequestMapping(value = "/leadenrichment")
@PreAuthorize("hasRole('Edit_PLS_Configurations')")
public class LeadEnrichmentResource {

    @Autowired
    private LeadEnrichmentService leadEnrichmentService;

    @RequestMapping(value = "/avariableattributes", method=RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all avariable attributes")
    public ResponseDocument<List<LeadEnrichmentAttribute>> getAvariableAttributes(HttpServletRequest request) {
        ResponseDocument<List<LeadEnrichmentAttribute>> response = new ResponseDocument<>();
        try
        {
            List<LeadEnrichmentAttribute> attributes = leadEnrichmentService.getAvariableAttributes();

            response.setSuccess(true);
            response.setResult(attributes);
        } catch (Exception ex) {
            response.setSuccess(false);
            response.setErrors(Arrays.asList(ex.getMessage()));
        }

        return response;
    }

    @RequestMapping(value = "/savedattributes", method=RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get saved attributes")
    public ResponseDocument<List<LeadEnrichmentAttribute>> getSavedAttributes(HttpServletRequest request) {
        ResponseDocument<List<LeadEnrichmentAttribute>> response = new ResponseDocument<>();
        try
        {
            List<LeadEnrichmentAttribute> attributes = leadEnrichmentService.getSavedAttributes();

            response.setSuccess(true);
            response.setResult(attributes);
        } catch (Exception ex) {
            response.setSuccess(false);
            response.setErrors(Arrays.asList(ex.getMessage()));
        }

        return response;
    }
}
