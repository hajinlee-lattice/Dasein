package com.latticeengines.pls.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.pls.service.MarketoCredentialService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "marketo", description = "REST resource for marketo credentials")
@RestController
@RequestMapping("/marketo/credentials")
@PreAuthorize("hasRole('View_PLS_MarketoCredential')")
public class MarketoCredentialResource {

    private static final Logger log = Logger.getLogger(ModelSummaryResource.class);

    @Autowired
    private MarketoCredentialService marketoCredentialService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a marketo credential")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredential')")
    public void create(@RequestBody MarketoCredential marketoCredential) {
        marketoCredentialService.createMarketoCredential(marketoCredential);
    }

    @RequestMapping(value = "/{credentialId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get marketo credential by id")
    @ResponseBody
    public MarketoCredential find(@PathVariable String credentialId) {
        return marketoCredentialService.findMarketoCredentialById(credentialId);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get all marketo credentials")
    @ResponseBody
    public List<MarketoCredential> findAll() {
        return marketoCredentialService.findAllMarketoCredentials();
    }

    @RequestMapping(value = "/{credentialId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ApiOperation(value = "Delete a marketo credential")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredential')")
    public void delete(@PathVariable String credentialId) {
        marketoCredentialService.deleteMarketoCredentialById(credentialId);
    }

    @RequestMapping(value = "/{credentialId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ApiOperation(value = "Updates a marketo credential")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredential')")
    public void update(@PathVariable String credentialId,
            @RequestBody MarketoCredential credential) {
        marketoCredentialService.updateMarketoCredentialById(credentialId, credential);
    }

    @RequestMapping(value = "/{credentialId}/enrichment", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ApiOperation(value = "Updates a enrichment mathcing fields")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredential')")
    public void updateEnrichment(@PathVariable String credentialId,
            @RequestBody List<MarketoMatchField> marketoMatchFields) {
        marketoCredentialService.updateCredentialMatchFields(credentialId, marketoMatchFields);
    }

}
