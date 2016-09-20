package com.latticeengines.pls.controller;

import java.util.List;

import com.latticeengines.remote.exposed.service.marketo.MarketoRestValidationService;
import com.latticeengines.remote.exposed.service.marketo.MarketoSoapService;
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

import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.pls.service.MarketoCredentialService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "marketo", description = "REST resource for marketo credentials")
@RestController
@RequestMapping("/marketo/credentials")
@PreAuthorize("hasRole('View_PLS_MarketoCredentials_Simplified')")
public class MarketoCredentialResource {

    private static final Log log = LogFactory.getLog(ModelSummaryResource.class);

    @Autowired
    private MarketoCredentialService marketoCredentialService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Create a marketo credential")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredentials')")
    public void create(@RequestBody MarketoCredential marketoCredential) {
        marketoCredentialService.createMarketoCredential(marketoCredential);
    }

    @RequestMapping(value = "/{credentialId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get marketo credential by id")
    @ResponseBody
    @PreAuthorize("hasRole('View_PLS_MarketoCredentials')")
    public MarketoCredential find(@PathVariable String credentialId) {
        return marketoCredentialService.findMarketoCredentialById(credentialId);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get all marketo credentials")
    @ResponseBody
    @PreAuthorize("hasRole('View_PLS_MarketoCredentials')")
    public List<MarketoCredential> findAll() {
        return marketoCredentialService.findAllMarketoCredentials();
    }

    @RequestMapping(value = "/simplified/{credentialId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get marketo credential by id")
    @ResponseBody
    public MarketoCredential findSimplified(@PathVariable String credentialId) {
        MarketoCredential marketoCredential = marketoCredentialService.findMarketoCredentialById(credentialId);
        marketoCredential.setEnrichment(null);
        return marketoCredential;
    }

    @RequestMapping(value = "/simplified", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get all marketo credentials")
    @ResponseBody
    public List<MarketoCredential> findAllSimplified() {
        List<MarketoCredential> marketoCredentials = marketoCredentialService.findAllMarketoCredentials();
        for (MarketoCredential marketoCredential : marketoCredentials) {
            marketoCredential.setEnrichment(null);
        }
        return marketoCredentials;
    }

    @RequestMapping(value = "/{credentialId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ApiOperation(value = "Delete a marketo credential")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredential')")
    public void delete(@PathVariable String credentialId) {
        marketoCredentialService.deleteMarketoCredentialById(credentialId);
    }

    @RequestMapping(value = "/{credentialId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ApiOperation(value = "Updates a marketo credential")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredentials')")
    public void update(@PathVariable String credentialId,
            @RequestBody MarketoCredential credential) {
        marketoCredentialService.updateMarketoCredentialById(credentialId, credential);
    }

    @RequestMapping(value = "/{credentialId}/enrichment", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ApiOperation(value = "Updates a enrichment mathcing fields")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredentials')")
    public void updateEnrichment(@PathVariable String credentialId,
            @RequestBody List<MarketoMatchField> marketoMatchFields) {
        marketoCredentialService.updateCredentialMatchFields(credentialId, marketoMatchFields);
    }

}
